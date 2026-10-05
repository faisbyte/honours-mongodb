#!/usr/bin/env python3
"""Read-your-writes test with a figure.

Each trial writes a new value, then immediately reads it back from a SECONDARY
with readConcern "majority". Three arms, differing only in how the read is made:

    plain          no session, read through find_one()
    causal (bug)   causal session, read through a hand-built db.command()
    causal (fixed) causal session, read through find_one()

The middle arm is the harness bug found earlier: pymongo does not merge
afterClusterTime into a hand-built find command that already carries a
readConcern, so the session timestamp never reaches the server. Showing it
next to the fixed arm documents both the bug and the fix in one figure.

Per-trial data is saved, so the figure can be redrawn without rerunning.

Output (measurements/merged/<stamp>__ryw/):
    ryw_trials.csv     one row per trial: arm, seq, fresh, latency_us
    ryw_summary.csv    one row per arm
    ryw_meta.json      write concern, read concern, journal default, trials
    ryw_figure.png

Usage:
    python -m harness.ryw_figure --trials 1000
    python -m harness.ryw_figure --redraw measurements/merged/<stamp>__ryw
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

HOSTS = "127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019,127.0.0.1:27020"
URI = f"mongodb://{HOSTS}/?replicaSet=rs0"

ARMS = [
    ("plain", "plain read (no session)"),
    ("causal_bug", "causal session, hand-built command (bug)"),
    ("causal_fixed", "causal session, find_one (fixed)"),
]
COLOURS = {"plain": "#7f7f7f", "causal_bug": "#d62728", "causal_fixed": "#2ca02c"}


def run_arm(client, arm: str, trials: int, wc: dict, rc: str) -> list[dict]:
    from pymongo.read_concern import ReadConcern
    from pymongo.read_preferences import Secondary

    db = client.freshness
    coll, key = "probe_ryw", f"ryw-{arm}"
    secondary = Secondary()
    session = client.start_session(causal_consistency=True) if arm != "plain" else None

    raw_cmd = {"find": coll, "filter": {"_id": key}, "limit": 1,
               "singleBatch": True, "readConcern": {"level": rc}}
    coll_obj = db.get_collection(coll, read_preference=secondary,
                                 read_concern=ReadConcern(rc))
    rows = []
    for seq in range(trials):
        try:
            reply = db.command({
                "update": coll,
                "updates": [{"q": {"_id": key}, "u": {"$set": {"seq": seq}},
                             "upsert": True}],
                "writeConcern": wc,
            }, session=session)
        except Exception:  # noqa: BLE001
            continue

        if session is not None:
            if reply.get("$clusterTime") is not None:
                session.advance_cluster_time(reply["$clusterTime"])
            if reply.get("operationTime") is not None:
                session.advance_operation_time(reply["operationTime"])

        t0 = time.perf_counter_ns()
        try:
            if arm == "causal_bug":
                r = db.command(raw_cmd, read_preference=secondary, session=session)
                t1 = time.perf_counter_ns()
                batch = r.get("cursor", {}).get("firstBatch", [])
                observed = batch[0].get("seq") if batch else None
            else:
                doc = coll_obj.find_one({"_id": key}, session=session)
                t1 = time.perf_counter_ns()
                observed = doc.get("seq") if doc else None
        except Exception:  # noqa: BLE001
            continue

        rows.append({"arm": arm, "seq": seq, "fresh": observed == seq,
                     "latency_us": (t1 - t0) / 1000})

    if session is not None:
        session.end_session()
    return rows


def summarise(df: pd.DataFrame) -> pd.DataFrame:
    out = []
    for arm, label in ARMS:
        s = df[df["arm"] == arm]
        if s.empty:
            continue
        out.append({
            "arm": arm, "label": label, "trials": len(s),
            "held": int(s["fresh"].sum()),
            "rate": round(s["fresh"].mean(), 4),
            "p50_us": round(s["latency_us"].median(), 1),
            "p95_us": round(s["latency_us"].quantile(0.95), 1),
            "p99_us": round(s["latency_us"].quantile(0.99), 1),
        })
    return pd.DataFrame(out)


def draw(df: pd.DataFrame, summary: pd.DataFrame, meta: dict, out: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.6))

    labels = ["plain\n(no session)", "causal\n(hand-built command,\nbug)",
              "causal\n(find_one,\nfixed)"]
    present = [a for a, _ in ARMS if a in set(summary["arm"])]
    rows = [summary[summary["arm"] == a].iloc[0] for a in present]
    xs = range(len(rows))
    bars = ax1.bar(xs, [r["rate"] * 100 for r in rows],
                   color=[COLOURS[a] for a in present])
    for b, r in zip(bars, rows):
        ax1.text(b.get_x() + b.get_width() / 2, max(r["rate"] * 100, 0) + 2,
                 f"{int(r['held'])} / {int(r['trials'])}",
                 ha="center", fontsize=10)
    ax1.set_xticks(list(xs))
    ax1.set_xticklabels([labels[[a for a, _ in ARMS].index(a)] for a in present],
                        fontsize=9)
    ax1.set_ylim(0, 112)
    ax1.set_ylabel("trials where the read returned its own write (%)")
    ax1.set_title("Read-your-writes")
    ax1.grid(axis="y", alpha=0.3)

    for a in present:
        lat = df[df["arm"] == a]["latency_us"].sort_values().to_numpy() / 1000
        n = len(lat)
        med = summary[summary["arm"] == a]["p50_us"].iloc[0] / 1000
        label = dict(ARMS)[a]
        ax2.plot(lat, [i / n for i in range(n)], lw=1.6, color=COLOURS[a],
                 label=f"{label} (p50 {med:.2f}ms)")
    ax2.set_xscale("log")
    ax2.set_xlabel("read latency (ms, log scale)")
    ax2.set_ylabel("cumulative fraction of reads")
    ax2.set_title("What the read costs")
    ax2.grid(alpha=0.3)
    ax2.legend(fontsize=8, loc="lower right")

    fig.suptitle(
        f"write {meta.get('write_concern')}, secondary read, "
        f"readConcern {meta.get('read_concern')}, journal default "
        f"{meta.get('journal_default')}", fontsize=10)
    fig.tight_layout()
    fig.savefig(out, dpi=160)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=1000)
    ap.add_argument("--write-concern", default="1")
    ap.add_argument("--read-concern", default="majority")
    ap.add_argument("--redraw", type=Path, default=None,
                    help="redraw the figure from a saved run directory")
    args = ap.parse_args()

    if args.redraw:
        out = args.redraw
        df = pd.read_csv(out / "ryw_trials.csv")
        meta = json.loads((out / "ryw_meta.json").read_text())
        summary = summarise(df)
        draw(df, summary, meta, out / "ryw_figure.png")
        print(summary.to_string(index=False))
        print(f"\nredrew {out / 'ryw_figure.png'}")
        return

    from pymongo import MongoClient
    wc = ({"w": int(args.write_concern)} if args.write_concern.isdigit()
          else {"w": args.write_concern})
    client = MongoClient(URI, serverSelectionTimeoutMS=5000)
    cfg = client.admin.command("replSetGetConfig")["config"]
    meta = {"write_concern": wc, "read_concern": args.read_concern,
            "trials": args.trials,
            "journal_default": cfg.get("writeConcernMajorityJournalDefault"),
            "chaining_allowed": cfg.get("settings", {}).get("chainingAllowed"),
            "run_utc": datetime.now(timezone.utc).isoformat()}
    print(f"write {wc}, read {args.read_concern!r}, {args.trials} trials per arm, "
          f"journal default {meta['journal_default']}")

    rows = []
    for arm, label in ARMS:
        print(f"  running: {label}")
        rows += run_arm(client, arm, args.trials, wc, args.read_concern)

    df = pd.DataFrame(rows)
    summary = summarise(df)

    out = (Path("measurements/merged") /
           f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}__ryw")
    out.mkdir(parents=True, exist_ok=True)
    df.to_csv(out / "ryw_trials.csv", index=False)
    summary.to_csv(out / "ryw_summary.csv", index=False)
    (out / "ryw_meta.json").write_text(json.dumps(meta, indent=2))
    draw(df, summary, meta, out / "ryw_figure.png")

    print()
    print(summary.drop(columns="label").to_string(index=False))
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()