#!/usr/bin/env python3
"""Event timings for simulation.

For every write, and every read path, work out WHEN the new value became
visible, as an interval rather than a point:

    lower bound = start of the last read that still saw the old value
    upper bound = end of the first read that saw the new value

The true moment of visibility lies somewhere in between. The width of that
interval is the harness's resolution, set by how often a path is polled and by
read round-trip time, and it is reported rather than hidden.

Outputs, per write concern condition:
    visibility_per_write.csv  one row per (run, target, write): the empirical
                              distribution a simulation can sample from
    visibility_summary.csv    percentiles per target
    ack_latency.csv           write acknowledgement percentiles
    read_latency.csv          read round-trip percentiles
    visibility_cdf.png
    TIMINGS.md

Every run is processed on its own before anything is pooled, because
perf_counter_ns timestamps from different processes share no epoch.

Usage:
    python -m harness.event_timings measurements/2026*__threaded-*
"""

from __future__ import annotations

import argparse
import gzip
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

Q = [0.5, 0.9, 0.99]


def read_jsonl(run_dir: Path, name: str) -> pd.DataFrame:
    gz, plain = run_dir / f"{name}.gz", run_dir / name
    if gz.exists():
        with gzip.open(gz, "rt") as fh:
            return pd.DataFrame(json.loads(x) for x in fh)
    with plain.open() as fh:
        return pd.DataFrame(json.loads(x) for x in fh)


def condition_of(manifest: dict) -> str:
    wc = manifest["config"]["workload"]["write_concern"]
    return f"w:{wc.get('w')}, j:{wc.get('j')}"


def visibility_for_run(run_dir: Path):
    manifest = json.loads((run_dir / "manifest.json").read_text())
    cond = condition_of(manifest)
    writes = read_jsonl(run_dir, "writes.jsonl")
    reads = read_jsonl(run_dir, "reads.jsonl")

    w = writes[writes["error"].isna()].sort_values("t_start_ns").copy()
    w["warmup"] = w.get("warmup", False)
    w = w.rename(columns={"t_start_ns": "w_start", "t_ack_ns": "w_ack"})

    r = reads[reads["error"].isna()].sort_values("t_start_ns").copy()
    r["observed_seq"] = pd.to_numeric(r["observed_seq"], errors="coerce").fillna(-2)

    # Attach each read to the most recently SUBMITTED write.
    r = pd.merge_asof(r, w[["seq", "w_start", "w_ack", "warmup"]]
                      .rename(columns={"warmup": "w_warmup"}),
                      left_on="t_start_ns", right_on="w_start",
                      direction="backward")
    r = r.dropna(subset=["seq"])
    r = r[~r["w_warmup"].astype(bool)]
    r["fresh"] = r["observed_seq"] >= r["seq"]

    keys = ["target", "seq"]
    fr = r[r["fresh"]]
    first_idx = fr.groupby(keys)["t_start_ns"].idxmin()
    first = (r.loc[first_idx, keys + ["t_start_ns", "t_end_ns"]]
               .rename(columns={"t_start_ns": "ff_start", "t_end_ns": "ff_end"})
               .set_index(keys))
    r = r.join(first["ff_start"], on=keys)

    stale_before = (r[~r["fresh"] & (r["t_start_ns"] < r["ff_start"])]
                    .groupby(keys)["t_start_ns"].max().rename("ls_start"))
    regress = (r[~r["fresh"] & (r["t_start_ns"] > r["ff_start"])]
               .groupby(keys).size().rename("regressions"))
    base = r.groupby(keys).agg(w_start=("w_start", "first"),
                               w_ack=("w_ack", "first"),
                               n_reads=("fresh", "size"))

    v = base.join([first, stale_before, regress]).reset_index()
    v["regressions"] = v["regressions"].fillna(0).astype(int)
    v["censored"] = v["ff_end"].isna()
    v["fresh_at_first_read"] = v["ls_start"].isna() & ~v["censored"]

    ms = 1e6
    v["ack_ms"] = (v["w_ack"] - v["w_start"]) / ms
    v["upper_from_start_ms"] = (v["ff_end"] - v["w_start"]) / ms
    v["lower_from_start_ms"] = ((v["ls_start"] - v["w_start"]) / ms).where(
        ~v["fresh_at_first_read"], 0.0)
    v["upper_from_ack_ms"] = v["upper_from_start_ms"] - v["ack_ms"]
    v["lower_from_ack_ms"] = v["lower_from_start_ms"] - v["ack_ms"]
    v["resolution_ms"] = v["upper_from_start_ms"] - v["lower_from_start_ms"]
    v["condition"] = cond
    v["run_id"] = run_dir.name

    wa = w[~w["warmup"].astype(bool)].copy()
    wa["ack_ms"] = wa["latency_ns"] / ms
    wa["condition"] = cond
    wa["run_id"] = run_dir.name

    rl = reads[reads["error"].isna()][["target", "latency_ns"]].copy()
    rl["read_ms"] = rl["latency_ns"] / ms
    rl["condition"] = cond

    cols = ["condition", "run_id", "target", "seq", "n_reads", "ack_ms",
            "lower_from_start_ms", "upper_from_start_ms",
            "lower_from_ack_ms", "upper_from_ack_ms", "resolution_ms",
            "fresh_at_first_read", "censored", "regressions"]
    return v[cols], wa[["condition", "run_id", "seq", "ack_ms"]], rl


def pct(s: pd.Series, q: float):
    s = s.dropna()
    return round(float(s.quantile(q)), 3) if len(s) else None


def md_table(df: pd.DataFrame) -> str:
    cols = [str(c) for c in df.columns]
    out = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for _, row in df.iterrows():
        out.append("| " + " | ".join("" if pd.isna(x) else str(x) for x in row) + " |")
    return "\n".join(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dirs", nargs="+", type=Path)
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    vis, acks, rls = [], [], []
    for d in sorted(args.run_dirs):
        if not (d / "manifest.json").exists():
            continue
        print(f"[timings] {d.name}")
        v, a, rl = visibility_for_run(d)
        vis.append(v); acks.append(a); rls.append(rl)

    vis = pd.concat(vis, ignore_index=True)
    acks = pd.concat(acks, ignore_index=True)
    rls = pd.concat(rls, ignore_index=True)

    out = args.out or (Path("measurements/merged") /
                       f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}__timings")
    out.mkdir(parents=True, exist_ok=True)
    vis.to_csv(out / "visibility_per_write.csv", index=False)

    # ---- visibility summary
    rows = []
    for (cond, tgt), s in vis.groupby(["condition", "target"]):
        ok = s[~s["censored"]]
        rows.append({
            "condition": cond, "target": tgt, "writes": len(s),
            "fresh_at_first_read": round(s["fresh_at_first_read"].mean(), 3),
            "censored": round(s["censored"].mean(), 4),
            "vis_from_start_p50_ms": pct(ok["upper_from_start_ms"], 0.5),
            "vis_from_start_p90_ms": pct(ok["upper_from_start_ms"], 0.9),
            "vis_from_start_p99_ms": pct(ok["upper_from_start_ms"], 0.99),
            "vis_from_ack_p50_ms": pct(ok["upper_from_ack_ms"], 0.5),
            "vis_from_ack_p99_ms": pct(ok["upper_from_ack_ms"], 0.99),
            "resolution_p50_ms": pct(ok.loc[~ok["fresh_at_first_read"],
                                            "resolution_ms"], 0.5),
            "regressions_per_write": round(s["regressions"].mean(), 4),
        })
    vsum = pd.DataFrame(rows)
    vsum.to_csv(out / "visibility_summary.csv", index=False)

    # ---- ack and read latency
    asum = (acks.groupby("condition")["ack_ms"]
                .quantile([0.01, 0.1, 0.5, 0.9, 0.99, 0.999]).unstack().round(3))
    asum.insert(0, "n", acks.groupby("condition").size())
    asum.to_csv(out / "ack_latency.csv")

    rsum = (rls.groupby(["condition", "target"])["read_ms"]
               .quantile(Q).unstack().round(3).reset_index())
    rsum.columns = ["condition", "target", "p50_ms", "p90_ms", "p99_ms"]
    rsum.to_csv(out / "read_latency.csv", index=False)

    # ---- plot
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    conds = sorted(vis["condition"].unique())
    fig, axes = plt.subplots(1, len(conds), figsize=(6.5 * len(conds), 4.5),
                             squeeze=False)
    for ax, cond in zip(axes[0], conds):
        sub = vis[(vis["condition"] == cond) & ~vis["censored"]]
        for tgt, s in sub.groupby("target"):
            x = s["upper_from_start_ms"].clip(lower=0.05).sort_values().to_numpy()
            ax.plot(x, [i / len(x) for i in range(len(x))], lw=1.2, label=tgt)
        ax.set_xscale("log")
        ax.set_title(cond)
        ax.set_xlabel("new value visible by (ms after write start, log)")
        ax.set_ylabel("fraction of writes")
        ax.grid(alpha=0.3)
        ax.legend(fontsize=7)
    fig.tight_layout()
    fig.savefig(out / "visibility_cdf.png", dpi=160)

    # ---- document
    md = ["# Event timings\n",
          f"Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC} from "
          f"{vis['run_id'].nunique()} runs, {len(vis)} write-by-path observations.\n",
          "## Write acknowledgement\n",
          "Time from sending a write to receiving its acknowledgement (ms).\n",
          md_table(asum.reset_index()), "",
          "## When a write becomes visible, per read path\n",
          "For each write, the new value became visible on a path somewhere "
          "between the last read that still saw the old value and the first "
          "read that saw the new one. Figures below are the upper bound of that "
          "interval, which is the conservative choice. `from_ack` values can be "
          "negative: that means the value was already visible before the "
          "writer received its acknowledgement.\n",
          "`fresh_at_first_read` is the share of writes where even the first "
          "read after the write already saw the new value. For those, the true "
          "visibility time is earlier than anything this harness can observe, "
          "so their bounds say only 'no later than the first read'.\n",
          "`resolution_p50_ms` is the typical width of the visibility interval, "
          "the harness's time resolution for that path.\n",
          md_table(vsum), "",
          "## Read round-trip time\n",
          md_table(rsum), "",
          "## Using these as simulation inputs\n",
          "These roughly correspond to the four latency distributions in the "
          "WARS model of Bailis et al. (PBS, VLDB 2012):\n",
          "- write ack latency: the `ack_latency` table, per write concern\n"
          "- propagation to each replica: visibility on the `direct-*` paths "
          "under `w:1`, measured from write start\n"
          "- read request and response: the read round-trip table\n"
          "- majority commit point delay (MongoDB-specific, no WARS "
          "equivalent): visibility on `rs-secondary-majority` under `w:1`\n",
          "`visibility_per_write.csv` holds one row per write per path, so a "
          "simulation can sample the empirical distributions directly instead "
          "of fitting a parametric model.\n",
          "## Limits\n",
          "- In isolated mode each read path is measured in its own run, so "
          "these files do not give the joint distribution of visibility across "
          "replicas for the same write. A simulation that assumes replicas are "
          "independent can use them; one that needs correlation between "
          "replicas cannot.\n"
          "- All replicas share one machine and loopback networking. Timings "
          "reflect scheduling, oplog apply and disk, not network distance.\n"
          "- Visibility is bounded, not measured exactly. See resolution.\n"]
    (out / "TIMINGS.md").write_text("\n".join(md))

    print("\n" + vsum.to_string(index=False))
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()