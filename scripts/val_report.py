"""Report on validation and re-collect runs.

Usage, from anywhere:
    .venv/bin/python scripts/val_report.py                  # the 15 Oct *-val runs
    .venv/bin/python scripts/val_report.py --plan runs/plans/<stamp>__recollect.json
    .venv/bin/python scripts/val_report.py [selection flags]

--plan takes the runs that scripts/recollect.py completed for that plan (from
its .progress.jsonl). With several runs per cell (rounds), two more sections
appear: per-cell figures pooled across rounds, and round agreement, which
flags a cell whose median read latency or median ack latency differs between
rounds by more than 20% (larger / smaller - 1 > 0.20).

Definitions
  warmup    reads and writes flagged warmup are excluded from every statistic
            except the raw totals and missed-slot sums
  stale     observed_seq < seq of the latest write ACKED before the read
            started. A read that misses a write still in flight is not stale.
  causal    share of reads whose recorded operationTime differs from the
  opTime    writer's opTime for causal_waited_for_seq (the write the session
            was advanced to before that read)
  operationTime check
            for causal targets, every read that returned without error must
            carry a request_id, that request_id must appear exactly once in
            the listener's own reply log (find_replies.jsonl), and the read's
            recorded operationTime must equal that reply's. Runs collected
            before find_replies.jsonl existed show "not recorded".
"""
import argparse
import gzip
import json
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from harness import runs  # noqa: E402

VAL = ["threaded-w1-val", "threaded-w2-val", "threaded-w3-val", "threaded-wmaj-val"]
ROUND_TOLERANCE = 0.20


def plan_run_ids(plan: Path) -> list[str]:
    progress = plan.with_name(plan.name.replace(".json", ".progress.jsonl"))
    if not progress.exists():
        raise SystemExit(f"[report] no progress log next to {plan}")
    done = [json.loads(line) for line in progress.read_text().splitlines()]
    ids = [e["run_id"] for e in done if e["status"] == "ok"]
    failed = [e for e in done if e["status"] != "ok"]
    for e in failed:
        print(f"[report] note: plan stopped at round {e['round']} position "
              f"{e['position']}: {e['config']} {e['target']} (exit {e['rc']})")
    if not ids:
        raise SystemExit(f"[report] no completed runs in {progress}")
    return ids


def pct(a, q):
    return round(float(np.quantile(a, q)) / 1e6, 3) if len(a) else None


def rel_diff(a, b):
    lo, hi = sorted((a, b))
    return hi / lo - 1 if lo else float("inf")


def jl(d, name):
    with gzip.open(d / f"{name}.jsonl.gz", "rt") as fh:
        return pd.DataFrame(json.loads(line) for line in fh)


def jl_optional(d, name):
    return jl(d, name) if (d / f"{name}.jsonl.gz").exists() else None


def optime_check(reads: pd.DataFrame, replies) -> dict:
    """Recorded operationTime against the listener's reply for the same request.

    reads: the run's read records. replies: find_replies records for the same
    target, or None if the run predates them.
    """
    r = reads[reads["error"].isna()]
    if replies is None or "request_id" not in r.columns:
        return {"reads": len(r), "status": "not recorded", "pass": None}
    ids = [int(x) for x in replies["request_id"]]
    by_id = dict(zip(ids, (ts(o) for o in replies["operationTime"])))
    no_id = int(r["request_id"].isna().sum())
    matched = equal = 0
    for rid, op in zip(r["request_id"], r["operationTime"]):
        if pd.isna(rid) or int(rid) not in by_id:
            continue
        matched += 1
        equal += ts(op) == by_id[int(rid)]
    n = len(r)
    ok = n > 0 and no_id == 0 and matched == n and equal == n and len(by_id) == len(ids)
    return {"reads": n, "no_request_id": no_id, "matched": matched, "equal": equal,
            "duplicate_reply_ids": len(ids) - len(by_id),
            "status": "PASS" if ok else "FAIL", "pass": ok}


def ts(x):
    return (x["t"], x["i"]) if isinstance(x, dict) else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", type=Path, default=None,
                    help="a runs/plans/<stamp>__recollect.json; reports the runs it completed")
    runs.add_selection_args(ap, defaults={"config": VAL})
    args = ap.parse_args()
    args.include_excluded = True
    if args.plan:
        args.run_id = plan_run_ids(args.plan)
        args.config = None
    chosen = runs.selected(args)

    rows, wire, acks, opchecks, per_run = [], [], [], [], []
    for r in chosen:
        m, d = r.manifest, r.path
        w, rd = jl(d, "writes"), jl(d, "reads")
        wc = m["config"]["workload"]["write_concern"]
        w_ok = w[~w["warmup"] & w["error"].isna()]
        r_ok = rd[~rd["warmup"] & rd["error"].isna()]

        # latest write acked before each read started
        wa = w[w["error"].isna()].sort_values("t_ack_ns")[["seq", "t_ack_ns"]]
        j = pd.merge_asof(r_ok.sort_values("t_start_ns"), wa, left_on="t_start_ns",
                          right_on="t_ack_ns", direction="backward")
        j = j.dropna(subset=["seq"])
        stale = int((j["observed_seq"].fillna(-1) < j["seq"]).sum())

        if r.target.startswith("rs-secondary-causal"):
            reps = jl_optional(d, "find_replies")
            if reps is not None:
                reps = reps[reps["target"] == r.target]
            opchecks.append({"run": r.run_id, "target": r.target,
                             **optime_check(rd, reps)})

        causal_diff = None
        if m["config"]["targets"][0].get("causal_consistency") or r.target.startswith("rs-secondary-causal"):
            wop = {s: ts(o) for s, o in zip(w["seq"], w["operationTime"])}
            c = r_ok[r_ok["causal_waited_for_seq"].notna()]
            diff = [ts(o) != wop.get(int(s)) for o, s in zip(c["operationTime"], c["causal_waited_for_seq"])]
            causal_diff = (sum(diff), len(diff))

        per_run.append({"config": r.row["config"], "target": r.target,
                        "round": m.get("round_index", 1),
                        "read_ns": r_ok["latency_ns"].to_numpy(),
                        "ack_ns": w_ok["latency_ns"].to_numpy(),
                        "stale": stale, "stale_of": len(j),
                        "r_missed": int(rd["missed_slots"].sum()),
                        "w_missed": int(w["missed_slots"].sum())})

        rows.append({
            "run": r.run_id.split("__", 1)[0].replace("scratch/", ""),
            "config": r.row["config"].replace("-val", ""),
            "target": r.target,
            "w": wc["w"],
            "topo": f"{m['topology']['before']}->{m['topology']['after']}",
            "jd": m["journal_default"],
            "power": f"{m['power']['before']}/{m['power']['after']}",
            "fatal": len(m["reader_fatal"]) + len(m["writer_fatal"]),
            "writes": len(w),
            "w_missed": int(w["missed_slots"].sum()),
            "reads": len(rd),
            "r_missed": int(rd["missed_slots"].sum()),
            "reads/write": round(len(rd) / len(w), 1),
            "r_err": int(rd["error"].notna().sum()),
            "w_err": int(w["error"].notna().sum()),
            "stale": stale,
            "stale_of": len(j),
            "read_p50_ms": round(r_ok["latency_ns"].median() / 1e6, 3),
            "causal_diff": causal_diff,
        })

        tw = next(x for x in m["wire"] if x["client"] == f"target:{r.target}")
        sent_lv = sorted({str(v) for v in r_ok["read_concern_sent"]})
        act = r_ok["after_cluster_time_sent"].notna()
        wire.append({"config": r.row["config"].replace("-val", ""), "target": r.target,
                     "level sent (per read)": ",".join(sent_lv),
                     "afterClusterTime present": f"{int(act.sum())}/{len(act)}",
                     "listener summary": tw["read_concerns"]})

        acks.append({"config": r.row["config"].replace("-val", ""), "target": r.target,
                     "w": wc["w"], "j": wc["j"], "n": len(w_ok),
                     "ack_p50_ms": round(w_ok["latency_ns"].quantile(0.5) / 1e6, 3),
                     "ack_p99_ms": round(w_ok["latency_ns"].quantile(0.99) / 1e6, 3),
                     "interval_ms": m["config"]["workload"]["write_interval_ms"]})

    pd.set_option("display.width", 250)
    pd.set_option("display.max_colwidth", 120)
    df = pd.DataFrame(rows)
    print("== every run: provenance, slots, reads per write\n")
    print(df[["run", "config", "target", "w", "topo", "jd", "power", "fatal", "writes", "w_missed",
              "reads", "r_missed", "reads/write", "w_err", "r_err"]].to_string(index=False))
    print("\n== wire log, per target (non-warmup reads)\n")
    print(pd.DataFrame(wire).to_string(index=False))
    print("\n== causal run: recorded operationTime vs the writer's opTime it was advanced to\n")
    for x in rows:
        if x["causal_diff"]:
            k, n = x["causal_diff"]
            print(f"{x['config']} {x['target']}: differs on {k}/{n} reads ({k / n:.1%})")
    print("\n== operationTime check: causal reads against the listener's reply "
          "for the same request_id\n")
    if opchecks:
        print(pd.DataFrame(opchecks).drop(columns="pass").to_string(index=False))
    else:
        print("no causal runs selected")
    print("\n== stale reads and median read latency\n")
    print(df[["config", "target", "stale", "stale_of", "read_p50_ms"]].to_string(index=False))
    print("\n== write ack latency, primary-local runs (non-warmup)\n")
    a = pd.DataFrame(acks)
    a = a[a["target"] == "rs-primary-local"]
    print(a.to_string(index=False))

    cells = defaultdict(list)
    for x in per_run:
        cells[(x["config"], x["target"])].append(x)
    if any(len(v) > 1 for v in cells.values()):
        print("\n== per cell, pooled across rounds (non-warmup; latencies in ms)\n")
        out = []
        for (cfg, tgt), xs in sorted(cells.items()):
            rl = np.concatenate([x["read_ns"] for x in xs])
            al = np.concatenate([x["ack_ns"] for x in xs])
            st, of = sum(x["stale"] for x in xs), sum(x["stale_of"] for x in xs)
            out.append({"config": cfg, "target": tgt, "runs": len(xs),
                        "stale": st, "stale_rate": f"{st / of:.4%}" if of else None,
                        "read_p50": pct(rl, .5), "read_p90": pct(rl, .9),
                        "read_p99": pct(rl, .99),
                        "ack_p50": pct(al, .5), "ack_p99": pct(al, .99),
                        "r_missed": sum(x["r_missed"] for x in xs),
                        "w_missed": sum(x["w_missed"] for x in xs)})
        print(pd.DataFrame(out).to_string(index=False))

        print(f"\n== round agreement: medians per round, flagged if they differ by "
              f"more than {ROUND_TOLERANCE:.0%}\n")
        out = []
        for (cfg, tgt), xs in sorted(cells.items()):
            by_round = defaultdict(list)
            for x in xs:
                by_round[x["round"]].append(x)
            if len(by_round) < 2:
                continue
            rr = sorted(by_round)[:2]
            med = {r_: (np.median(np.concatenate([x["read_ns"] for x in by_round[r_]])) / 1e6,
                        np.median(np.concatenate([x["ack_ns"] for x in by_round[r_]])) / 1e6,
                        sum(x["stale"] for x in by_round[r_])
                        / max(1, sum(x["stale_of"] for x in by_round[r_])))
                   for r_ in rr}
            dr = rel_diff(med[rr[0]][0], med[rr[1]][0])
            da = rel_diff(med[rr[0]][1], med[rr[1]][1])
            out.append({"config": cfg, "target": tgt,
                        f"read_p50_r{rr[0]}": round(med[rr[0]][0], 3),
                        f"read_p50_r{rr[1]}": round(med[rr[1]][0], 3),
                        "read_diff": f"{dr:.1%}",
                        f"ack_p50_r{rr[0]}": round(med[rr[0]][1], 3),
                        f"ack_p50_r{rr[1]}": round(med[rr[1]][1], 3),
                        "ack_diff": f"{da:.1%}",
                        f"stale_r{rr[0]}": f"{med[rr[0]][2]:.3%}",
                        f"stale_r{rr[1]}": f"{med[rr[1]][2]:.3%}",
                        "flag": "DIFFERS" if max(dr, da) > ROUND_TOLERANCE else ""})
        agree = pd.DataFrame(out)
        print(agree.to_string(index=False))
        flagged = agree[agree["flag"] != ""] if len(agree) else agree
        print(f"\n{len(flagged)} of {len(agree)} cells differ by more than "
              f"{ROUND_TOLERANCE:.0%} between rounds")


if __name__ == "__main__":
    main()
