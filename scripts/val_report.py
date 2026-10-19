"""Report on the 60 s validation runs (configs *-val in runs/VALID.csv).

Usage, from anywhere:  .venv/bin/python scripts/val_report.py [selection flags]

Definitions
  warmup    reads and writes flagged warmup are excluded from every statistic
            except the raw totals and missed-slot sums
  stale     observed_seq < seq of the latest write ACKED before the read
            started. A read that misses a write still in flight is not stale.
  causal    share of reads whose recorded operationTime differs from the
  opTime    writer's opTime for causal_waited_for_seq (the write the session
            was advanced to before that read)
"""
import argparse
import gzip
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from harness import runs  # noqa: E402

VAL = ["threaded-w1-val", "threaded-w2-val", "threaded-w3-val", "threaded-wmaj-val"]


def jl(d, name):
    with gzip.open(d / f"{name}.jsonl.gz", "rt") as fh:
        return pd.DataFrame(json.loads(line) for line in fh)


def ts(x):
    return (x["t"], x["i"]) if isinstance(x, dict) else None


def main():
    ap = argparse.ArgumentParser()
    runs.add_selection_args(ap, defaults={"config": VAL})
    args = ap.parse_args()
    args.include_excluded = True
    chosen = runs.selected(args)

    rows, wire, acks = [], [], []
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

        causal_diff = None
        if m["config"]["targets"][0].get("causal_consistency") or r.target.startswith("rs-secondary-causal"):
            wop = {s: ts(o) for s, o in zip(w["seq"], w["operationTime"])}
            c = r_ok[r_ok["causal_waited_for_seq"].notna()]
            diff = [ts(o) != wop.get(int(s)) for o, s in zip(c["operationTime"], c["causal_waited_for_seq"])]
            causal_diff = (sum(diff), len(diff))

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
    print("\n== stale reads and median read latency\n")
    print(df[["config", "target", "stale", "stale_of", "read_p50_ms"]].to_string(index=False))
    print("\n== write ack latency, primary-local runs (non-warmup)\n")
    a = pd.DataFrame(acks)
    a = a[a["target"] == "rs-primary-local"]
    print(a.to_string(index=False))
    print("\nall w:1 runs pooled:",
          pd.DataFrame(acks).query("w == 1")[["n", "ack_p50_ms", "ack_p99_ms"]].to_string(index=False))


if __name__ == "__main__":
    main()
