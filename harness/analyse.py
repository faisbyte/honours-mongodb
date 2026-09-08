#!/usr/bin/env python3
"""Turn a raw run directory into freshness curves and summary tables.

Nothing here writes back into the run's raw files. Re-running analysis with a
different staleness definition is expected and should be cheap.

Definitions
-----------
A read R is FRESH with respect to write W if the value it returns carries W's
sequence number, where W is the most recent write preceding R. "Preceding" has
two defensible anchors and we compute both, because CIDR 2011 uses both in
different places (Table 1 measures from write start, Figure 2 from write
completion):

    elapsed_from_start = R.t_start - W.t_start
    elapsed_from_ack   = R.t_start - W.t_ack

We also record LAG = (current write seq) - (observed seq), i.e. how many
writes behind the replica is. CIDR's section 3.1 finding was that SimpleDB
replicas were almost never more than one write behind; the same statistic is
worth having here.

Usage:
    python -m harness.analyse measurements/20260908T...__baseline
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
from pathlib import Path

import pandas as pd


# --------------------------------------------------------------------------

def _read_jsonl(run_dir: Path, name: str) -> pd.DataFrame:
    plain, gz = run_dir / name, run_dir / (name + ".gz")
    if gz.exists():
        with gzip.open(gz, "rt") as fh:
            rows = [json.loads(line) for line in fh]
    elif plain.exists():
        rows = [json.loads(line) for line in plain.open()]
    else:
        raise FileNotFoundError(f"{name} not found in {run_dir}")
    return pd.DataFrame(rows)


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson score interval. Normal approximation is useless here because
    p sits at 0 or 1 for most buckets."""
    if n == 0:
        return (float("nan"), float("nan"))
    p = k / n
    d = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / d
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, centre - half), min(1.0, centre + half))


# --------------------------------------------------------------------------

def join(reads: pd.DataFrame, writes: pd.DataFrame) -> pd.DataFrame:
    """Attach to each read the most recent write that preceded it.

    merge_asof on t_ack gives the last acknowledged write; we then look up that
    write's start time separately. Reads that land while a write is in flight
    (t_start < R < t_ack) are flagged rather than silently attributed, because
    "the latest write" is genuinely ambiguous there.
    """
    w = writes.sort_values("t_ack_ns")[
        ["seq", "t_start_ns", "t_ack_ns", "latency_ns", "primary"]
    ].rename(columns={"seq": "write_seq",
                      "t_start_ns": "w_start_ns",
                      "t_ack_ns": "w_ack_ns",
                      "latency_ns": "w_latency_ns"})
    r = reads.sort_values("t_start_ns").copy()

    df = pd.merge_asof(r, w, left_on="t_start_ns", right_on="w_ack_ns",
                       direction="backward")

    nxt = w.rename(columns={"write_seq": "next_seq", "w_start_ns": "next_start_ns"})
    df = pd.merge_asof(df, nxt[["next_start_ns", "next_seq"]].sort_values("next_start_ns"),
                       left_on="t_start_ns", right_on="next_start_ns",
                       direction="backward")

    df["in_flight"] = df["next_seq"] != df["write_seq"]
    df["elapsed_from_ack_ns"] = df["t_start_ns"] - df["w_ack_ns"]
    df["elapsed_from_start_ns"] = df["t_start_ns"] - df["w_start_ns"]
    df["fresh"] = df["observed_seq"] == df["write_seq"]
    df["lag_writes"] = df["write_seq"] - df["observed_seq"]
    return df


def curve(df: pd.DataFrame, bucket_us: int, min_n: int,
          anchor: str = "ack") -> pd.DataFrame:
    col = f"elapsed_from_{anchor}_ns"
    d = df[(df["error"].isna()) & (~df["in_flight"]) & (~df["warmup"])].copy()
    d = d[d[col] >= 0]
    d["bucket_us"] = (d[col] // (bucket_us * 1000)) * bucket_us

    g = d.groupby(["target", "bucket_us"])["fresh"].agg(["sum", "count"])
    g = g.rename(columns={"sum": "n_fresh", "count": "n"}).reset_index()
    g["p_fresh"] = g["n_fresh"] / g["n"]
    ci = g.apply(lambda r: wilson(int(r["n_fresh"]), int(r["n"])), axis=1)
    g["ci_low"] = [c[0] for c in ci]
    g["ci_high"] = [c[1] for c in ci]
    g["adequate"] = g["n"] >= min_n
    return g.sort_values(["target", "bucket_us"])


def headline(g: pd.DataFrame) -> pd.DataFrame:
    """Per-target scalars: the CIDR-style 'time to freshness' numbers."""
    rows = []
    for target, sub in g[g["adequate"]].groupby("target"):
        sub = sub.sort_values("bucket_us")
        over99 = sub[sub["p_fresh"] >= 0.99]
        under100 = sub[sub["p_fresh"] < 1.0]
        rows.append({
            "target": target,
            "n_reads": int(sub["n"].sum()),
            "p_fresh_overall": sub["n_fresh"].sum() / sub["n"].sum(),
            "first_bucket_us_p99": int(over99["bucket_us"].iloc[0]) if len(over99) else None,
            "last_bucket_us_below_1": int(under100["bucket_us"].iloc[-1]) if len(under100) else None,
        })
    return pd.DataFrame(rows)


def monotonic_violations(df: pd.DataFrame) -> pd.DataFrame:
    """Consecutive reads on the same target where the observed value goes
    backwards. This is the MongoDB analogue of CIDR's Table 2."""
    rows = []
    for target, sub in df[~df["warmup"]].groupby("target"):
        sub = sub.sort_values("t_start_ns")
        prev = sub["observed_seq"].shift(1)
        valid = prev.notna() & sub["observed_seq"].notna()
        regress = valid & (sub["observed_seq"] < prev)
        rows.append({
            "target": target,
            "pairs": int(valid.sum()),
            "violations": int(regress.sum()),
            "rate": float(regress.sum() / valid.sum()) if valid.sum() else float("nan"),
        })
    return pd.DataFrame(rows)


def plot(g: pd.DataFrame, out: Path, anchor: str) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 5))
    for target, sub in g[g["adequate"]].groupby("target"):
        sub = sub.sort_values("bucket_us")
        ax.plot(sub["bucket_us"] / 1000.0, sub["p_fresh"], label=target, lw=1.2)
        ax.fill_between(sub["bucket_us"] / 1000.0, sub["ci_low"],
                        sub["ci_high"], alpha=0.15, linewidth=0)
    ax.set_xlabel(f"time elapsed from write {anchor} until read start (ms)")
    ax.set_ylabel("P(read observes freshest value)")
    ax.set_ylim(-0.02, 1.02)
    ax.grid(alpha=0.3)
    ax.legend(fontsize=8, ncol=2)
    fig.tight_layout()
    fig.savefig(out, dpi=160)


# --------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir", type=Path)
    ap.add_argument("--bucket-us", type=int, default=None)
    ap.add_argument("--min-n", type=int, default=None)
    args = ap.parse_args()

    run_dir: Path = args.run_dir
    manifest = json.loads((run_dir / "manifest.json").read_text())
    acfg = manifest["config"].get("analysis", {})
    bucket_us = args.bucket_us or acfg.get("bucket_us", 100)
    min_n = args.min_n or acfg.get("min_samples_per_bucket", 30)

    reads = _read_jsonl(run_dir, "reads.jsonl")
    writes = _read_jsonl(run_dir, "writes.jsonl")
    df = join(reads, writes)

    out = run_dir / "derived"
    out.mkdir(exist_ok=True)
    df.to_parquet(out / "reads_joined.parquet", index=False)

    for anchor in ("ack", "start"):
        g = curve(df, bucket_us, min_n, anchor=anchor)
        g.to_csv(out / f"curve_{anchor}.csv", index=False)
        headline(g).to_csv(out / f"headline_{anchor}.csv", index=False)
        plot(g, out / f"freshness_curve_{anchor}.png", anchor)

    monotonic_violations(df).to_csv(out / "monotonic_reads.csv", index=False)

    df[~df["warmup"]].groupby("target")["lag_writes"].describe().to_csv(
        out / "lag_writes.csv")

    print(headline(curve(df, bucket_us, min_n, "ack")).to_string(index=False))
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
