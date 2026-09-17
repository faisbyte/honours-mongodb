#!/usr/bin/env python3
"""Turn a raw run directory into freshness curves and summary tables.

Version 2. Changes from v1:

  * In-flight reads are no longer discarded. With independent threads, many
    reads land between a write's submit and its acknowledgement, and those are
    exactly the reads v1 could never collect. They are kept for the
    from-write-start curve (where the anchor is well defined) and excluded
    from the from-ack curve (where the ack has not happened yet).
  * Error rates per target are reported instead of silently dropped.
  * Read latency distributions are reported, which is where a causal read
    should show its cost: it trades staleness for waiting.
  * Several run directories can be given at once and are analysed as one
    sample. Isolated runs measure one target per process, so a full picture of
    every target now lives across several directories rather than one.

Nothing here writes back into the run's raw files.

Definitions
-----------
A read R is FRESH with respect to write W if it returns W's sequence number,
where W is the most recent write preceding R. Two anchors, both computed:

    elapsed_from_start = R.t_start - W.t_start
    elapsed_from_ack   = R.t_start - W.t_ack

LAG is (current write seq) minus (observed seq): how many writes behind.

Usage:
    python -m harness.analyse measurements/<run_id>
    python -m harness.analyse measurements/<run_id> --bucket-us 2000 --min-n 5
    python -m harness.analyse measurements/<run_a> measurements/<run_b> ...
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


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
    """Wilson score interval. The normal approximation is useless here because
    p sits at 0 or 1 in most buckets."""
    if n == 0:
        return (float("nan"), float("nan"))
    p = k / n
    d = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / d
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, centre - half), min(1.0, centre + half))


def join(reads: pd.DataFrame, writes: pd.DataFrame) -> pd.DataFrame:
    """Attach to each read the most recent write that preceded it.

    Two joins: one on write acknowledgement, one on write submission. When
    they disagree, a write was in flight at the moment of the read. Those reads
    are flagged, not dropped: with independent threads they are a large and
    genuinely interesting share of the sample.
    """
    w = writes.sort_values("t_ack_ns")[["seq", "t_start_ns", "t_ack_ns",
                                        "latency_ns"]].rename(
        columns={"seq": "acked_seq", "t_start_ns": "w_start_ns",
                 "t_ack_ns": "w_ack_ns", "latency_ns": "w_latency_ns"})
    s = writes.sort_values("t_start_ns")[["seq", "t_start_ns"]].rename(
        columns={"seq": "submitted_seq", "t_start_ns": "sub_start_ns"})

    r = reads.sort_values("t_start_ns").copy()
    df = pd.merge_asof(r, w, left_on="t_start_ns", right_on="w_ack_ns",
                       direction="backward")
    df = pd.merge_asof(df, s, left_on="t_start_ns", right_on="sub_start_ns",
                       direction="backward")

    df["in_flight"] = df["submitted_seq"] != df["acked_seq"]
    df["elapsed_from_ack_ns"] = df["t_start_ns"] - df["w_ack_ns"]
    df["elapsed_from_start_ns"] = df["t_start_ns"] - df["sub_start_ns"]

    # Fresh is judged against the most recently SUBMITTED write: a read during
    # the write window that already sees the new value is fresh, and that is a
    # real observation the cycle-based harness could not make.
    df["fresh"] = df["observed_seq"] == df["submitted_seq"]
    df["fresh_vs_acked"] = df["observed_seq"] == df["acked_seq"]
    df["lag_writes"] = df["submitted_seq"] - df["observed_seq"]
    return df


def curve(df: pd.DataFrame, bucket_us: int, min_n: int,
          anchor: str = "ack") -> pd.DataFrame:
    col = f"elapsed_from_{anchor}_ns"
    d = df[df["error"].isna() & ~df["warmup"]].copy()
    if anchor == "ack":
        # The acknowledgement has not happened for an in-flight read, so the
        # anchor is undefined. Judge against the acked write.
        d = d[~d["in_flight"]]
        d["is_fresh"] = d["fresh_vs_acked"]
    else:
        d["is_fresh"] = d["fresh"]
    d = d[d[col] >= 0]
    d["bucket_us"] = (d[col] // (bucket_us * 1000)) * bucket_us

    g = (d.groupby(["target", "bucket_us"])["is_fresh"]
           .agg(["sum", "count"])
           .rename(columns={"sum": "n_fresh", "count": "n"})
           .reset_index())
    g["p_fresh"] = g["n_fresh"] / g["n"]
    ci = g.apply(lambda r: wilson(int(r["n_fresh"]), int(r["n"])), axis=1)
    g["ci_low"] = [c[0] for c in ci]
    g["ci_high"] = [c[1] for c in ci]
    g["adequate"] = g["n"] >= min_n
    return g.sort_values(["target", "bucket_us"])


def headline(g: pd.DataFrame) -> pd.DataFrame:
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


def errors(df: pd.DataFrame) -> pd.DataFrame:
    """Failures are data. Under load MongoDB returns errors, and which ones
    and how often is part of the result."""
    rows = []
    for target, sub in df[~df["warmup"]].groupby("target"):
        failed = sub[sub["error"].notna()]
        kinds = failed["error"].str.split(":").str[0].value_counts().to_dict()
        rows.append({"target": target, "reads": len(sub),
                     "errors": len(failed),
                     "error_rate": len(failed) / len(sub) if len(sub) else 0.0,
                     "kinds": json.dumps(kinds)})
    return pd.DataFrame(rows)


def latency(df: pd.DataFrame) -> pd.DataFrame:
    """A causal read trades staleness for waiting, so its latency is the other
    half of the story and belongs beside the freshness numbers."""
    d = df[df["error"].isna() & ~df["warmup"]]
    q = d.groupby("target")["latency_ns"].quantile([0.5, 0.95, 0.99]).unstack()
    q.columns = ["p50_ns", "p95_ns", "p99_ns"]
    q["mean_ns"] = d.groupby("target")["latency_ns"].mean()
    q["max_ns"] = d.groupby("target")["latency_ns"].max()
    return q.reset_index()


def monotonic_violations(df: pd.DataFrame) -> pd.DataFrame:
    """Consecutive reads on one target where the observed value goes backwards.
    The MongoDB analogue of CIDR Table 2.

    Grouped by run as well as by target: two reads either side of a run
    boundary are not consecutive reads, and comparing across it would invent
    regressions out of nothing.
    """
    rows = []
    for (_run_id, target), sub in df[~df["warmup"]].groupby(["run_id", "target"]):
        sub = sub.sort_values("t_start_ns")
        prev = sub["observed_seq"].shift(1)
        valid = prev.notna() & sub["observed_seq"].notna()
        regress = valid & (sub["observed_seq"] < prev)
        rows.append({"target": target, "pairs": int(valid.sum()),
                     "violations": int(regress.sum())})
    if not rows:
        return pd.DataFrame(columns=["target", "pairs", "violations", "rate"])
    out = (pd.DataFrame(rows)
             .groupby("target", as_index=False)[["pairs", "violations"]].sum())
    out["rate"] = [v / n if n else float("nan")
                   for v, n in zip(out["violations"], out["pairs"])]
    return out


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


def load_run(run_dir: Path) -> tuple[dict, pd.DataFrame]:
    """Join one run's reads to its own writes, tagged with where they came from.

    The join MUST happen per run and never on pooled frames. t_*_ns is
    perf_counter_ns(), whose epoch is per process, so a read from one run and a
    write from another share no clock; merging the raw records first would pair
    them anyway and produce plausible nonsense.
    """
    manifest = json.loads((run_dir / "manifest.json").read_text())
    if "mode" not in manifest:
        raise SystemExit(f"[analyse] {run_dir} has no mode in its manifest, so "
                         f"it predates this harness version and cannot be "
                         f"analysed here.")
    df = join(_read_jsonl(run_dir, "reads.jsonl"),
              _read_jsonl(run_dir, "writes.jsonl"))
    df["run_id"] = manifest["run_id"]
    df["mode"] = manifest["mode"]
    df["round_index"] = manifest.get("round_index", 1)
    return manifest, df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dirs", type=Path, nargs="+")
    ap.add_argument("--out", type=Path, default=None,
                    help="where derived outputs go; defaults to "
                         "<run_dir>/derived for a single run and to "
                         "measurements/merged/<stamp>__<name> for a merge")
    ap.add_argument("--bucket-us", type=int, default=None)
    ap.add_argument("--min-n", type=int, default=None)
    args = ap.parse_args()

    loaded = [load_run(d) for d in args.run_dirs]
    manifests = [m for m, _ in loaded]

    # Isolated and concurrent runs are not the same measurement: concurrent
    # runs carry the latency cost of every other reader in the process. Pooling
    # them would average two different experiments into one curve.
    if len({m["mode"] for m in manifests}) > 1:
        listing = "\n  ".join(f"{m['mode']:<10} {d}"
                              for m, d in zip(manifests, args.run_dirs))
        raise SystemExit(f"[analyse] refusing to merge isolated and concurrent "
                         f"runs into one curve:\n  {listing}")

    acfg = manifests[0]["config"].get("analysis", {})
    bucket_us = args.bucket_us or acfg.get("bucket_us", 500)
    min_n = args.min_n or acfg.get("min_samples_per_bucket", 30)
    if len({json.dumps(m["config"].get("analysis", {}), sort_keys=True)
            for m in manifests}) > 1:
        print(f"[analyse] WARNING runs disagree on analysis settings; using "
              f"bucket_us={bucket_us}, min_n={min_n} from "
              f"{manifests[0]['run_id']}")

    df = pd.concat([d for _, d in loaded], ignore_index=True)

    if len(loaded) == 1:
        out = args.out or (args.run_dirs[0] / "derived")
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        name = manifests[0]["config"]["experiment"]["name"]
        out = args.out or (args.run_dirs[0].parent / "merged" /
                           f"{stamp}__{name}")
    out.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out / "reads_joined.parquet", index=False)

    if len(loaded) > 1:
        pd.DataFrame([{"run_id": m["run_id"], "mode": m["mode"],
                       "target": m.get("target"),
                       "round_index": m.get("round_index", 1),
                       "path": str(d)}
                      for m, d in zip(manifests, args.run_dirs)]
                     ).to_csv(out / "sources.csv", index=False)

    for anchor in ("ack", "start"):
        g = curve(df, bucket_us, min_n, anchor=anchor)
        g.to_csv(out / f"curve_{anchor}.csv", index=False)
        headline(g).to_csv(out / f"headline_{anchor}.csv", index=False)
        plot(g, out / f"freshness_curve_{anchor}.png", anchor)

    errors(df).to_csv(out / "errors.csv", index=False)
    latency(df).to_csv(out / "read_latency.csv", index=False)
    monotonic_violations(df).to_csv(out / "monotonic_reads.csv", index=False)
    df[~df["warmup"]].groupby("target")["lag_writes"].describe().to_csv(
        out / "lag_writes.csv")

    inflight = df[~df["warmup"]]["in_flight"].mean()
    if len(loaded) > 1:
        print(f"[analyse] merged {len(loaded)} runs, mode="
              f"{manifests[0]['mode']}")
    print(headline(curve(df, bucket_us, min_n, "ack")).to_string(index=False))
    print(f"\nin-flight reads: {inflight:.1%} of sample "
          f"(invisible to the v1 cycle harness)")
    print(errors(df).to_string(index=False))
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
