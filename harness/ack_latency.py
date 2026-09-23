#!/usr/bin/env python3
"""Write acknowledgement latency, per write concern.

Every write record already carries t_start_ns and t_ack_ns, so no new
collection is needed. This pools writes across run directories, groups them by
the write concern recorded in each run's manifest, and reports the
distribution of (t_ack_ns - t_start_ns).

Why distributions, not means: the gap between w:1 and w:"majority" ack time is
the client-side view of one replication round trip plus journal flush. That is
a system property inferred from measurement, and it is the kind of input a
WARS-style latency model (Bailis et al., PBS) or a simulation needs. Those need
the shape and the tail, not an average.

Usage:
    python -m harness.ack_latency measurements/2026*__threaded-w1__* \
                                  measurements/2026*__threaded-wmaj__*
"""

from __future__ import annotations

import argparse
import gzip
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def load_writes(run_dir: Path) -> pd.DataFrame:
    manifest = json.loads((run_dir / "manifest.json").read_text())
    wc = manifest["config"]["workload"]["write_concern"]["w"]
    name = manifest["config"]["experiment"]["name"]

    gz, plain = run_dir / "writes.jsonl.gz", run_dir / "writes.jsonl"
    opener = gzip.open(gz, "rt") if gz.exists() else plain.open()
    with opener as fh:
        df = pd.DataFrame(json.loads(line) for line in fh)

    df["write_concern"] = f"w:{wc}"
    df["experiment"] = name
    df["run_id"] = run_dir.name
    return df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dirs", nargs="+", type=Path)
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    frames = [load_writes(d) for d in args.run_dirs
              if (d / "manifest.json").exists()]
    df = pd.concat(frames, ignore_index=True)

    total = len(df)
    df = df[df["error"].isna() & ~df.get("warmup", False)]
    df["latency_ms"] = df["latency_ns"] / 1e6
    print(f"[ack] {len(frames)} runs, {total} writes, "
          f"{len(df)} after dropping errors and warmup")

    q = [0.01, 0.10, 0.50, 0.90, 0.95, 0.99, 0.999]
    summary = (df.groupby("write_concern")["latency_ms"]
                 .describe(percentiles=q)
                 .round(3))
    print(summary.to_string())

    out = args.out or (Path("measurements/merged") /
                       f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}__ack_latency")
    out.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out / "ack_latency_summary.csv")
    df[["run_id", "experiment", "write_concern", "seq",
        "latency_ms"]].to_csv(out / "ack_latency_raw.csv", index=False)

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))
    for wc, sub in df.groupby("write_concern"):
        lat = sub["latency_ms"].sort_values().to_numpy()
        n = len(lat)
        ax1.plot(lat, [i / n for i in range(n)], lw=1.5,
                 label=f"{wc} (n={n}, p50={lat[n // 2]:.2f}ms)")
        ax2.hist(lat, bins=100, alpha=0.6, label=wc)

    ax1.set_xscale("log")
    ax1.set_xlabel("write ack latency (ms, log scale)")
    ax1.set_ylabel("cumulative fraction of writes")
    ax1.set_title("CDF")
    ax1.grid(alpha=0.3)
    ax1.legend(fontsize=8)

    ax2.set_xscale("log")
    ax2.set_yscale("log")
    ax2.set_xlabel("write ack latency (ms, log scale)")
    ax2.set_ylabel("count (log scale)")
    ax2.set_title("Distribution")
    ax2.grid(alpha=0.3)
    ax2.legend(fontsize=8)

    fig.tight_layout()
    fig.savefig(out / "ack_latency.png", dpi=160)
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()