#!/usr/bin/env python3
"""Sweep every read target in a config, one collector process at a time.

Isolated collection puts a single reader in each process, so a full picture of
a condition takes one run per target. This runner does that sweep and nothing
else: it starts no threads, opens no connections and writes no measurement
files. Each child produces its own run directory, and analyse.py merges them.

With --rounds greater than one the sweep loops over the whole target list once
per round, rather than finishing every round of one target before moving on.
A transient disturbance on the machine then lands across the targets instead of
entirely on whichever one happened to be running.

Usage:
    python -m harness.run_all --config config/threaded-w1.yaml
    python -m harness.run_all --config config/threaded-w1.yaml --rounds 3
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent

# How long a child gets to flush its data after Ctrl-C before we escalate.
GRACE_S = 30.0


def _hms(seconds: float) -> str:
    s = int(round(seconds))
    h, rem = divmod(s, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h{m:02d}m{s:02d}s" if h else f"{m}m{s:02d}s"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, type=Path)
    ap.add_argument("--out", type=Path, default=REPO_ROOT / "measurements")
    ap.add_argument("--rounds", type=int, default=1,
                    help="sweeps over the full target list; default 1")
    ap.add_argument("--no-calibrate", action="store_true")
    args = ap.parse_args()

    if args.rounds < 1:
        raise SystemExit("[args] --rounds must be at least 1")

    cfg_path = args.config.resolve()
    cfg = yaml.safe_load(cfg_path.read_text())
    mode = cfg["workload"].get("mode", "isolated")
    if mode != "isolated":
        raise SystemExit(f"[config] workload.mode is {mode!r}. This runner "
                         f"sweeps one target per process, which only means "
                         f"anything for 'isolated'; a concurrent run is a "
                         f"single harness.collect invocation.")

    ids = [t["id"] for t in cfg["targets"]]
    duration = float(cfg["workload"]["duration_s"])
    total_runs = len(ids) * args.rounds

    print(f"[sweep] config {cfg_path}")
    print(f"[sweep] {len(ids)} targets x {_hms(duration)} x {args.rounds} "
          f"round(s) = {total_runs} runs")
    print(f"[sweep] estimated wall time {_hms(total_runs * duration)}, "
          f"excluding per-run startup, calibration and provenance")

    results: list[dict] = []
    interrupted = False
    started = time.monotonic()

    for rnd in range(1, args.rounds + 1):
        if interrupted:
            break
        for i, tid in enumerate(ids, start=1):
            print(f"\n[sweep] round {rnd}/{args.rounds}  "
                  f"target {i}/{len(ids)}  run {len(results) + 1}/{total_runs}"
                  f"  {tid}")

            cmd = [sys.executable, "-m", "harness.collect",
                   "--config", str(cfg_path),
                   "--out", str(args.out.resolve()),
                   "--target", tid]
            if not args.no_calibrate:
                cmd.append("--calibrate")
            if args.rounds > 1:
                cmd += ["--round-index", str(rnd)]

            t0 = time.monotonic()
            proc = subprocess.Popen(cmd, cwd=REPO_ROOT)
            try:
                rc = proc.wait()
            except KeyboardInterrupt:
                # The child is in our process group, so it already took the
                # SIGINT and is finalising its own run directory. Give it room
                # to finish writing before escalating.
                interrupted = True
                print(f"\n[sweep] interrupted; giving {tid} up to "
                      f"{_hms(GRACE_S)} to finish writing")
                try:
                    rc = proc.wait(timeout=GRACE_S)
                except subprocess.TimeoutExpired:
                    proc.terminate()
                    try:
                        rc = proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        rc = proc.wait()

            elapsed = time.monotonic() - t0
            results.append({"round": rnd, "target": tid, "rc": rc,
                            "elapsed": elapsed})
            print(f"[sweep] {tid} exit {rc} in {_hms(elapsed)}")
            if interrupted:
                break

    print(f"\n[sweep] {len(results)}/{total_runs} runs in "
          f"{_hms(time.monotonic() - started)}")
    for r in results:
        flag = "ok" if r["rc"] == 0 else f"FAIL({r['rc']})"
        print(f"  r{r['round']}  {flag:<9} {_hms(r['elapsed']):>9}  "
              f"{r['target']}")

    failed = [r for r in results if r["rc"] != 0]
    if failed:
        print(f"[sweep] {len(failed)} run(s) failed")
    if failed or interrupted:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
