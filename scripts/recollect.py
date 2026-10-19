#!/usr/bin/env python3
"""Trimmed RQ1/RQ2 re-collect: 18 cells, one collector process per cell.

Cells:
    threaded-w1    all 10 targets
    threaded-wmaj  the six rs-* targets
    threaded-w2    rs-primary-local
    threaded-w3    rs-primary-local

Preflight, before the first cell:
    scripts/start.sh unchained   starts the set if it is down; chainingAllowed false
    waits up to 3 min for a star layout with journal default on
    drops freshness.nudge, left over from the 15 Oct chain setup
Power is recorded, never checked.

Each cell runs harness.collect with --calibrate into measurements/. The
collector's own expect check runs before every cell. The runner stops at the
first cell that exits non-zero (expect refusal, writer or reader abort, any
error), says which cell, and starts nothing after it.

Every completed cell is added to runs/VALID.csv with include=yes, also when
the runner stops early or is interrupted. A failed cell's folder, if one was
created, is reported and left out of VALID.csv.

Usage, from the repo root:
    caffeinate -i .venv/bin/python scripts/recollect.py
    .venv/bin/python scripts/recollect.py --print-plan    # show cells, run nothing
"""

from __future__ import annotations

import argparse
import csv
import subprocess
import sys
import time
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from harness import provenance, runs  # noqa: E402

GRACE_S = 30.0          # time a child gets to flush its data after Ctrl-C
OVERHEAD_S = 20         # per-run startup, calibration and provenance, roughly

RS_TARGETS = ["rs-primary-local", "rs-primary-linearizable", "rs-secondary-local",
              "rs-secondary-majority", "rs-secondary-causal-majority",
              "rs-secondary-causal-local"]


def load_cfg(name: str) -> dict:
    return yaml.safe_load((REPO / "config" / f"{name}.yaml").read_text())


def cell_plan() -> list[dict]:
    w1 = load_cfg("threaded-w1")
    cells = [{"config": "threaded-w1", "target": t["id"]} for t in w1["targets"]]
    cells += [{"config": "threaded-wmaj", "target": t} for t in RS_TARGETS]
    cells += [{"config": c, "target": "rs-primary-local"}
              for c in ("threaded-w2", "threaded-w3")]
    for c in cells:
        ids = [t["id"] for t in load_cfg(c["config"])["targets"]]
        if c["target"] not in ids:
            raise SystemExit(f"[plan] {c['target']} is not a target of {c['config']}")
    if len(cells) != 18:
        raise SystemExit(f"[plan] expected 18 cells, got {len(cells)}")
    return cells


def preflight() -> dict:
    print("[preflight] scripts/start.sh unchained")
    if subprocess.run([str(REPO / "scripts" / "start.sh"), "unchained"],
                      cwd=REPO).returncode != 0:
        raise SystemExit("[preflight] scripts/start.sh unchained failed")

    from pymongo import MongoClient
    rs = load_cfg("threaded-w1")["replica_set"]
    uri = f"mongodb://{','.join(rs['seed_hosts'])}/?replicaSet={rs['name']}"
    with MongoClient(uri, serverSelectionTimeoutMS=5000) as client:
        deadline = time.monotonic() + 180
        while True:
            topo = provenance.topology(client)
            if (topo["label"] == "star"
                    and topo["writeConcernMajorityJournalDefault"] is True):
                break
            if time.monotonic() > deadline:
                raise SystemExit(
                    f"[preflight] after 3 min: topology {topo['label']!r}, "
                    f"journal default {topo['writeConcernMajorityJournalDefault']!r}; "
                    f"need star and true")
            time.sleep(5)
        db = client["freshness"]
        nudge = "nudge" in db.list_collection_names()
        if nudge:
            db.drop_collection("nudge")
    power = provenance.power_source()["source"]
    print(f"[preflight] topology star, journal default on, "
          f"freshness.nudge {'dropped' if nudge else 'absent'}, power {power}")
    if power != "AC":
        print("[preflight] note: not on AC power; recorded, not blocking")
    return {"topology": topo["label"],
            "journal_default": topo["writeConcernMajorityJournalDefault"],
            "chaining_allowed": topo["chainingAllowed"],
            "nudge_dropped": nudge, "power": power}


def run_cell(cell: dict, round_index: int | None) -> tuple[int, Path | None, bool]:
    """Run one cell. Returns (exit code, new run folder or None, interrupted)."""
    before = {p.name for p in runs.MEASUREMENTS.iterdir()}
    cmd = [sys.executable, "-m", "harness.collect",
           "--config", str(REPO / "config" / f"{cell['config']}.yaml"),
           "--target", cell["target"], "--calibrate"]
    if round_index is not None:
        cmd += ["--round-index", str(round_index)]
    proc = subprocess.Popen(cmd, cwd=REPO)
    interrupted = False
    try:
        rc = proc.wait()
    except KeyboardInterrupt:
        # The child shares our process group, so it took the SIGINT too and is
        # writing its run folder. Give it time before escalating.
        interrupted = True
        print(f"\n[recollect] interrupted; giving the collector up to "
              f"{GRACE_S:.0f} s to finish writing")
        try:
            rc = proc.wait(timeout=GRACE_S)
        except subprocess.TimeoutExpired:
            proc.terminate()
            rc = proc.wait()
    new = sorted({p.name for p in runs.MEASUREMENTS.iterdir()} - before)
    return rc, (runs.MEASUREMENTS / new[-1]) if new else None, interrupted


def add_to_valid(run_dirs: list[Path]) -> None:
    rows = runs.load()
    known = {r["run_id"] for r in rows}
    added = [runs.describe(d) for d in run_dirs if d.name not in known]
    for r in added:
        r["include"] = "yes"
    with runs.VALID_CSV.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=runs.COLUMNS)
        w.writeheader()
        w.writerows(rows + added)
    print(f"[recollect] {len(added)} run(s) added to {runs.VALID_CSV} with include=yes")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--print-plan", action="store_true",
                    help="print the cells and exit; touches nothing")
    args = ap.parse_args()

    cells = cell_plan()
    dur = max(load_cfg(c)["workload"]["duration_s"]
              for c in {c["config"] for c in cells})
    print(f"[recollect] {len(cells)} cells, about "
          f"{len(cells) * (dur + OVERHEAD_S) / 60:.0f} min")
    for i, c in enumerate(cells, 1):
        print(f"  {i:2d}  {c['config']:14} {c['target']}")
    if args.print_plan:
        return

    preflight()
    completed: list[Path] = []
    failed = None
    try:
        for i, cell in enumerate(cells, 1):
            print(f"\n[recollect] cell {i}/{len(cells)}: {cell['config']} {cell['target']}")
            rc, run_dir, interrupted = run_cell(cell, None)
            if rc != 0 or interrupted:
                failed = {**cell, "position": i, "rc": rc, "run_dir": run_dir,
                          "interrupted": interrupted}
                break
            completed.append(run_dir)
    finally:
        if completed:
            add_to_valid(completed)

    print(f"\n[recollect] {len(completed)}/{len(cells)} cells completed")
    if failed:
        print(f"[recollect] STOPPED at cell {failed['position']}: {failed['config']} "
              f"{failed['target']}, exit {failed['rc']}"
              + (" (interrupted)" if failed["interrupted"] else ""))
        if failed["run_dir"]:
            print(f"[recollect] its folder {failed['run_dir'].name} is not in VALID.csv")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
