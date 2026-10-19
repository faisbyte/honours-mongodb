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

Rounds: every round runs all 18 cells, in an order shuffled with
random.Random(seed): one Random, shuffled once per round in round order, so
the seed alone reproduces every round's order. The seed is drawn at random
unless --seed is given, and is recorded. Before the first cell the plan
(seed, cells, each round's order, preflight result, git state, config
hashes) is written to runs/plans/<UTC stamp>__recollect.json; every attempted
cell then appends one line to the matching .progress.jsonl.

Each cell runs harness.collect with --calibrate and --round-index into
measurements/. The
collector's own expect check runs before every cell. The runner stops at the
first cell that exits non-zero (expect refusal, writer or reader abort, any
error), says which cell, and starts nothing after it.

Every completed cell is added to runs/VALID.csv with include=yes, also when
the runner stops early or is interrupted. A failed cell's folder, if one was
created, is reported and left out of VALID.csv.

Usage, from the repo root:
    caffeinate -i .venv/bin/python scripts/recollect.py
    caffeinate -i .venv/bin/python scripts/recollect.py --seed 12345
    .venv/bin/python scripts/recollect.py --print-plan --seed 12345   # touches nothing
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import secrets
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from harness import provenance, runs  # noqa: E402

PLANS = REPO / "runs" / "plans"
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


def show(path: Path) -> str:
    """Path relative to the repo when it is inside it, absolute otherwise."""
    return str(path.relative_to(REPO)) if path.is_relative_to(REPO) else str(path)


def round_orders(n_cells: int, rounds: int, seed: int) -> dict[int, list[int]]:
    """Cell indices per round, each round shuffled from one seeded Random."""
    rng = random.Random(seed)
    orders = {}
    for r in range(1, rounds + 1):
        order = list(range(n_cells))
        rng.shuffle(order)
        orders[r] = order
    return orders


def write_plan(path: Path, cells: list[dict], orders: dict, seed: int,
               pre: dict) -> None:
    configs = {}
    for name in sorted({c["config"] for c in cells}):
        f = REPO / "config" / f"{name}.yaml"
        w = load_cfg(name)["workload"]
        configs[name] = {"sha256": hashlib.sha256(f.read_bytes()).hexdigest(),
                         "duration_s": w["duration_s"],
                         "write_interval_ms": w["write_interval_ms"],
                         "write_concern": w["write_concern"]}
    plan = {"created_utc": datetime.now(timezone.utc).isoformat(),
            "seed": seed,
            "rounds": len(orders),
            "shuffle": "random.Random(seed); one shuffle of range(n_cells) per round, in round order",
            "cells": [{"index": i, **c} for i, c in enumerate(cells)],
            "order": {str(r): [{"position": k + 1, "cell": i, **cells[i]}
                               for k, i in enumerate(o)] for r, o in orders.items()},
            "preflight": pre,
            "git": provenance.git_state(REPO),
            "configs": configs}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(plan, indent=2))
    print(f"[recollect] plan written to {show(path)}")


def log_progress(path: Path, entry: dict) -> None:
    with path.open("a") as fh:
        fh.write(json.dumps(entry, default=str) + "\n")


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
    rows = runs.load(runs.VALID_CSV)
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
    ap.add_argument("--rounds", type=int, default=2)
    ap.add_argument("--seed", type=int, default=None,
                    help="shuffle seed; drawn at random and recorded if omitted")
    ap.add_argument("--print-plan", action="store_true",
                    help="print the cells and each round's order, then exit; "
                         "touches nothing")
    args = ap.parse_args()
    if args.rounds < 1:
        raise SystemExit("[args] --rounds must be at least 1")

    cells = cell_plan()
    seed = args.seed if args.seed is not None else secrets.randbelow(2**32)
    orders = round_orders(len(cells), args.rounds, seed)
    total = len(cells) * args.rounds
    dur = max(load_cfg(c)["workload"]["duration_s"]
              for c in {c["config"] for c in cells})
    print(f"[recollect] {len(cells)} cells x {args.rounds} rounds = {total} runs, "
          f"seed {seed}, about {total * (dur + OVERHEAD_S) / 60:.0f} min")
    for r, order in orders.items():
        print(f"  round {r}:")
        for k, i in enumerate(order, 1):
            print(f"    {k:2d}  {cells[i]['config']:14} {cells[i]['target']}")
    if args.print_plan:
        if args.seed is None:
            print("[recollect] no --seed given: a real run draws its own seed, "
                  "so its order will differ from this one")
        return

    pre = preflight()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    plan_path = PLANS / f"{stamp}__recollect.json"
    progress = PLANS / f"{stamp}__recollect.progress.jsonl"
    write_plan(plan_path, cells, orders, seed, pre)

    completed: list[Path] = []
    failed = None
    n = 0
    try:
        for r, order in orders.items():
            for k, i in enumerate(order, 1):
                cell = cells[i]
                n += 1
                print(f"\n[recollect] run {n}/{total}  round {r} position {k}/{len(order)}: "
                      f"{cell['config']} {cell['target']}")
                started = datetime.now(timezone.utc).isoformat()
                rc, run_dir, interrupted = run_cell(cell, r)
                ok = rc == 0 and not interrupted
                log_progress(progress, {
                    "round": r, "position": k, "cell": i, **cell,
                    "started_utc": started,
                    "finished_utc": datetime.now(timezone.utc).isoformat(),
                    "rc": rc, "interrupted": interrupted,
                    "run_id": run_dir.name if run_dir else None,
                    "status": "ok" if ok else "failed"})
                if not ok:
                    failed = {**cell, "round": r, "position": k, "rc": rc,
                              "run_dir": run_dir, "interrupted": interrupted}
                    break
                completed.append(run_dir)
            if failed:
                break
    finally:
        if completed:
            add_to_valid(completed)

    print(f"\n[recollect] {len(completed)}/{total} runs completed. Plan: "
          f"{show(plan_path)}, progress: {show(progress)}")
    if failed:
        print(f"[recollect] STOPPED at round {failed['round']} position "
              f"{failed['position']}: {failed['config']} {failed['target']}, "
              f"exit {failed['rc']}"
              + (" (interrupted)" if failed["interrupted"] else ""))
        if failed["run_dir"]:
            print(f"[recollect] its folder {failed['run_dir'].name} is not in VALID.csv")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
