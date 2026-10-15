#!/usr/bin/env python3
"""The list of runs the analysis is allowed to see: runs/VALID.csv.

Analysis scripts never glob measurements/. They select rows from VALID.csv,
so a void or forgotten run cannot slip into a merge just because its folder
name matches a pattern. A run that is not in the file cannot be analysed.

Columns:
    run_id           folder name under measurements/
    config           experiment.name from the run's manifest
    topology         sync sources at the start of the run, "star" or an
                     explicit map such as "27018<27020 27019<27020 27020<27017"
    journal_default  writeConcernMajorityJournalDefault at the start of the run
    power            AC or battery, from pmset at the start of the run
    include          yes / no. Only "yes" rows are selected unless asked.

topology, journal_default and power are read from each run's provenance, never
typed by hand. include is the only column meant for editing.

Usage:
    python -m harness.runs build      append rows for runs not yet listed
    python -m harness.runs check      compare every row with its provenance
    python -m harness.runs list --config a1-chained --topology star
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass, field
from pathlib import Path

from .provenance import topology_label

REPO_ROOT = Path(__file__).resolve().parent.parent
MEASUREMENTS = REPO_ROOT / "measurements"
VALID_CSV = REPO_ROOT / "runs" / "VALID.csv"
COLUMNS = ["run_id", "config", "topology", "journal_default", "power",
           "include"]


@dataclass
class Run:
    row: dict
    path: Path
    _manifest: dict | None = field(default=None, repr=False)

    @property
    def run_id(self) -> str:
        return self.row["run_id"]

    @property
    def manifest(self) -> dict:
        if self._manifest is None:
            self._manifest = json.loads((self.path / "manifest.json").read_text())
        return self._manifest

    @property
    def target(self) -> str | None:
        return self.manifest.get("target")


# --------------------------------------------------------------------------
# deriving a row from provenance
# --------------------------------------------------------------------------

def _load(p: Path) -> dict:
    try:
        return json.loads(p.read_text())
    except Exception:  # noqa: BLE001
        return {}


def _bool(v) -> str:
    return {True: "true", False: "false"}.get(v, "unknown")


def describe(run_dir: Path) -> dict:
    """Build a VALID.csv row for one run folder from what it recorded."""
    manifest = json.loads((run_dir / "manifest.json").read_text())
    prov = run_dir / "provenance"

    topo = _load(prov / "topology_before.json")
    repl = _load(prov / "repl_before.json")
    if topo:
        topology = topo.get("label", "unknown")
        jd = topo.get("writeConcernMajorityJournalDefault")
    else:
        members = (repl.get("replSetGetStatus") or {}).get("members", [])
        topology = topology_label(members)
        jd = ((repl.get("replSetGetConfig") or {}).get("config") or {}).get(
            "writeConcernMajorityJournalDefault")

    env = _load(prov / "client_env.json")
    power = env.get("power")
    if power is None:
        raw = (env.get("power_source") or {}).get("stdout", "")
        power = ("AC" if "AC Power" in raw
                 else "battery" if "Battery Power" in raw else "unknown")

    return {"run_id": run_dir.name,
            "config": manifest["config"]["experiment"]["name"],
            "topology": topology,
            "journal_default": _bool(jd),
            "power": power,
            "include": "yes"}


# --------------------------------------------------------------------------
# reading and selecting
# --------------------------------------------------------------------------

def load(csv_path: Path = VALID_CSV) -> list[dict]:
    if not csv_path.exists():
        raise SystemExit(f"[runs] {csv_path} not found. Create it with "
                         f"python -m harness.runs build")
    with csv_path.open(newline="") as fh:
        rows = list(csv.DictReader(fh))
    if rows and list(rows[0].keys()) != COLUMNS:
        raise SystemExit(f"[runs] {csv_path} has columns {list(rows[0])}, "
                         f"expected {COLUMNS}")
    return rows


def add_selection_args(ap: argparse.ArgumentParser,
                       defaults: dict | None = None) -> None:
    d = defaults or {}
    g = ap.add_argument_group("run selection, from runs/VALID.csv")
    g.add_argument("--runs-csv", type=Path, default=VALID_CSV)
    g.add_argument("--run-id", nargs="+", default=d.get("run_id"),
                   help="exact run ids")
    g.add_argument("--config", nargs="+", default=d.get("config"),
                   help="experiment names, e.g. threaded-w1 a1-chained")
    g.add_argument("--target", nargs="+", default=d.get("target"),
                   help="read target ids, from each run's manifest")
    g.add_argument("--topology", nargs="+", default=d.get("topology"),
                   help='"star" or an explicit map, quoted')
    g.add_argument("--journal-default", choices=["true", "false"],
                   default=d.get("journal_default"))
    g.add_argument("--power", nargs="+", default=d.get("power"),
                   help="AC and/or battery")
    g.add_argument("--include-excluded", action="store_true",
                   help='also select rows marked include=no')


def selected(args: argparse.Namespace, require_filter: bool = False) -> list[Run]:
    rows = load(args.runs_csv)
    known = {r["run_id"] for r in rows}

    if args.run_id:
        unknown = [r for r in args.run_id if r not in known]
        if unknown:
            raise SystemExit(f"[runs] not in {args.runs_csv}: {unknown}. A run "
                             f"must be listed there before it can be analysed.")
    if require_filter and not (args.run_id or args.config):
        raise SystemExit("[runs] give --run-id or --config; refusing to pool "
                         "every listed run into one analysis")

    def keep(r: dict) -> bool:
        if not args.include_excluded and r["include"] != "yes":
            return False
        if args.run_id and r["run_id"] not in args.run_id:
            return False
        if args.config and r["config"] not in args.config:
            return False
        if args.topology and r["topology"] not in args.topology:
            return False
        if args.journal_default and r["journal_default"] != args.journal_default:
            return False
        if args.power and r["power"] not in args.power:
            return False
        return True

    out = []
    for r in rows:
        if not keep(r):
            continue
        run = Run(row=r, path=MEASUREMENTS / r["run_id"])
        if not (run.path / "manifest.json").exists():
            raise SystemExit(f"[runs] {r['run_id']} is listed but "
                             f"{run.path} has no manifest.json")
        if args.target and run.target not in args.target:
            continue
        out.append(run)
    if not out:
        raise SystemExit("[runs] no runs in VALID.csv match the selection")
    return out


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def _run_folders() -> list[Path]:
    return sorted(p for p in MEASUREMENTS.iterdir()
                  if p.is_dir() and p.name[:4].isdigit())


def build(csv_path: Path) -> None:
    rows = load(csv_path) if csv_path.exists() else []
    known = {r["run_id"] for r in rows}
    added, skipped = [], []
    for d in _run_folders():
        if d.name in known:
            continue
        if not (d / "manifest.json").exists():
            skipped.append(d.name)
            continue
        added.append(describe(d))
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows + added)
    for r in added:
        print(f"[runs] added {r['run_id']}  topology={r['topology']}  "
              f"jd={r['journal_default']}  power={r['power']}")
    for s in skipped:
        print(f"[runs] skipped {s}: no manifest.json")
    print(f"[runs] {len(rows)} existing rows kept, {len(added)} added -> "
          f"{csv_path}")


def check(csv_path: Path) -> int:
    problems = []
    rows = load(csv_path)
    for r in rows:
        d = MEASUREMENTS / r["run_id"]
        if not (d / "manifest.json").exists():
            problems.append(f"{r['run_id']}: listed but no manifest.json")
            continue
        derived = describe(d)
        for col in ("config", "topology", "journal_default", "power"):
            if derived[col] != r[col]:
                problems.append(f"{r['run_id']}: {col} is {r[col]!r} in the "
                                f"csv but {derived[col]!r} in provenance")
    listed = {r["run_id"] for r in rows}
    for d in _run_folders():
        if d.name not in listed:
            problems.append(f"{d.name}: in measurements/ but not listed")
    for p in problems:
        print(f"[runs] {p}")
    print(f"[runs] {len(rows)} rows, {len(problems)} problem(s)")
    return 1 if problems else 0


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("build", "check"):
        sub.add_parser(name).add_argument("--runs-csv", type=Path,
                                          default=VALID_CSV)
    add_selection_args(sub.add_parser("list"))
    args = ap.parse_args()

    if args.cmd == "build":
        build(args.runs_csv)
    elif args.cmd == "check":
        raise SystemExit(check(args.runs_csv))
    else:
        for run in selected(args):
            r = run.row
            print(f"{r['run_id']:64} {r['topology']:32} jd={r['journal_default']:5} "
                  f"{r['power']:7} include={r['include']}")


if __name__ == "__main__":
    main()
