#!/usr/bin/env python3
"""Describe the hardware and execution setup behind a set of runs.

Reads only what each run already recorded in provenance/ and manifest.json,
and writes SETUP.md. Nothing is typed by hand, so the document always matches
the data it describes.

It also checks the runs against each other: if the MongoDB version, host or
replica set shape changed between runs, that is flagged, because it would make
the runs not directly comparable.

Usage:
    python -m harness.describe_setup measurements/2026*__threaded-*
"""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def g(d, *keys, default=None):
    """Safe nested get."""
    for k in keys:
        if isinstance(d, dict) and k in d:
            d = d[k]
        elif isinstance(d, list) and isinstance(k, int) and k < len(d):
            d = d[k]
        else:
            return default
    return d


def load_json(p: Path):
    try:
        return json.loads(p.read_text())
    except Exception:  # noqa: BLE001
        return {}


def sh(cmd: list[str]) -> str:
    try:
        return subprocess.run(cmd, capture_output=True, text=True,
                              timeout=20).stdout.strip()
    except Exception:  # noqa: BLE001
        return ""


def table(headers: list[str], rows: list[list]) -> str:
    out = ["| " + " | ".join(headers) + " |",
           "|" + "|".join("---" for _ in headers) + "|"]
    for r in rows:
        out.append("| " + " | ".join("" if v is None else str(v) for v in r) + " |")
    return "\n".join(out)


def load_run(d: Path) -> dict:
    prov = d / "provenance"
    return {
        "dir": d,
        "manifest": load_json(d / "manifest.json"),
        "client": load_json(prov / "client_env.json"),
        "nodes": load_json(prov / "nodes.json") or [],
        "repl": load_json(prov / "repl_before.json"),
        "status": load_json(prov / "server_status_before.json"),
        "calib": load_json(prov / "calibration.json"),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dirs", nargs="+", type=Path)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--no-live", action="store_true",
                    help="skip live macOS hardware queries (disk, RAM)")
    args = ap.parse_args()

    runs = [load_run(d) for d in sorted(args.run_dirs)
            if (d / "manifest.json").exists()]
    if not runs:
        raise SystemExit("no run directories with manifest.json found")

    first = runs[0]
    c = first["client"]
    node0 = first["nodes"][0] if first["nodes"] else {}
    hi = node0.get("hostInfo", {})
    cfg = g(first["repl"], "replSetGetConfig", "config", default={})

    md = []
    md.append("# Hardware and execution setup\n")
    md.append(f"Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC} "
              f"from {len(runs)} run directories.\n")

    # ---------------------------------------------------------------- topology
    md.append("## Topology\n")
    md.append(
        "All four `mongod` processes and the measurement client run on one "
        "physical machine and talk over loopback (127.0.0.1). There is no real "
        "network between replicas, so replication delay here reflects "
        "process scheduling, oplog fetching and applying, and disk, not "
        "network distance.\n\n"
        "The client is a single Python process. Writer and reader run on "
        "separate threads inside it, so every timestamp comes from one "
        "monotonic clock (`time.perf_counter_ns`) and write and read times are "
        "directly comparable with no clock skew.\n")

    # ---------------------------------------------------------------- hardware
    md.append("## Host hardware\n")
    live_disk, live_mem = "", ""
    if not args.no_live and platform.system() == "Darwin":
        mem = sh(["sysctl", "-n", "hw.memsize"])
        live_mem = f"{int(mem) / 1024**3:.0f} GB" if mem.isdigit() else ""
        disk = sh(["system_profiler", "SPStorageDataType"])
        media = [ln.split(":", 1)[1].strip() for ln in disk.splitlines()
                 if "Medium Type" in ln or "Device Name" in ln]
        live_disk = ", ".join(dict.fromkeys(media))

    md.append(table(["Property", "Value"], [
        ["Hostname", c.get("hostname")],
        ["CPU", g(c, "sysctl_cpu", "stdout") or c.get("processor")],
        ["Logical cores", c.get("cpu_count") or g(hi, "system", "numCores")],
        ["Memory", live_mem or (f"{g(hi, 'system', 'memSizeMB')} MB"
                                if g(hi, "system", "memSizeMB") else None)],
        ["Architecture", c.get("machine") or g(hi, "system", "cpuArch")],
        ["OS", c.get("platform")],
        ["Storage", live_disk or "not captured (run without --no-live on the host)"],
        ["Power source at run time", (g(c, "power_source", "stdout") or "")
         .splitlines()[0] if g(c, "power_source", "stdout") else None],
    ]))
    md.append("")

    # ---------------------------------------------------------------- software
    md.append("## Software\n")
    md.append(table(["Component", "Version"], [
        ["MongoDB", g(node0, "buildInfo", "version")],
        ["Feature compatibility version",
         g(node0, "featureCompatibilityVersion", "featureCompatibilityVersion", "version")],
        ["Python", (c.get("python") or "").split(" ")[0]],
        ["PyMongo", c.get("pymongo")],
    ]))
    md.append("")

    # ---------------------------------------------------------------- nodes
    md.append("## MongoDB nodes\n")
    cache = g(first["status"], "wiredTiger", "cache", "maximum bytes configured")
    node_rows = []
    for n in first["nodes"]:
        p = g(n, "getCmdLineOpts", "parsed", default={})
        node_rows.append([
            n.get("host"),
            g(p, "net", "bindIp"),
            g(p, "storage", "dbPath"),
            g(p, "storage", "engine") or "wiredTiger (default)",
            g(p, "storage", "wiredTiger", "engineConfig", "cacheSizeGB") or "default",
        ])
    md.append(table(["Host", "Bind IP", "dbPath", "Storage engine",
                     "WT cache (GB)"], node_rows))
    if cache:
        md.append(f"\nWiredTiger cache configured on the primary: "
                  f"{int(cache) / 1024**3:.2f} GB. No explicit cache limit is "
                  "set, so each of the four processes sizes its cache as if it "
                  "owned the machine.\n")

    # ---------------------------------------------------------------- replica set
    md.append("## Replica set configuration\n")
    members = cfg.get("members", [])
    md.append(table(["Member", "Priority", "Votes", "Hidden",
                     "secondaryDelaySecs"], [
        [m.get("host"), m.get("priority"), m.get("votes"), m.get("hidden"),
         m.get("secondaryDelaySecs", 0)] for m in members]))
    s = cfg.get("settings", {})
    md.append("")
    md.append(table(["Setting", "Value"], [
        ["Replica set name", cfg.get("_id")],
        ["Members", len(members)],
        ["Majority (of voting members)",
         sum(1 for m in members if m.get("votes", 1)) // 2 + 1 if members else None],
        ["writeConcernMajorityJournalDefault",
         cfg.get("writeConcernMajorityJournalDefault")],
        ["heartbeatIntervalMillis", s.get("heartbeatIntervalMillis")],
        ["electionTimeoutMillis", s.get("electionTimeoutMillis")],
        ["chainingAllowed", s.get("chainingAllowed")],
    ]))
    md.append("\nNote: `writeConcernMajorityJournalDefault` only applies when a "
              "write concern does not set `j` explicitly.\n")

    # ---------------------------------------------------------------- executions
    md.append("## Executions\n")
    md.append("One row per run directory. In isolated mode each run measures "
              "one read path with its own writer, so read paths are never "
              "sampled at the same instant.\n")
    rows = []
    for r in runs:
        m = r["manifest"]
        wl = g(m, "config", "workload", default={})
        wc = wl.get("write_concern", {})
        primary = g(r["repl"], "hello", "primary")
        totals = m.get("totals", {})
        reads = totals.get("reads")
        rows.append([
            r["dir"].name.split("__")[0],
            g(m, "config", "experiment", "name"),
            m.get("mode", "pre-v2"),
            m.get("target") or ("all" if reads else None),
            wc.get("w"), wc.get("j"),
            wl.get("duration_s"),
            wl.get("write_interval_ms"),
            wl.get("read_interval_us"),
            primary,
            totals.get("writes"),
            sum(reads.values()) if isinstance(reads, dict) else reads,
        ])
    md.append(table(["Run", "Experiment", "Mode", "Target", "w", "j",
                     "Duration (s)", "Write every (ms)", "Read every (µs)",
                     "Primary", "Writes", "Reads"], rows))
    md.append("")

    # ---------------------------------------------------------------- calibration
    md.append("## Measurement resolution\n")
    md.append("Round-trip time of a read that cannot be stale, measured before "
              "each run. Effects smaller than this are below what the harness "
              "can resolve.\n")
    cal_rows = []
    for r in runs:
        for tgt, v in (r["calib"] or {}).items():
            if isinstance(v, dict) and v.get("p50_ns"):
                cal_rows.append([r["dir"].name.split("__")[0], tgt,
                                 f"{v['p50_ns'] / 1000:.0f}",
                                 f"{v['p99_ns'] / 1000:.0f}"])
    md.append(table(["Run", "Target", "p50 (µs)", "p99 (µs)"], cal_rows))
    md.append("")

    # ---------------------------------------------------------------- consistency checks
    md.append("## Consistency checks across runs\n")
    checks = {
        "MongoDB version": {g(r["nodes"], 0, "buildInfo", "version") for r in runs},
        "Hostname": {g(r["client"], "hostname") for r in runs},
        "Replica set members": {len(g(r["repl"], "replSetGetConfig", "config",
                                      "members", default=[])) for r in runs},
        "Primary": {g(r["repl"], "hello", "primary") for r in runs},
    }
    for k, vals in checks.items():
        vals.discard(None)
        flag = "consistent" if len(vals) <= 1 else "**CHANGED between runs**"
        md.append(f"- {k}: {', '.join(map(str, sorted(vals)))} ({flag})")
    md.append("")

    text = "\n".join(md)
    out = args.out or (Path("measurements/merged") /
                       f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}__setup")
    out.mkdir(parents=True, exist_ok=True)
    (out / "SETUP.md").write_text(text)
    print(text)
    print(f"\nwrote {out / 'SETUP.md'}")


if __name__ == "__main__":
    main()