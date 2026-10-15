"""Capture everything needed to reproduce (or distrust) a measurement run.

Every function here is best-effort: if a command fails we record the error
string rather than aborting, because a partially documented run is still worth
more than a crashed one.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


def _safe(fn, *args, **kwargs) -> Any:
    try:
        return fn(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001 - deliberately broad
        return {"__error__": f"{type(exc).__name__}: {exc}"}


def _run(cmd: list[str]) -> Any:
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        return {"cmd": " ".join(cmd), "rc": out.returncode,
                "stdout": out.stdout.strip(), "stderr": out.stderr.strip()}
    except Exception as exc:  # noqa: BLE001
        return {"cmd": " ".join(cmd), "__error__": str(exc)}


def git_state(repo_root: Path) -> dict:
    """Commit hash plus whether the working tree was dirty at run time.

    A dirty tree means the committed code is NOT what produced this data, which
    is exactly the thing you want flagged six months later.
    """
    head = _run(["git", "-C", str(repo_root), "rev-parse", "HEAD"])
    status = _run(["git", "-C", str(repo_root), "status", "--porcelain"])
    dirty = bool(status.get("stdout"))
    return {
        "commit": head.get("stdout"),
        "dirty": dirty,
        "dirty_files": status.get("stdout", "").splitlines(),
        "branch": _run(["git", "-C", str(repo_root), "rev-parse",
                        "--abbrev-ref", "HEAD"]).get("stdout"),
    }


def power_source() -> dict:
    """Where the machine draws power from, via pmset -g batt (macOS).

    source is "AC", "battery" or "unknown"; raw keeps the full command
    output. Battery power can change CPU frequency and so the timings.
    """
    raw = _run(["pmset", "-g", "batt"])
    out = raw.get("stdout", "") if isinstance(raw, dict) else ""
    source = ("AC" if "AC Power" in out
              else "battery" if "Battery Power" in out else "unknown")
    return {"source": source, "raw": raw}


def client_env() -> dict:
    import pymongo
    power = power_source()
    return {
        "python": sys.version,
        "python_executable": sys.executable,
        "pymongo": pymongo.version,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "hostname": platform.node(),
        "tz": time.strftime("%Z%z"),
        "cwd": os.getcwd(),
        "sysctl_cpu": _run(["sysctl", "-n", "machdep.cpu.brand_string"]),
        "power": power["source"],
        "power_source": power["raw"],  # macOS: thermal/perf state matters
        "clock_sync": _run(["sntp", "-d", "time.apple.com"]),
    }


def node_info(client, host: str) -> dict:
    """Per-mongod build, config and runtime state."""
    admin = client.admin
    return {
        "host": host,
        "buildInfo": _safe(admin.command, "buildInfo"),
        "hostInfo": _safe(admin.command, "hostInfo"),
        "getCmdLineOpts": _safe(admin.command, "getCmdLineOpts"),
        "featureCompatibilityVersion": _safe(
            admin.command, {"getParameter": 1, "featureCompatibilityVersion": 1}),
        "writeConcernDefaults": _safe(admin.command, "getDefaultRWConcern"),
        # A replica set config field, not a server parameter, so it is read
        # from replSetGetConfig (getParameter fails with "no option found").
        "writeConcernMajorityJournalDefault": _safe(
            lambda: admin.command("replSetGetConfig")["config"].get(
                "writeConcernMajorityJournalDefault")),
    }


def repl_snapshot(client) -> dict:
    admin = client.admin
    return {
        "captured_ns": time.time_ns(),
        "replSetGetStatus": _safe(admin.command, "replSetGetStatus"),
        "replSetGetConfig": _safe(admin.command, "replSetGetConfig"),
        "hello": _safe(admin.command, "hello"),
    }


def topology(client) -> dict:
    """Who each member copies from, plus the settings that decide it.

    Recorded before and after every run. label is the string compared with a
    config's expect.topology and stored in runs/VALID.csv.
    """
    admin = client.admin
    status = _safe(admin.command, "replSetGetStatus")
    config = _safe(admin.command, "replSetGetConfig")
    members = [{"name": m.get("name"), "stateStr": m.get("stateStr"),
                "syncSourceHost": m.get("syncSourceHost") or ""}
               for m in status.get("members", [])]
    conf = config.get("config", {}) if "__error__" not in config else {}
    return {
        "captured_ns": time.time_ns(),
        "members": members,
        "label": topology_label(members),
        "chainingAllowed": (conf.get("settings") or {}).get("chainingAllowed"),
        "writeConcernMajorityJournalDefault": conf.get(
            "writeConcernMajorityJournalDefault"),
        "errors": {k: v["__error__"] for k, v in
                   (("replSetGetStatus", status), ("replSetGetConfig", config))
                   if "__error__" in v},
    }


def _port(host: str | None) -> str:
    return host.rsplit(":", 1)[-1] if host else "-"


def topology_label(members: list) -> str:
    """One string naming who each secondary copies from.

    "star" when every secondary syncs from the primary, otherwise the explicit
    map, e.g. "27018<27020 27019<27020 27020<27017". A secondary with no sync
    source shows as "<-". The same string is used in runs/VALID.csv and in a
    config's expect.topology, so the two can be compared directly.
    """
    if not members:
        return "unknown"
    primary = [_port(m.get("name")) for m in members
               if m.get("stateStr") == "PRIMARY"]
    pairs = sorted((_port(m.get("name")), _port(m.get("syncSourceHost")))
                   for m in members if m.get("stateStr") != "PRIMARY")
    if len(primary) == 1 and all(src == primary[0] for _, src in pairs):
        return "star"
    return " ".join(f"{p}<{s}" for p, s in pairs)


def server_status(client) -> dict:
    """Trimmed serverStatus. The full doc is enormous and mostly noise."""
    full = _safe(client.admin.command, "serverStatus")
    if "__error__" in full:
        return full
    keep = ["host", "version", "process", "uptimeMillis", "localTime",
            "opcounters", "opcountersRepl", "repl", "connections",
            "network", "metrics", "wiredTiger", "electionMetrics"]
    out = {k: full.get(k) for k in keep if k in full}
    # metrics and wiredTiger are huge; keep only the replication-relevant parts
    if isinstance(out.get("metrics"), dict):
        out["metrics"] = {k: v for k, v in out["metrics"].items()
                          if k in ("repl", "operation", "ttl", "commands")}
    if isinstance(out.get("wiredTiger"), dict):
        out["wiredTiger"] = {k: v for k, v in out["wiredTiger"].items()
                             if k in ("log", "cache", "transaction")}
    return out


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_checksums(run_dir: Path) -> None:
    lines = []
    for p in sorted(run_dir.rglob("*")):
        if p.is_file() and p.name != "checksums.txt":
            lines.append(f"{sha256(p)}  {p.relative_to(run_dir)}")
    (run_dir / "checksums.txt").write_text("\n".join(lines) + "\n")


def dump(run_dir: Path, name: str, obj: Any) -> None:
    (run_dir / name).write_text(json.dumps(obj, indent=2, default=str))
