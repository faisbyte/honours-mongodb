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


def client_env() -> dict:
    import pymongo
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
        "power_source": _run(["pmset", "-g", "batt"]),  # macOS: thermal/perf state matters
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
        "wcMajorityJournalDefault": _safe(
            admin.command, {"getParameter": 1,
                            "writeConcernMajorityJournalDefault": 1}),
    }


def repl_snapshot(client) -> dict:
    admin = client.admin
    return {
        "captured_ns": time.time_ns(),
        "replSetGetStatus": _safe(admin.command, "replSetGetStatus"),
        "replSetGetConfig": _safe(admin.command, "replSetGetConfig"),
        "hello": _safe(admin.command, "hello"),
    }


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
