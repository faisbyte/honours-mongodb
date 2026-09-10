#!/usr/bin/env python3
"""Client-observed freshness collector for a MongoDB replica set.

Methodology follows Wada, Fekete, Zhao, Lee and Liu, "Data Consistency
Properties and the Trade-offs in Commercial Cloud Storages: the Consumers'
Perspective", CIDR 2011 -- specifically the write-then-poll cycle used in
their section 3.1, rather than the independent writer/reader loops of their
section 2. On a local replica set the visibility window is roughly three
orders of magnitude shorter than SimpleDB's, so independent loops would put
almost no samples in the interesting region.

This script only COLLECTS. It computes no freshness verdicts, no buckets and
no summary statistics. Everything derived lives in analyse.py so that the
raw record on disk survives changes to how we define staleness.

Usage:
    python -m harness.collect --config config/baseline.yaml
    python -m harness.collect --config config/baseline.yaml --calibrate
"""

from __future__ import annotations

import argparse
import gzip
import json
import random
import shutil
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from pymongo import MongoClient
from pymongo.read_preferences import (Nearest, Primary, PrimaryPreferred,
                                      Secondary, SecondaryPreferred)

from . import provenance

REPO_ROOT = Path(__file__).resolve().parent.parent

_READ_PREFS = {
    "primary": Primary,
    "primaryPreferred": PrimaryPreferred,
    "secondary": Secondary,
    "secondaryPreferred": SecondaryPreferred,
    "nearest": Nearest,
}


# --------------------------------------------------------------------------
# timing helpers
# --------------------------------------------------------------------------

def _spin_until(deadline_ns: int) -> None:
    """Wait until deadline_ns on the perf_counter clock.

    time.sleep() on macOS has roughly millisecond granularity, which is the
    same order as the whole effect we are trying to measure. So we sleep only
    for the coarse part and busy-spin the remainder. This burns a core; it is
    recorded in the manifest so the CPU cost is not a hidden variable.
    """
    coarse = deadline_ns - time.perf_counter_ns() - 1_500_000  # leave 1.5ms
    if coarse > 0:
        time.sleep(coarse / 1e9)
    while time.perf_counter_ns() < deadline_ns:
        pass


# --------------------------------------------------------------------------
# targets
# --------------------------------------------------------------------------

@dataclass
class Target:
    """One distinct read path.

    kind="direct"     -> connect straight to one mongod (directConnection=true).
                         Tells you where the bytes physically are.
    kind="replicaset" -> go through the driver's topology and read preference.
                         Tells you what an application actually observes.
    """
    id: str
    kind: str
    read_preference: str
    read_concern: str | None = None
    host: str | None = None
    causal_consistency: bool = False
    max_staleness_seconds: int | None = None
    client: Any = field(default=None, repr=False)
    session: Any = field(default=None, repr=False)

    def pref(self):
        cls = _READ_PREFS[self.read_preference]
        if self.max_staleness_seconds is not None and cls is not Primary:
            return cls(max_staleness=self.max_staleness_seconds)
        return cls()


def build_targets(cfg: dict) -> list[Target]:
    rs_name = cfg["replica_set"]["name"]
    hosts = ",".join(cfg["replica_set"]["seed_hosts"])
    targets: list[Target] = []

    for spec in cfg["targets"]:
        t = Target(
            id=spec["id"],
            kind=spec["kind"],
            read_preference=spec["read_preference"],
            read_concern=spec.get("read_concern"),
            host=spec.get("host"),
            causal_consistency=bool(spec.get("causal_consistency", False)),
            max_staleness_seconds=spec.get("max_staleness_seconds"),
        )
        if t.kind == "direct":
            uri = f"mongodb://{t.host}/?directConnection=true"
        elif t.kind == "replicaset":
            uri = f"mongodb://{hosts}/?replicaSet={rs_name}"
        else:
            raise ValueError(f"unknown target kind: {t.kind}")

        t.client = MongoClient(
            uri,
            read_preference=t.pref(),
            appname=f"freshness-{t.id}",
            serverSelectionTimeoutMS=5000,
            # One socket per target keeps the read path deterministic; without
            # this the driver may open a new connection mid-run and the first
            # read on it pays a handshake we would otherwise misread as lag.
            maxPoolSize=1,
            minPoolSize=1,
        )
        targets.append(t)
    return targets


# --------------------------------------------------------------------------
# operations
# --------------------------------------------------------------------------

def do_write(db, coll_name: str, key: str, seq: int, pad: str,
             wc_doc: dict) -> dict:
    """One write. Returns the raw record, timings on the perf_counter clock."""
    cmd = {
        "update": coll_name,
        "updates": [{
            "q": {"_id": key},
            "u": {"$set": {"seq": seq, "wall_ns": time.time_ns(), "pad": pad}},
            "upsert": True,
        }],
        "writeConcern": wc_doc,
    }
    t0 = time.perf_counter_ns()
    err = None
    reply: dict = {}
    try:
        reply = db.command(cmd)
    except Exception as exc:  # noqa: BLE001
        err = f"{type(exc).__name__}: {exc}"
    t1 = time.perf_counter_ns()

    op_time = reply.get("operationTime")
    return {
        "seq": seq,
        "t_start_ns": t0,
        "t_ack_ns": t1,
        "latency_ns": t1 - t0,
        "ok": reply.get("ok"),
        "n": reply.get("n"),
        "nModified": reply.get("nModified"),
        "operationTime": {"t": op_time.time, "i": op_time.inc} if op_time else None,
        "_raw_op_time": op_time,
        "_raw_cluster_time": reply.get("$clusterTime"),
        "writeConcernError": reply.get("writeConcernError"),
        "error": err,
    }


def do_read(target: Target, db_name: str, coll_name: str, key: str,
            capture_op_time: bool) -> dict:
    """One read against one target. Timings on the perf_counter clock."""
    db = target.client[db_name]
    err = None
    doc = None
    op_time = None

    if capture_op_time:
        cmd: dict[str, Any] = {
            "find": coll_name,
            "filter": {"_id": key},
            "limit": 1,
            "singleBatch": True,
        }
        if target.read_concern:
            cmd["readConcern"] = {"level": target.read_concern}
        t0 = time.perf_counter_ns()
        try:
            reply = db.command(cmd, read_preference=target.pref(),
                               session=target.session)
            batch = reply.get("cursor", {}).get("firstBatch", [])
            doc = batch[0] if batch else None
            op_time = reply.get("operationTime")
        except Exception as exc:  # noqa: BLE001
            err = f"{type(exc).__name__}: {exc}"
        t1 = time.perf_counter_ns()
    else:
        from pymongo.read_concern import ReadConcern
        coll = db.get_collection(
            coll_name,
            read_preference=target.pref(),
            read_concern=ReadConcern(target.read_concern) if target.read_concern else None,
        )
        t0 = time.perf_counter_ns()
        try:
            doc = coll.find_one({"_id": key}, session=target.session)
        except Exception as exc:  # noqa: BLE001
            err = f"{type(exc).__name__}: {exc}"
        t1 = time.perf_counter_ns()

    return {
        "target": target.id,
        "t_start_ns": t0,
        "t_end_ns": t1,
        "latency_ns": t1 - t0,
        "observed_seq": doc.get("seq") if doc else None,
        "observed_wall_ns": doc.get("wall_ns") if doc else None,
        "operationTime": {"t": op_time.time, "i": op_time.inc} if op_time else None,
        "error": err,
    }


# --------------------------------------------------------------------------
# calibration
# --------------------------------------------------------------------------

def calibrate(targets: list[Target], db_name: str, coll_name: str,
              key: str, n: int = 500) -> dict:
    """Measure client-side overhead so it is not mistaken for replication lag.

    Reports the round-trip distribution of a read that cannot possibly be
    stale. Anything smaller than the p50 here is below our resolution.
    """
    out = {}
    for t in targets:
        lat = []
        for _ in range(n):
            r = do_read(t, db_name, coll_name, key, capture_op_time=True)
            if r["error"] is None:
                lat.append(r["latency_ns"])
        if lat:
            lat.sort()
            out[t.id] = {
                "n": len(lat),
                "min_ns": lat[0],
                "p50_ns": lat[len(lat) // 2],
                "p95_ns": lat[int(len(lat) * 0.95)],
                "p99_ns": lat[int(len(lat) * 0.99)],
                "max_ns": lat[-1],
                "mean_ns": statistics.fmean(lat),
            }
    return out


# --------------------------------------------------------------------------
# main run
# --------------------------------------------------------------------------

def run(cfg: dict, cfg_path: Path, out_root: Path, do_calibrate: bool) -> Path:
    w = cfg["workload"]
    cap = cfg["capture"]
    rs_name = cfg["replica_set"]["name"]
    hosts = cfg["replica_set"]["seed_hosts"]

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"{stamp}__{cfg['experiment']['name']}"
    run_dir = out_root / run_id
    (run_dir / "provenance").mkdir(parents=True, exist_ok=True)

    print(f"[run] {run_dir}")

    rng = random.Random(cfg["experiment"]["seed"])
    targets = build_targets(cfg)

    rs_uri = f"mongodb://{','.join(hosts)}/?replicaSet={rs_name}"
    rs_client = MongoClient(rs_uri, appname="freshness-writer",
                            serverSelectionTimeoutMS=5000)
    db = rs_client[w["database"]]

    wc_doc: dict[str, Any] = {"w": w["write_concern"]["w"]}
    if w["write_concern"].get("wtimeout_ms") is not None:
        wc_doc["wtimeout"] = w["write_concern"]["wtimeout_ms"]
    if w["write_concern"].get("j") is not None:
        wc_doc["j"] = w["write_concern"]["j"]

    pad = "x" * max(0, int(w["payload_bytes"]))

    # ---- sessions -------------------------------------------------------
    for t in targets:
        if t.causal_consistency:
            t.session = t.client.start_session(causal_consistency=True)

    # ---- provenance, before --------------------------------------------
    shutil.copy(cfg_path, run_dir / "config.yaml")
    provenance.dump(run_dir / "provenance", "client_env.json",
                    provenance.client_env())
    provenance.dump(run_dir / "provenance", "git.json",
                    provenance.git_state(REPO_ROOT))
    node_docs = []
    for h in hosts:
        c = MongoClient(f"mongodb://{h}/?directConnection=true",
                        serverSelectionTimeoutMS=5000)
        node_docs.append(provenance.node_info(c, h))
    provenance.dump(run_dir / "provenance", "nodes.json", node_docs)
    provenance.dump(run_dir / "provenance", "repl_before.json",
                    provenance.repl_snapshot(rs_client))
    provenance.dump(run_dir / "provenance", "server_status_before.json",
                    provenance.server_status(rs_client))

    # ---- calibration ----------------------------------------------------
    if do_calibrate:
        db[w["collection"]].update_one(
            {"_id": w["doc_key"]}, {"$set": {"seq": -1, "wall_ns": 0, "pad": pad}},
            upsert=True)
        provenance.dump(run_dir / "provenance", "calibration.json",
                        calibrate(targets, w["database"], w["collection"],
                                  w["doc_key"]))
        print("[run] calibration written")

    # ---- clock anchor ---------------------------------------------------
    anchor = {"wall_ns": time.time_ns(), "perf_ns": time.perf_counter_ns()}

    writes_f = (run_dir / "writes.jsonl").open("w")
    reads_f = (run_dir / "reads.jsonl").open("w")
    repl_f = (run_dir / "repl_samples.jsonl").open("w")

    poll_ns = int(w["poll_window_ms"] * 1e6)
    sweep_gap_ns = int(w["inter_sweep_us"] * 1e3)
    quiesce_ns = int(w["quiesce_ms"] * 1e6)

    try:
        for cycle in range(w["cycles"]):
            hello = rs_client.admin.command("hello")
            primary = hello.get("primary")

            if cap.get("repl_status_per_cycle"):
                repl_f.write(json.dumps(
                    {"cycle": cycle, "phase": "pre",
                     "snapshot": provenance.repl_snapshot(rs_client)},
                    default=str) + "\n")

            wrec = do_write(db, w["collection"], w["doc_key"], cycle, pad, wc_doc)
            wrec.update({"cycle": cycle, "primary": primary,
                         "write_concern": wc_doc,
                         "warmup": cycle < w["warmup_cycles"]})

            raw_op = wrec.pop("_raw_op_time", None)
            raw_ct = wrec.pop("_raw_cluster_time", None)
            for t in targets:
                if t.session is not None:
                    if raw_ct is not None:
                        t.session.advance_cluster_time(raw_ct)
                    if raw_op is not None:
                        t.session.advance_operation_time(raw_op)
                        
            writes_f.write(json.dumps(wrec, default=str) + "\n")

            if wrec["error"]:
                print(f"[cycle {cycle}] write failed: {wrec['error']}")

            # ---- poll window: no I/O, no allocation-heavy work ----------
            buf: list[dict] = []
            deadline = wrec["t_ack_ns"] + poll_ns
            sweep = 0
            while time.perf_counter_ns() < deadline:
                order = targets[:]
                rng.shuffle(order)          # kills systematic position bias
                for t in order:
                    rec = do_read(t, w["database"], w["collection"],
                                  w["doc_key"], cap.get("operation_time", True))
                    rec.update({"cycle": cycle, "sweep": sweep,
                                "warmup": cycle < w["warmup_cycles"]})
                    buf.append(rec)
                sweep += 1
                if sweep_gap_ns:
                    _spin_until(time.perf_counter_ns() + sweep_gap_ns)

            for rec in buf:
                reads_f.write(json.dumps(rec, default=str) + "\n")

            if cap.get("repl_status_per_cycle"):
                repl_f.write(json.dumps(
                    {"cycle": cycle, "phase": "post",
                     "snapshot": provenance.repl_snapshot(rs_client)},
                    default=str) + "\n")

            if (cycle + 1) % 20 == 0:
                print(f"[cycle {cycle + 1}/{w['cycles']}] "
                      f"{len(buf)} reads, primary={primary}")

            _spin_until(time.perf_counter_ns() + quiesce_ns)
    finally:
        writes_f.close()
        reads_f.close()
        repl_f.close()

    # ---- provenance, after ---------------------------------------------
    provenance.dump(run_dir / "provenance", "repl_after.json",
                    provenance.repl_snapshot(rs_client))
    provenance.dump(run_dir / "provenance", "server_status_after.json",
                    provenance.server_status(rs_client))
    provenance.dump(run_dir, "manifest.json", {
        "run_id": run_id,
        "started_utc": stamp,
        "finished_utc": datetime.now(timezone.utc).isoformat(),
        "clock_anchor": anchor,
        "clock_note": ("All t_*_ns are time.perf_counter_ns() from a single "
                       "client process, so they share one monotonic clock and "
                       "are free of inter-host skew. Map to wall clock with "
                       "wall = clock_anchor.wall_ns + (t - clock_anchor.perf_ns)."),
        "config": cfg,
        "files": {
            "writes.jsonl": "one record per write",
            "reads.jsonl": "one record per read",
            "repl_samples.jsonl": "replSetGetStatus at cycle boundaries",
        },
    })

    if cap.get("compress_output"):
        for name in ("writes.jsonl", "reads.jsonl", "repl_samples.jsonl"):
            p = run_dir / name
            if p.exists():
                with p.open("rb") as src, gzip.open(str(p) + ".gz", "wb") as dst:
                    shutil.copyfileobj(src, dst)
                p.unlink()

    provenance.write_checksums(run_dir)
    print(f"[run] done -> {run_dir}")
    return run_dir


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, type=Path)
    ap.add_argument("--out", type=Path, default=REPO_ROOT / "measurements")
    ap.add_argument("--calibrate", action="store_true",
                    help="measure client-side read overhead before the run")
    args = ap.parse_args()

    cfg = yaml.safe_load(args.config.read_text())
    run(cfg, args.config, args.out, args.calibrate)


if __name__ == "__main__":
    main()
