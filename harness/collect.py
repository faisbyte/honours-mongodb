#!/usr/bin/env python3
"""Client-observed freshness collector for a MongoDB replica set.

Version 3 reads every target, and calibration, through Collection.find_one
(see read_collection). Runs from version 2 read non-causal targets through a
hand-built find sent with Database.command, so the two are not directly
comparable on read latency.

Version 2. Changes from v1, following supervisor feedback:

  * Writer and readers run on INDEPENDENT THREADS. In v1 reads only began
    after the write was acknowledged, which meant the entire write window was
    invisible. That mattered most for w:"majority", where the ack takes 14 to
    40ms and by then there is nothing left to catch. Readers now poll
    continuously, so reads land before, during and after every write.
  * Causal sessions are advanced with the writer's operationTime and
    clusterTime. MongoDB's docs are explicit that a causally consistent
    session tracks these two values and that one session can be advanced to
    match another. In v1 the reader sessions never saw the writer's
    timestamps, so the causal target was measuring an ordinary secondary read.
    Each session is touched by exactly one thread, as the docs require.
  * Errors are recorded rather than discarded, so failures under load become
    data instead of gaps.
  * Both write timestamps (submit and acknowledge) were already captured and
    remain so; nothing derived is stored.

Only collection happens here. No freshness verdicts, no buckets, no summary
statistics: those live in analyse.py so the raw record survives changes to how
staleness is defined.

Modes (workload.mode):
    isolated    one reader target per process, chosen with --target. The
                default, because nine reader threads in one process inflate
                read latency by roughly an order of magnitude and that is
                measurement noise, not replication behaviour.
    concurrent  every target as a thread in one process. All targets see the
                same instant, at the cost of contaminated latency.

Usage:
    python -m harness.collect --config config/threaded-w1.yaml --target rs-primary-local --calibrate
    python -m harness.run_all --config config/threaded-w1.yaml --rounds 3
"""

from __future__ import annotations

import argparse
import gzip
import json
import shutil
import statistics
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from pymongo import MongoClient
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import (Nearest, Primary, PrimaryPreferred,
                                      Secondary, SecondaryPreferred)

from . import provenance
from .wire import WireListener, causal_problem

REPO_ROOT = Path(__file__).resolve().parent.parent

_READ_PREFS = {
    "primary": Primary,
    "primaryPreferred": PrimaryPreferred,
    "secondary": Secondary,
    "secondaryPreferred": SecondaryPreferred,
    "nearest": Nearest,
}

_MODES = ("isolated", "concurrent")

# Workload keys with no default. Checked before anything is created on disk:
# a v1 config (cycles / poll_window_ms) otherwise fails inside the writer
# thread, after the run directory and provenance have already been written.
_REQUIRED_WORKLOAD = ("database", "collection", "doc_key", "payload_bytes",
                      "duration_s", "write_interval_ms", "write_concern")


def resolve_mode(cfg: dict) -> str:
    mode = cfg["workload"].get("mode", "isolated")
    if mode not in _MODES:
        raise SystemExit(f"[config] workload.mode must be one of "
                         f"{list(_MODES)}, got {mode!r}")
    return mode


def check_workload(cfg: dict) -> None:
    missing = [k for k in _REQUIRED_WORKLOAD if k not in cfg["workload"]]
    if missing:
        raise SystemExit(
            f"[config] workload is missing {missing}. A v1 config, which "
            f"describes the workload as cycles and poll_window_ms, cannot be "
            f"run by this collector.")


def check_expect_schema(cfg: dict) -> dict:
    """The replica set state this config claims to measure.

        expect:
          topology: star             # or an explicit map, quoted, e.g.
                                     # "27018<27020 27019<27020 27020<27017"
          journal_default: true      # writeConcernMajorityJournalDefault
          chaining_allowed: false    # optional; checked only if present

    Required, because a run's condition is not only its write and read
    concerns: topology and journal default change the result and are set
    outside the harness. The live values are compared before the run folder
    is created (check_expect); a mismatch refuses to start.
    """
    exp = cfg.get("expect")
    problems = []
    if not isinstance(exp, dict):
        problems.append("there is no expect block")
    else:
        if not isinstance(exp.get("topology"), str):
            problems.append("expect.topology must be a string")
        if not isinstance(exp.get("journal_default"), bool):
            problems.append("expect.journal_default must be true or false")
        if ("chaining_allowed" in exp
                and not isinstance(exp["chaining_allowed"], bool)):
            problems.append("expect.chaining_allowed must be true or false")
    if problems:
        raise SystemExit(f"[config] {'; '.join(problems)}. See "
                         f"check_expect_schema in harness/collect.py.")
    return exp


def check_expect(exp: dict, topo: dict) -> None:
    """Refuse to start unless the live replica set matches the config."""
    problems = []
    if topo["label"] != exp["topology"]:
        problems.append(f"topology is {topo['label']!r}, config expects "
                        f"{exp['topology']!r}")
    if topo["writeConcernMajorityJournalDefault"] is not exp["journal_default"]:
        problems.append(f"writeConcernMajorityJournalDefault is "
                        f"{topo['writeConcernMajorityJournalDefault']!r}, "
                        f"config expects {exp['journal_default']!r}")
    if ("chaining_allowed" in exp
            and topo["chainingAllowed"] is not exp["chaining_allowed"]):
        problems.append(f"chainingAllowed is {topo['chainingAllowed']!r}, "
                        f"config expects {exp['chaining_allowed']!r}")
    if topo.get("errors"):
        problems.append(f"could not read replica set state: {topo['errors']}")
    if problems:
        raise SystemExit("[expect] refusing to start, no run folder created: "
                         + "; ".join(problems))


def check_targets(cfg: dict) -> None:
    """Target combinations the server would reject or that would not measure
    what their name says. Checked before any connection is opened."""
    problems = []
    for s in cfg["targets"]:
        if s.get("read_concern") == "linearizable":
            if s.get("read_preference") != "primary":
                problems.append(f"{s['id']}: linearizable reads need "
                                f"read_preference primary")
            if s.get("kind") != "replicaset":
                problems.append(f"{s['id']}: linearizable target must be "
                                f"kind replicaset, so it follows the primary")
            if s.get("causal_consistency"):
                problems.append(f"{s['id']}: linearizable reads cannot carry "
                                f"afterClusterTime; drop causal_consistency")
            if not s.get("max_time_ms"):
                problems.append(f"{s['id']}: give linearizable reads a "
                                f"max_time_ms; they block while no majority "
                                f"is reachable")
    if problems:
        raise SystemExit("[config] " + "; ".join(problems))


def select_specs(cfg: dict, mode: str, target_id: str | None) -> list[dict]:
    """Pick the target specs this process will read from."""
    ids = [s["id"] for s in cfg["targets"]]
    listing = "\n  ".join(ids)
    if mode == "concurrent":
        if target_id is not None:
            raise SystemExit(
                "[args] --target applies to workload.mode 'isolated'; this "
                "config is 'concurrent', which reads every target in one "
                "process.")
        return list(cfg["targets"])
    if target_id is None:
        raise SystemExit(
            f"[args] workload.mode is 'isolated', so --target is required. "
            f"Available targets:\n  {listing}")
    if target_id not in ids:
        raise SystemExit(f"[args] unknown target {target_id!r}. Available "
                         f"targets:\n  {listing}")
    return [s for s in cfg["targets"] if s["id"] == target_id]


def _spin_until(deadline_ns: int) -> None:
    """Wait until deadline_ns on the perf_counter clock.

    time.sleep() has roughly millisecond granularity, the same order as the
    effect being measured, so we sleep the coarse part and spin the rest.
    """
    coarse = deadline_ns - time.perf_counter_ns() - 1_500_000
    if coarse > 0:
        time.sleep(coarse / 1e9)
    while time.perf_counter_ns() < deadline_ns:
        pass


class WriteClock:
    """The writer's latest operationTime and clusterTime, shared with readers.

    Readers copy these out under the lock and then advance their OWN session on
    their OWN thread, which keeps each session single-threaded as the MongoDB
    documentation requires while still propagating causality across threads.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._op_time = None
        self._cluster_time = None
        self._seq = -1

    def publish(self, op_time, cluster_time, seq: int) -> None:
        with self._lock:
            self._op_time = op_time
            self._cluster_time = cluster_time
            self._seq = seq

    def snapshot(self):
        with self._lock:
            return self._op_time, self._cluster_time, self._seq


@dataclass
class Target:
    """One distinct read path.

    kind="direct"     -> one mongod, addressed directly. Where the bytes are.
    kind="replicaset" -> through the driver's topology. What an app observes.
    """
    id: str
    kind: str
    read_preference: str
    read_concern: str | None = None
    host: str | None = None
    causal_consistency: bool = False
    max_staleness_seconds: int | None = None
    max_time_ms: int | None = None
    read_interval_us: int = 500
    client: Any = field(default=None, repr=False)
    session: Any = field(default=None, repr=False)
    wire: Any = field(default=None, repr=False)
    records: list = field(default_factory=list, repr=False)

    def pref(self):
        cls = _READ_PREFS[self.read_preference]
        if self.max_staleness_seconds is not None and cls is not Primary:
            return cls(max_staleness=self.max_staleness_seconds)
        return cls()


def build_targets(cfg: dict, specs: list[dict] | None = None) -> list[Target]:
    rs_name = cfg["replica_set"]["name"]
    hosts = ",".join(cfg["replica_set"]["seed_hosts"])
    default_gap = cfg["workload"].get("read_interval_us", 500)
    targets: list[Target] = []

    for spec in (cfg["targets"] if specs is None else specs):
        t = Target(
            id=spec["id"],
            kind=spec["kind"],
            read_preference=spec["read_preference"],
            read_concern=spec.get("read_concern"),
            host=spec.get("host"),
            causal_consistency=bool(spec.get("causal_consistency", False)),
            max_staleness_seconds=spec.get("max_staleness_seconds"),
            max_time_ms=spec.get("max_time_ms"),
            read_interval_us=spec.get("read_interval_us", default_gap),
        )
        if t.kind == "direct":
            uri = f"mongodb://{t.host}/?directConnection=true"
        elif t.kind == "replicaset":
            uri = f"mongodb://{hosts}/?replicaSet={rs_name}"
        else:
            raise ValueError(f"unknown target kind: {t.kind}")

        t.wire = WireListener(f"target:{t.id}")
        t.client = MongoClient(
            uri,
            read_preference=t.pref(),
            appname=f"freshness-{t.id}",
            serverSelectionTimeoutMS=5000,
            maxPoolSize=1,
            minPoolSize=1,
            event_listeners=[t.wire],
        )
        targets.append(t)
    return targets


# --------------------------------------------------------------------------
# writer
# --------------------------------------------------------------------------

def _writer_body(stop: threading.Event, clock: WriteClock, out: list,
                 rs_client, cfg: dict) -> None:
    w = cfg["workload"]
    db = rs_client[w["database"]]
    coll_name, key = w["collection"], w["doc_key"]
    pad = "x" * max(0, int(w["payload_bytes"]))
    interval_ns = int(w["write_interval_ms"] * 1e6)

    wc_doc: dict[str, Any] = {"w": w["write_concern"]["w"]}
    if w["write_concern"].get("wtimeout_ms") is not None:
        wc_doc["wtimeout"] = w["write_concern"]["wtimeout_ms"]
    if w["write_concern"].get("j") is not None:
        wc_doc["j"] = w["write_concern"]["j"]

    # The writer has its own session, touched only by this thread.
    session = rs_client.start_session(causal_consistency=True)

    seq = 0
    next_at = time.perf_counter_ns()
    while not stop.is_set():
        _spin_until(next_at)
        next_at += interval_ns

        cmd = {
            "update": coll_name,
            "updates": [{
                "q": {"_id": key},
                "u": {"$set": {"seq": seq, "wall_ns": time.time_ns(),
                               "pad": pad}},
                "upsert": True,
            }],
            "writeConcern": wc_doc,
        }

        err = None
        reply: dict = {}
        t0 = time.perf_counter_ns()
        try:
            reply = db.command(cmd, session=session)
        except Exception as exc:  # noqa: BLE001 - failures are data
            err = f"{type(exc).__name__}: {exc}"
        t1 = time.perf_counter_ns()

        op_time = reply.get("operationTime")
        cluster_time = reply.get("$clusterTime")
        if op_time is not None:
            clock.publish(op_time, cluster_time, seq)

        out.append({
            "seq": seq,
            "t_start_ns": t0,
            "t_ack_ns": t1,
            "latency_ns": t1 - t0,
            "ok": reply.get("ok"),
            "n": reply.get("n"),
            "nModified": reply.get("nModified"),
            "operationTime": {"t": op_time.time, "i": op_time.inc} if op_time else None,
            "writeConcernError": reply.get("writeConcernError"),
            "error": err,
            "write_concern": wc_doc,
        })
        seq += 1


def writer_loop(stop: threading.Event, clock: WriteClock, out: list,
                rs_client, cfg: dict, fatal: list) -> None:
    """Run the writer, and never leave the readers running without it.

    If the writer dies the run is meaningless: the readers would poll a frozen
    document for the full duration and the analysis would have nothing to join
    against. Setting stop in a finally ends the run at once, and the reason is
    recorded in the manifest rather than only printed to a terminal.
    """
    try:
        _writer_body(stop, clock, out, rs_client, cfg)
    except BaseException as exc:  # noqa: BLE001 - recorded, then re-raised
        fatal.append(f"{type(exc).__name__}: {exc}")
        raise
    finally:
        stop.set()


# --------------------------------------------------------------------------
# readers
# --------------------------------------------------------------------------

def read_collection(t: Target, cfg: dict):
    """The collection object every read on this target goes through.

    All targets, causal or not, and calibration read with
    Collection.find_one on this object, so every path shares one driver code
    path and differs only in read preference, read concern and session.

    Reads never go through Database.command. In PyMongo 4.18
    Database.command does not pass a read concern to the command runner, so
    session._update_read_concern() is never called and a causal session's
    afterClusterTime never reaches the server, whatever the command document
    contains. find_one builds the find itself and merges afterClusterTime in.
    """
    w = cfg["workload"]
    return t.client[w["database"]][w["collection"]].with_options(
        read_preference=t.pref(),
        read_concern=ReadConcern(t.read_concern))


def reader_loop(t: Target, stop: threading.Event, clock: WriteClock,
                cfg: dict, fatal: list) -> None:
    key = cfg["workload"]["doc_key"]
    gap_ns = int(t.read_interval_us * 1e3)
    coll = read_collection(t, cfg)

    # Reads sit on a fixed grid of slots gap_ns apart. A slot whose time has
    # already passed when the reader is ready (the previous read overran it)
    # is skipped and counted, never made up. Catching up would fire reads back
    # to back after every slow read and bunch the sample there.
    next_at = time.perf_counter_ns() + gap_ns
    while not stop.is_set():
        missed = 0
        now = time.perf_counter_ns()
        if now > next_at:
            missed = (now - next_at) // gap_ns + 1
            next_at += missed * gap_ns
        t_sched = next_at
        _spin_until(t_sched)
        next_at += gap_ns

        # Causal targets pull the writer's timestamps across and advance their
        # own session, on this thread. find_one then puts afterClusterTime on
        # the wire and the server waits until it has caught up.
        causal_seq = None
        if t.session is not None:
            op_time, cluster_time, causal_seq = clock.snapshot()
            try:
                if cluster_time is not None:
                    t.session.advance_cluster_time(cluster_time)
                if op_time is not None:
                    t.session.advance_operation_time(op_time)
            except Exception:  # noqa: BLE001 - never kill the reader for this
                causal_seq = None

        # The driver has an afterClusterTime to send exactly when the session
        # holds an operationTime. From then on, every causal read must carry
        # one on the wire.
        expect_act = (t.session is not None
                      and t.session.operation_time is not None)

        err = None
        doc = None
        t.wire.begin()
        t0 = time.perf_counter_ns()
        try:
            doc = coll.find_one({"_id": key}, session=t.session,
                                max_time_ms=t.max_time_ms)
        except Exception as exc:  # noqa: BLE001 - failures are data
            err = f"{type(exc).__name__}: {exc}"
        t1 = time.perf_counter_ns()
        sent = t.wire.last_find()

        t.records.append({
            "target": t.id,
            "t_sched_ns": t_sched,      # the slot this read was scheduled for
            "missed_slots": missed,     # slots skipped just before this read
            "t_start_ns": t0,           # actual send, just before find_one
            "t_end_ns": t1,
            "latency_ns": t1 - t0,
            "observed_seq": doc.get("seq") if doc else None,
            "observed_wall_ns": doc.get("wall_ns") if doc else None,
            # The reply's own operationTime, from the command listener. Not
            # session.operation_time: on a causal session that is the running
            # maximum, which the reader has just advanced to the writer's.
            "operationTime": sent["reply_operation_time"] if sent else None,
            "causal_waited_for_seq": causal_seq,
            # readConcern exactly as the find command carried it on the wire
            "read_concern_sent": sent["read_concern"] if sent else None,
            "after_cluster_time_sent": (sent["after_cluster_time"]
                                        if sent else None),
            "error": err,
        })

        if t.session is not None:
            problem = causal_problem(sent, expect_act, err)
            if problem:
                fatal.append(f"{t.id}: {problem}, read {len(t.records) - 1}; "
                             f"the target is not reading causally")
                stop.set()
                return


# --------------------------------------------------------------------------
# calibration
# --------------------------------------------------------------------------

def calibrate(targets: list[Target], cfg: dict, n: int = 500) -> dict:
    """Read round-trip against a value that cannot be stale.

    Anything below the p50 here is under the harness's resolution and should
    not be interpreted as replication behaviour.
    """
    key = cfg["workload"]["doc_key"]
    out = {}
    for t in targets:
        # Same read path as reader_loop, session included for causal
        # targets, so the calibration covers what the run actually does.
        coll = read_collection(t, cfg)
        lat, errs = [], 0
        for i in range(n):
            expect_act = (t.session is not None
                          and t.session.operation_time is not None)
            t.wire.begin()
            t0 = time.perf_counter_ns()
            err = None
            try:
                coll.find_one({"_id": key}, session=t.session,
                              max_time_ms=t.max_time_ms)
                lat.append(time.perf_counter_ns() - t0)
            except Exception as exc:  # noqa: BLE001
                errs += 1
                err = str(exc)
            if t.session is not None:
                problem = causal_problem(t.wire.last_find(), expect_act, err)
                if problem:
                    raise SystemExit(f"[calibrate] {t.id}: {problem}, "
                                     f"calibration read {i}. Aborting before "
                                     f"collection: the target is not reading "
                                     f"causally.")
        if lat:
            lat.sort()
            out[t.id] = {
                "n": len(lat), "errors": errs,
                "min_ns": lat[0], "p50_ns": lat[len(lat) // 2],
                "p95_ns": lat[int(len(lat) * 0.95)],
                "p99_ns": lat[int(len(lat) * 0.99)],
                "max_ns": lat[-1], "mean_ns": statistics.fmean(lat),
            }
        else:
            out[t.id] = {"n": 0, "errors": errs}
    return out


# --------------------------------------------------------------------------
# run
# --------------------------------------------------------------------------

def run(cfg: dict, cfg_path: Path, out_root: Path, do_calibrate: bool,
        target_id: str | None = None, round_index: int | None = None) -> Path:
    w = cfg["workload"]
    cap = cfg["capture"]
    hosts = cfg["replica_set"]["seed_hosts"]
    rs_name = cfg["replica_set"]["name"]

    # Everything that can reject the invocation happens first, so a bad run
    # never creates a directory. Config problems are caught before any
    # connection; a replica set that does not match expect is caught before
    # the folder exists.
    check_workload(cfg)
    mode = resolve_mode(cfg)
    specs = select_specs(cfg, mode, target_id)
    check_targets(cfg)
    expect = check_expect_schema(cfg)

    # Before the run folder exists, so the folder cannot mark the tree dirty.
    git = provenance.git_state(REPO_ROOT)

    writer_wire = WireListener("writer")
    rs_client = MongoClient(f"mongodb://{','.join(hosts)}/?replicaSet={rs_name}",
                            appname="freshness-writer",
                            serverSelectionTimeoutMS=5000,
                            event_listeners=[writer_wire])
    topo_before = provenance.topology(rs_client)
    try:
        check_expect(expect, topo_before)
    except SystemExit:
        rs_client.close()
        raise

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    parts = [stamp, cfg["experiment"]["name"]]
    if mode == "isolated":
        parts.append(target_id)
    if round_index is not None:
        parts.append(f"r{round_index}")
    run_id = "__".join(parts)
    run_dir = out_root / run_id
    (run_dir / "provenance").mkdir(parents=True, exist_ok=True)
    print(f"[run] {run_dir}")

    targets = build_targets(cfg, specs)
    wires = [writer_wire] + [t.wire for t in targets]

    for t in targets:
        if t.causal_consistency:
            t.session = t.client.start_session(causal_consistency=True)

    shutil.copy(cfg_path, run_dir / "config.yaml")
    env = provenance.client_env()
    provenance.dump(run_dir / "provenance", "client_env.json", env)
    provenance.dump(run_dir / "provenance", "git.json", git)
    provenance.dump(run_dir / "provenance", "topology_before.json",
                    topo_before)
    node_infos = []
    for h in hosts:
        node_wire = WireListener(f"node:{h}")
        wires.append(node_wire)
        with MongoClient(f"mongodb://{h}/?directConnection=true",
                         serverSelectionTimeoutMS=5000,
                         event_listeners=[node_wire]) as node_client:
            node_infos.append(provenance.node_info(node_client, h))
    provenance.dump(run_dir / "provenance", "nodes.json", node_infos)
    provenance.dump(run_dir / "provenance", "repl_before.json",
                    provenance.repl_snapshot(rs_client))
    provenance.dump(run_dir / "provenance", "server_status_before.json",
                    provenance.server_status(rs_client))

    db = rs_client[w["database"]]
    db[w["collection"]].update_one(
        {"_id": w["doc_key"]},
        {"$set": {"seq": -1, "wall_ns": 0, "pad": "x" * w["payload_bytes"]}},
        upsert=True)

    if do_calibrate:
        provenance.dump(run_dir / "provenance", "calibration.json",
                        calibrate(targets, cfg))
        print("[run] calibration written")

    anchor = {"wall_ns": time.time_ns(), "perf_ns": time.perf_counter_ns()}

    clock = WriteClock()
    stop = threading.Event()
    writes: list = []
    fatal: list = []
    reader_fatal: list = []

    threads = [threading.Thread(target=writer_loop, name="writer",
                                args=(stop, clock, writes, rs_client, cfg,
                                      fatal),
                                daemon=True)]
    for t in targets:
        threads.append(threading.Thread(target=reader_loop, name=f"r-{t.id}",
                                        args=(t, stop, clock, cfg,
                                              reader_fatal),
                                        daemon=True))

    duration = float(w["duration_s"])
    print(f"[run] mode={mode}, {len(targets)} reader thread(s) + 1 writer, "
          f"{duration}s")
    for th in threads:
        th.start()

    try:
        end_at = time.monotonic() + duration
        while time.monotonic() < end_at and not stop.is_set():
            time.sleep(1.0)
            if int(time.monotonic()) % 10 == 0:
                print(f"[run] {len(writes)} writes, "
                      f"{sum(len(t.records) for t in targets)} reads")
    except KeyboardInterrupt:
        print("[run] interrupted, stopping cleanly")
    finally:
        stop.set()
        for th in threads:
            th.join(timeout=10)

    if fatal:
        print(f"[run] WARNING the writer died and ended the run early: "
              f"{fatal[0]}")
    for msg in reader_fatal:
        print(f"[run] ABORTED {msg}")

    warmup_ns = int(w.get("warmup_s", 5) * 1e9)
    start_perf = anchor["perf_ns"]
    with (run_dir / "writes.jsonl").open("w") as fh:
        for rec in writes:
            rec["warmup"] = (rec["t_start_ns"] - start_perf) < warmup_ns
            fh.write(json.dumps(rec, default=str) + "\n")
    with (run_dir / "reads.jsonl").open("w") as fh:
        for t in targets:
            for rec in t.records:
                rec["warmup"] = (rec["t_start_ns"] - start_perf) < warmup_ns
                fh.write(json.dumps(rec, default=str) + "\n")

    topo_after = provenance.topology(rs_client)
    power_after = provenance.power_source()["source"]
    provenance.dump(run_dir / "provenance", "topology_after.json", topo_after)
    if topo_after["label"] != topo_before["label"]:
        print(f"[run] WARNING topology changed during the run: "
              f"{topo_before['label']!r} -> {topo_after['label']!r}")
    provenance.dump(run_dir / "provenance", "repl_after.json",
                    provenance.repl_snapshot(rs_client))
    provenance.dump(run_dir / "provenance", "server_status_after.json",
                    provenance.server_status(rs_client))
    provenance.dump(run_dir, "manifest.json", {
        "run_id": run_id,
        "harness_version": 3,
        "read_path": "Collection.find_one for every target and calibration",
        "mode": mode,
        "target": target_id,
        "round_index": round_index if round_index is not None else 1,
        "writer_fatal": fatal,
        "reader_fatal": reader_fatal,
        "wire": [wl.summary() for wl in wires],
        "started_utc": stamp,
        "finished_utc": datetime.now(timezone.utc).isoformat(),
        "clock_anchor": anchor,
        "expect": expect,
        "topology": {"before": topo_before["label"],
                     "after": topo_after["label"]},
        "chaining_allowed": topo_before["chainingAllowed"],
        "journal_default": topo_before["writeConcernMajorityJournalDefault"],
        "power": {"before": env["power"], "after": power_after},
        "clock_note": ("All t_*_ns are time.perf_counter_ns() from one "
                       "process. Threads share that clock, so writer and "
                       "reader times are directly comparable with no skew. "
                       "wall = anchor.wall_ns + (t - anchor.perf_ns)."),
        "threading_note": ("Writer and each reader run on their own thread "
                           "with their own session. Causal readers copy the "
                           "writer's operationTime and clusterTime out under "
                           "a lock and advance their own session on their own "
                           "thread, so no session is touched by two threads."),
        "totals": {"writes": len(writes),
                   "reads": {t.id: len(t.records) for t in targets}},
        "config": cfg,
    })

    if cap.get("compress_output"):
        for name in ("writes.jsonl", "reads.jsonl"):
            p = run_dir / name
            if p.exists():
                with p.open("rb") as src, gzip.open(str(p) + ".gz", "wb") as dst:
                    shutil.copyfileobj(src, dst)
                p.unlink()

    provenance.write_checksums(run_dir)
    print(f"[run] done: {len(writes)} writes, "
          f"{sum(len(t.records) for t in targets)} reads -> {run_dir}")
    if fatal or reader_fatal:
        # Data and manifest are on disk; the exit status tells run_all.
        raise SystemExit(f"[run] run ended early: {(fatal + reader_fatal)[0]}")
    return run_dir


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, type=Path)
    ap.add_argument("--out", type=Path, default=REPO_ROOT / "measurements")
    ap.add_argument("--calibrate", action="store_true")
    ap.add_argument("--target", default=None,
                    help="target id to read from; required when "
                         "workload.mode is 'isolated'")
    ap.add_argument("--round-index", type=int, default=None,
                    help="round number, recorded in the run id and manifest")
    args = ap.parse_args()
    cfg = yaml.safe_load(args.config.read_text())
    run(cfg, args.config, args.out, args.calibrate, args.target,
        args.round_index)


if __name__ == "__main__":
    main()
