"""Offline dry run of the collector: fake PyMongo objects, no server.

Exercises config validation, the read path, reader and writer slots, the
command listener and its abort rule, reply operationTime, provenance and the
expect check, linearizable targets, and whole runs of collect.run() against an
in-memory fake MongoClient. Nothing here opens a network connection.

Usage, from anywhere:
    .venv/bin/python tests/dryrun.py                  # every group
    .venv/bin/python tests/dryrun.py wire optime      # named groups only
Exit status is non-zero if any check fails.
"""
import copy
import gzip
import json
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import yaml  # noqa: E402
from bson.timestamp import Timestamp  # noqa: E402

from harness import collect  # noqa: E402
from harness.wire import WireListener  # noqa: E402

FAILS = []
SHARED = {"doc": {"_id": "probe-1", "seq": 7, "wall_ns": 1},
          "members": None, "jd": True, "chaining": False}


def check(cond, msg):
    print(("  ok   " if cond else "  FAIL ") + msg)
    if not cond:
        FAILS.append(msg)


def load_cfg(name):
    return yaml.safe_load((REPO / "config" / f"{name}.yaml").read_text())


# ---------------------------------------------------------------- fakes

class FakeSession:
    def __init__(self):
        self.operation_time = None
        self.cluster_time = None

    def advance_cluster_time(self, ct):
        self.cluster_time = ct

    def advance_operation_time(self, ot):
        if self.operation_time is None or ot > self.operation_time:
            self.operation_time = ot


class _Ev:
    def __init__(self, **kw):
        self.__dict__.update(kw)


class FakeColl:
    """Mimics Collection.with_options / find_one, and fires the started and
    succeeded command events PyMongo publishes for a find."""

    def __init__(self, client, opts=None):
        self.client, self.opts = client, opts or {}

    def with_options(self, **kw):
        return FakeColl(self.client, kw)

    def find_one(self, flt, session=None, **kw):
        c = self.client
        c.calls.append({"filter": flt, "session": session, "opts": self.opts, "kw": kw})
        rc = {}
        level = self.opts["read_concern"].level if self.opts.get("read_concern") else None
        if level:
            rc["level"] = level
        if session is not None and session.operation_time is not None and not c.drop_act:
            rc["afterClusterTime"] = session.operation_time
        cmd = {"find": "probe", "filter": flt, "limit": 1, "singleBatch": True}
        if rc:
            cmd["readConcern"] = rc
        c.req += 1
        for lst in c.listeners:
            lst.started(_Ev(command_name="find", command=cmd, request_id=c.req))
        time.sleep(c.latency_s(c.req) if callable(c.latency_s) else c.latency_s)
        # Older than any writer opTime the tests publish, so a session's
        # running maximum and the reply's own operationTime differ.
        reply_ot = Timestamp(500, c.req)
        c.replies[c.req] = reply_ot   # ground truth for the operationTime checks
        reply = {"cursor": {"firstBatch": [c.doc]}, "ok": 1.0,
                 "operationTime": reply_ot}
        if session is not None:
            session.advance_operation_time(reply_ot)
        for lst in c.listeners:
            lst.succeeded(_Ev(command_name="find", reply=reply, request_id=c.req))
        return dict(c.doc)


class FakeClient:
    def __init__(self, latency_s=0.0002, listeners=(), drop_act=False):
        self.calls, self.req = [], 0
        self.replies = {}
        self.latency_s = latency_s
        self.listeners = list(listeners)
        self.drop_act = drop_act

    @property
    def doc(self):
        return SHARED["doc"]

    def __getitem__(self, name):
        return _FakeDB(self)


class _FakeDB:
    def __init__(self, client):
        self.client = client

    def __getitem__(self, name):
        return FakeColl(self.client)


def make_target(spec, cfg, client):
    t = collect.Target(id=spec["id"], kind=spec["kind"],
                       read_preference=spec["read_preference"],
                       read_concern=spec.get("read_concern"),
                       host=spec.get("host"),
                       causal_consistency=bool(spec.get("causal_consistency")),
                       max_time_ms=spec.get("max_time_ms"),
                       read_interval_us=cfg["workload"].get("read_interval_us", 500))
    t.client = client
    t.wire = WireListener(f"target:{t.id}")
    client.listeners.append(t.wire)
    if t.causal_consistency:
        t.session = FakeSession()
    return t


def run_reader(t, cfg, seconds=0.3, clock=None, fatal=None):
    clock = clock or collect.WriteClock()
    clock.publish(Timestamp(999, 1), {"clusterTime": Timestamp(999, 1)}, 3)
    stop = threading.Event()
    fatal = [] if fatal is None else fatal
    th = threading.Thread(target=collect.reader_loop, args=(t, stop, clock, cfg, fatal))
    th.start()
    time.sleep(seconds)
    stop.set()
    th.join(5)
    return t.records


# ---------------------------------------------------------------- unit groups

def test_configs():
    print("configs")
    for p in sorted((REPO / "config").glob("*.yaml")):
        cfg = yaml.safe_load(p.read_text())
        try:
            collect.check_workload(cfg)
        except SystemExit as e:
            if "cycles" in cfg["workload"]:
                print(f"  skip {p.name}: rejected as expected for a v1 config")
            else:
                check(False, f"{p.name}: {e}")
            continue
        collect.check_expect_schema(cfg)
        collect.check_targets(cfg)
        mode = collect.resolve_mode(cfg)
        for s in cfg["targets"]:
            collect.select_specs(cfg, mode, s["id"])
        check(True, f"{p.name}: {len(cfg['targets'])} targets validate")


def test_read_path():
    print("read path")
    cfg = load_cfg("threaded-w1")
    for spec in cfg["targets"]:
        client = FakeClient()
        t = make_target(spec, cfg, client)
        recs = run_reader(t, cfg, 0.1)
        c = client.calls[0]
        check(len(recs) > 50 and all(r["observed_seq"] == 7 for r in recs),
              f"{t.id}: {len(recs)} reads via find_one, all observed seq 7")
        check(c["opts"]["read_concern"].level == spec.get("read_concern")
              and type(c["opts"]["read_preference"]).__name__.lower()
              == spec["read_preference"].lower(),
              f"{t.id}: with_options read_concern={c['opts']['read_concern'].level} "
              f"read_preference={type(c['opts']['read_preference']).__name__}")
        check((c["session"] is not None) == t.causal_consistency,
              f"{t.id}: session passed only for causal targets")
    t = make_target(cfg["targets"][-1], cfg, FakeClient())
    cal = collect.calibrate([t], cfg, n=50)[t.id]
    check(cal["n"] == 50 and cal["errors"] == 0, "calibrate: 50 find_one reads, 0 errors")
    spec = next(s for s in cfg["targets"] if s.get("causal_consistency"))
    t = make_target(spec, cfg, FakeClient())
    collect.calibrate([t], cfg, n=20)
    check(all(c["session"] is t.session for c in t.client.calls),
          "calibrate: causal target reads with its session")


def test_schedule():
    print("reader slots")
    cfg = load_cfg("threaded-w1")
    gap = cfg["workload"]["read_interval_us"] * 1000
    slow = lambda n: 0.0023 if n % 10 == 0 else 0.0001  # noqa: E731
    t = make_target(cfg["targets"][4], cfg, FakeClient(latency_s=slow))
    recs = run_reader(t, cfg, 0.5)
    check(all("t_sched_ns" in r and "missed_slots" in r for r in recs),
          "every read has t_sched_ns and missed_slots")
    check(all((r["t_sched_ns"] - recs[0]["t_sched_ns"]) % gap == 0 for r in recs),
          "every scheduled time is on the slot grid")
    lag = sorted(r["t_start_ns"] - r["t_sched_ns"] for r in recs)
    check(lag[0] >= 0, f"no read sent before its slot (min lag {lag[0]/1e3:.1f} us, "
          f"p99 {lag[int(len(lag)*.99)]/1e3:.1f} us)")
    starts = [r["t_start_ns"] for r in recs]
    min_gap = min(b - a for a, b in zip(starts, starts[1:]))
    check(min_gap > gap * 0.9, f"no catch-up bursts: min gap between sends {min_gap/1e3:.0f} us")
    skipped = sum(r["missed_slots"] for r in recs)
    slow_reads = sum(1 for r in recs if r["latency_ns"] > 2e6)
    check(skipped >= slow_reads * 4,
          f"{slow_reads} slow reads, {skipped} slots skipped (expect ~4-5 per slow read)")
    span = (recs[-1]["t_sched_ns"] - recs[0]["t_sched_ns"]) // gap + 1
    check(span == len(recs) + sum(r["missed_slots"] for r in recs[1:]),
          f"reads + skipped slots account for every slot ({span})")


def test_wire():
    print("command listener")
    cfg = load_cfg("threaded-w1")
    for spec in cfg["targets"]:
        t = make_target(spec, cfg, FakeClient())
        fatal = []
        recs = run_reader(t, cfg, 0.05, fatal=fatal)
        lv = {r["read_concern_sent"] for r in recs}
        acts = [r["after_cluster_time_sent"] for r in recs]
        ok = (all(a is not None for a in acts) if t.causal_consistency
              else all(a is None for a in acts))
        check(ok and lv == {spec.get("read_concern")} and not fatal,
              f"{t.id}: read_concern_sent={lv}, afterClusterTime on "
              f"{sum(a is not None for a in acts)}/{len(acts)} reads, no abort")
    spec = next(s for s in cfg["targets"] if s.get("causal_consistency"))
    t = make_target(spec, cfg, FakeClient(drop_act=True))
    fatal = []
    recs = run_reader(t, cfg, 0.2, fatal=fatal)
    check(len(fatal) == 1 and len(recs) == 1 and "afterClusterTime" in fatal[0],
          f"causal read without afterClusterTime aborts at once: {fatal}")
    spec_nc = next(s for s in cfg["targets"] if not s.get("causal_consistency"))
    t = make_target(spec_nc, cfg, FakeClient(drop_act=True))
    fatal = []
    run_reader(t, cfg, 0.05, fatal=fatal)
    check(not fatal, "non-causal target is never aborted by the rule")
    t = make_target(spec, cfg, FakeClient(drop_act=True))
    try:
        collect.calibrate([t], cfg, n=20)
        check(False, "calibration should abort")
    except SystemExit as e:
        check("calibration read 1" in str(e), f"calibration aborts: {e}")
    t = make_target(spec, cfg, FakeClient())
    collect.calibrate([t], cfg, n=20)
    summ = t.wire.summary()
    lvl = spec["read_concern"]
    check(summ["read_concerns"] == {f"find level={lvl} afterClusterTime=no": 1,
                                    f"find level={lvl} afterClusterTime=yes": 19},
          f"listener summary: {summ['read_concerns']}")


def test_optime():
    print("reply operationTime")
    cfg = load_cfg("threaded-w1")
    for spec in cfg["targets"]:
        t = make_target(spec, cfg, FakeClient())
        recs = run_reader(t, cfg, 0.03)
        ok = all(r["operationTime"] and r["operationTime"]["t"] == 500 for r in recs)
        extra = f", session holds {t.session.operation_time}" if t.session is not None else ""
        check(ok, f"{t.id}: operationTime is the reply's (t=500) on all "
              f"{len(recs)} reads{extra}")
        if t.causal_consistency:
            truth = t.client.replies
            same = all(r["request_id"] in truth
                       and (r["operationTime"]["t"], r["operationTime"]["i"])
                       == (truth[r["request_id"]].time, truth[r["request_id"]].inc)
                       for r in recs)
            logged = {x["request_id"]: (x["operationTime"]["t"], x["operationTime"]["i"])
                      for x in t.wire.replies()}
            check(same and logged == {k: (v.time, v.inc) for k, v in truth.items()},
                  f"{t.id}: each read's operationTime is the reply the driver "
                  f"sent for its request_id, and the listener logged all "
                  f"{len(logged)} replies")


def test_writer_schedule():
    print("writer slots")
    cfg = load_cfg("threaded-w1")
    cfg["workload"]["write_interval_ms"] = 2
    gap = 2_000_000
    SHARED["write_latency"] = lambda n: 0.007 if n % 5 == 4 else 0.0002
    client = FakeMongoClient("mongodb://fake")
    stop, out = threading.Event(), []
    th = threading.Thread(target=collect._writer_body,
                          args=(stop, collect.WriteClock(), out, client, cfg))
    th.start()
    time.sleep(0.5)
    stop.set()
    th.join(5)
    SHARED.pop("write_latency")
    check(all("t_sched_ns" in w and "missed_slots" in w for w in out),
          f"every write has t_sched_ns and missed_slots ({len(out)} writes)")
    check(all((w["t_sched_ns"] - out[0]["t_sched_ns"]) % gap == 0 for w in out),
          "every scheduled write time is on the slot grid")
    starts = [w["t_start_ns"] for w in out]
    mg = min(b - a for a, b in zip(starts, starts[1:]))
    check(mg > gap * 0.9, f"no back-to-back catch-up writes: min gap {mg/1e3:.0f} us")
    slow = sum(1 for w in out if w["latency_ns"] > 5e6)
    skipped = sum(w["missed_slots"] for w in out)
    check(skipped >= slow * 3, f"{slow} slow writes, {skipped} write slots skipped "
          f"(expect 3 per slow write)")
    span = (out[-1]["t_sched_ns"] - out[0]["t_sched_ns"]) // gap + 1
    check(span == len(out) + sum(w["missed_slots"] for w in out[1:]),
          f"writes + skipped slots account for every slot ({span})")
    check([w["seq"] for w in out] == list(range(len(out))), "seq has no gaps")


# ---------------------------------------------------------------- end to end

def _members(layout):
    """layout like {"27018": "27017", ...}; 27017 is primary."""
    out = [{"name": "127.0.0.1:27017", "stateStr": "PRIMARY", "syncSourceHost": ""}]
    for port, src in sorted(layout.items()):
        out.append({"name": f"127.0.0.1:{port}", "stateStr": "SECONDARY",
                    "syncSourceHost": f"127.0.0.1:{src}" if src else ""})
    return out


STAR = {"27018": "27017", "27019": "27017", "27020": "27017"}
CHAINED = {"27018": "27020", "27019": "27020", "27020": "27017"}


class FakeAdmin:
    def command(self, cmd, *a, **kw):
        name = cmd if isinstance(cmd, str) else next(iter(cmd))
        if name == "replSetGetStatus":
            return {"members": SHARED["members"], "ok": 1}
        if name == "replSetGetConfig":
            return {"config": {"_id": "rs0",
                               "writeConcernMajorityJournalDefault": SHARED["jd"],
                               "settings": {"chainingAllowed": SHARED["chaining"]}},
                    "ok": 1}
        if name == "buildInfo":
            return {"version": "8.3.7"}
        if name == "serverStatus":
            return {"host": "fake", "version": "8.3.7"}
        raise RuntimeError(f"fake admin has no {name}")


class FakeWriteDB(_FakeDB):
    def command(self, cmd, session=None, **kw):
        seq = cmd["updates"][0]["u"]["$set"]["seq"]
        time.sleep(SHARED.get("write_latency", lambda n: 0.0005)(seq))
        SHARED["doc"] = {"_id": "probe-1", "seq": seq, "wall_ns": time.time_ns()}
        ot = Timestamp(2000, seq + 10)
        return {"ok": 1.0, "n": 1, "nModified": 1, "operationTime": ot,
                "$clusterTime": {"clusterTime": ot}}

    def __getitem__(self, name):
        return _UpsertColl(self.client)


class _UpsertColl(FakeColl):
    def update_one(self, flt, upd, upsert=False):
        SHARED["doc"] = {"_id": flt["_id"], **upd["$set"]}


class FakeMongoClient(FakeClient):
    instances = []

    def __init__(self, uri, event_listeners=(), **kw):
        super().__init__(listeners=event_listeners)
        self.uri, self.kw = uri, kw
        self.admin = FakeAdmin()
        FakeMongoClient.instances.append(self)

    def __getitem__(self, name):
        return FakeWriteDB(self)

    def start_session(self, causal_consistency=True):
        return FakeSession()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def close(self):
        pass


def e2e(cfg_name, target, out, members=STAR, jd=True, chaining=False, mutate=None):
    from harness import provenance
    real_run = provenance._run
    # sntp would contact time.apple.com; everything else runs for real
    provenance._run = lambda cmd: ({"cmd": " ".join(cmd), "rc": 0, "stdout": "stubbed",
                                    "stderr": ""} if cmd[0] == "sntp" else real_run(cmd))
    collect.MongoClient = FakeMongoClient
    FakeMongoClient.instances = []
    SHARED.update(members=_members(members), jd=jd, chaining=chaining)
    cfg_path = REPO / "config" / f"{cfg_name}.yaml"
    cfg = yaml.safe_load(cfg_path.read_text())
    cfg["workload"].update(duration_s=2, warmup_s=0.5)
    if mutate:
        mutate(cfg)
    try:
        return collect.run(cfg, cfg_path, out, True, target), None
    except SystemExit as e:
        return None, str(e)
    finally:
        provenance._run = real_run


def _jl(run_dir, name):
    with gzip.open(run_dir / f"{name}.jsonl.gz", "rt") as fh:
        return [json.loads(line) for line in fh]


def test_e2e():
    print("end to end with fake MongoClient (run())")
    out = Path(tempfile.mkdtemp())
    try:
        run_dir, err = e2e("threaded-w1", "rs-secondary-causal-majority", out)
        check(err is None and run_dir is not None, f"run completed: {err or run_dir.name}")
        if run_dir is None:
            return
        m = json.loads((run_dir / "manifest.json").read_text())
        reads, writes = _jl(run_dir, "reads"), _jl(run_dir, "writes")
        check(len(writes) > 10 and len(reads) > 1000, f"{len(writes)} writes, {len(reads)} reads")
        check(m["harness_version"] == 3 and m["reader_fatal"] == [], "manifest v3, no reader_fatal")
        names = sorted(w["client"] for w in m["wire"])
        check(names[0] == "node:127.0.0.1:27017" and "writer" in names
              and "target:rs-secondary-causal-majority" in names,
              f"wire summary covers every client: {names}")
        tw = next(w for w in m["wire"] if w["client"].startswith("target:"))
        check(set(tw["read_concerns"]) == {"find level=majority afterClusterTime=no",
                                          "find level=majority afterClusterTime=yes"}
              and tw["read_concerns"]["find level=majority afterClusterTime=no"] == 1,
              f"target wire log: {tw['read_concerns']}")
        check(all(r["after_cluster_time_sent"] for r in reads),
              "every collected causal read carried afterClusterTime")

        import importlib.util
        import pandas as pd
        spec = importlib.util.spec_from_file_location(
            "val_report", REPO / "scripts" / "val_report.py")
        vr = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(vr)
        reps = _jl(run_dir, "find_replies")
        rdf, pdf = pd.DataFrame(reads), pd.DataFrame(reps)
        res = vr.optime_check(rdf, pdf)
        check(res["status"] == "PASS" and res["matched"] == len(reads),
              f"val_report optime_check on the run: {res['status']}, "
              f"{res['equal']}/{res['reads']} equal, {len(reps)} replies logged")
        bad = rdf.copy()
        bad.at[bad.index[100], "operationTime"] = {"t": 1, "i": 1}
        check(vr.optime_check(bad, pdf)["status"] == "FAIL",
              "optime_check fails when one recorded operationTime is altered")
        # The log also holds the calibration reads' replies (not in reads.jsonl),
        # so drop the reply belonging to an actual collected read.
        gone = rdf["request_id"].iloc[100]
        check(vr.optime_check(rdf, pdf[pdf["request_id"] != gone])["status"] == "FAIL",
              "optime_check fails when a read's reply is missing from the log")
        bad = rdf.copy()
        bad.at[bad.index[10], "request_id"] = None
        check(vr.optime_check(bad, pdf)["status"] == "FAIL",
              "optime_check fails when one read has no request_id")
    finally:
        shutil.rmtree(out)


def test_provenance():
    from harness import provenance, runs
    print("provenance and expect")
    out = Path(tempfile.mkdtemp())
    seen = {}
    real_git = provenance.git_state

    def spy(root):
        seen["dirs_at_git_state"] = list(out.iterdir())
        return real_git(root)

    provenance.git_state = spy
    try:
        run_dir, err = e2e("threaded-w1", "rs-primary-local", out)
        check(err is None, f"matching star run completes ({err})")
        check(seen["dirs_at_git_state"] == [], "git_state ran before the run folder existed")
        prov = run_dir / "provenance"
        tb = json.loads((prov / "topology_before.json").read_text())
        check(tb["label"] == "star" and tb["chainingAllowed"] is False
              and tb["writeConcernMajorityJournalDefault"] is True
              and all("syncSourceHost" in m for m in tb["members"]),
              f"topology_before.json: {tb['label']}, members with syncSourceHost")
        check((prov / "topology_after.json").exists(), "topology_after.json written")
        nodes = json.loads((prov / "nodes.json").read_text())
        check(all(n["writeConcernMajorityJournalDefault"] is True for n in nodes),
              "nodes.json journal default read via replSetGetConfig")
        env = json.loads((prov / "client_env.json").read_text())
        check(env["power"] in ("AC", "battery"), f"client_env power = {env['power']} (pmset)")
        m = json.loads((run_dir / "manifest.json").read_text())
        check(m["topology"] == {"before": "star", "after": "star"}
              and m["journal_default"] is True and m["power"]["before"] == env["power"],
              f"manifest: topology {m['topology']}, power {m['power']}")
        row = runs.describe(run_dir)
        check(row["topology"] == "star" and row["journal_default"] == "true"
              and row["power"] == env["power"], f"runs.describe on a v3 run: {row}")

        n_before = len(list(out.iterdir()))
        cases = [("chained live, star expected", "threaded-w1", dict(members=CHAINED), "topology is"),
                 ("journal default off, on expected", "threaded-w1", dict(jd=False),
                  "writeConcernMajorityJournalDefault is"),
                 ("chaining on, off expected", "threaded-w1-nochain", dict(chaining=True),
                  "chainingAllowed is")]
        for label, cfg_name, kw, needle in cases:
            rd, err = e2e(cfg_name, "direct-27017", out, **kw)
            check(rd is None and err and needle in err and "refusing to start" in err,
                  f"{label}: refused ({err[:90] if err else err}...)")
        rd, err = e2e("threaded-w1", "direct-27017", out, mutate=lambda c: c.pop("expect"))
        check(rd is None and "no expect block" in err, f"missing expect: {err}")
        check(len(list(out.iterdir())) == n_before, "no folder created by any refused run")
        rd, err = e2e("a1-chained", "direct-27018", out, members=CHAINED, chaining=True)
        check(err is None, "a1-chained accepts the chained layout")
    finally:
        provenance.git_state = real_git
        shutil.rmtree(out)


def test_linearizable():
    print("w:2, w:3 and linearizable")
    for name, w in (("threaded-w2", 2), ("threaded-w3", 3)):
        cfg = load_cfg(name)
        check(cfg["workload"]["write_concern"]["w"] == w and cfg["experiment"]["name"] == name,
              f"{name}: w={cfg['workload']['write_concern']['w']}, {len(cfg['targets'])} targets")
    cfg = load_cfg("threaded-w1")
    spec = next(s for s in cfg["targets"] if s.get("read_concern") == "linearizable")
    client = FakeClient()
    t = make_target(spec, cfg, client)
    recs = run_reader(t, cfg, 0.05)
    check({r["read_concern_sent"] for r in recs} == {"linearizable"}
          and all(r["after_cluster_time_sent"] is None for r in recs),
          f"{t.id}: {len(recs)} reads sent readConcern linearizable, no afterClusterTime")
    check(all(c["kw"].get("max_time_ms") == 5000 for c in client.calls)
          and type(client.calls[0]["opts"]["read_preference"]).__name__ == "Primary",
          "find_one gets max_time_ms=5000 and Primary read preference")
    for key, val, needle in [("read_preference", "secondary", "need read_preference primary"),
                             ("kind", "direct", "kind replicaset"),
                             ("causal_consistency", True, "cannot carry afterClusterTime"),
                             ("max_time_ms", None, "max_time_ms")]:
        c2 = copy.deepcopy(cfg)
        s2 = next(s for s in c2["targets"] if s.get("read_concern") == "linearizable")
        s2[key] = val
        try:
            collect.check_targets(c2)
            check(False, f"linearizable with {key}={val} should be refused")
        except SystemExit as e:
            check(needle in str(e), f"refused linearizable with {key}={val}")
    out = Path(tempfile.mkdtemp())
    try:
        run_dir, err = e2e("threaded-w2", "rs-primary-linearizable", out)
        check(err is None, f"end-to-end threaded-w2 linearizable run: {err or run_dir.name}")
    finally:
        shutil.rmtree(out)


GROUPS = ["configs", "read_path", "schedule", "wire", "optime", "writer_schedule",
          "e2e", "provenance", "linearizable"]

if __name__ == "__main__":
    for name in sys.argv[1:] or GROUPS:
        globals()[f"test_{name}"]()
    print(f"\n{len(FAILS)} failure(s)")
    sys.exit(1 if FAILS else 0)
