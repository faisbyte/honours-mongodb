# mongo-freshness

Client-side measurement of replica staleness and consistency trade-offs on a
local MongoDB replica set, following the consumer's-view method of Wada et al.
(CIDR 2011).

## Install

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Replica set

Four `mongod` processes on one machine, replica set `rs0`, ports 27017 to
27020, 27017 at priority 10. Each run records the server version in
`provenance/nodes.json`; every run collected so far reports MongoDB 8.3.7,
FCV 8.3.

```bash
scripts/start.sh unchained   # or: chained. Sets chainingAllowed explicitly.
scripts/stop.sh              # secondaries first, primary last
```

Data lives in `$MONGO_RS_DIR` (default `~/mongo-rs`). `start.sh` sets whether
chaining is allowed, not which node syncs from which; `scripts/a1_alternate.sh`
shows how to force a chain with `replSetSyncFrom`.

## Design

**One writer, one reader, one process.** `harness.collect` runs a writer
thread and one reader thread for a single read path (`--target`). The writer
updates one probe document every `write_interval_ms`, writing an increasing
`seq`. The reader reads it on a fixed grid of slots `read_interval_us` apart,
before, during and after every write. `harness.run_all` sweeps every target of
a config, one process per target, so reader threads never share a process
(nine readers in one process inflate read latency about tenfold). Setting
`workload.mode: concurrent` runs every target in one process instead.

**Slots.** Writer and reader each run on a fixed grid of slots. A slot that
passed while the previous operation was still in flight is skipped and counted
(`missed_slots`), never made up, so a slow write or read does not cause a
burst of back-to-back operations.

**One read path for every target.** Every target, and calibration, reads with
`Collection.find_one` on a collection built with
`with_options(read_preference, read_concern)`. Causal targets pass their
session. Nothing reads through `Database.command`: in PyMongo 4.18 it never
applies a session's `afterClusterTime`, which is the bug that voided the early
causal runs (see `void/VOID.md`).

**Causal targets.** The writer publishes each write's `operationTime` and
`$clusterTime`; a causal reader advances its own session with them, on its own
thread, before each read.

**What went on the wire.** Every client carries a PyMongo `CommandListener`
(`harness/wire.py`). Each read records the `readConcern` level and
`afterClusterTime` exactly as sent, and the reply's own `operationTime`. If a
causal target sends a read without `afterClusterTime` once its session has an
operation time, the run stops at once with `reader_fatal` in the manifest and
a non-zero exit, and the same fault during calibration stops it before
collection.

**Replica set state is part of the condition.** Every config declares the
topology and journal default it is meant to measure (`expect`, below). Before
creating the run folder, the collector reads them from the live replica set
and refuses to start on any mismatch.

## Run

```bash
python -m harness.collect --config config/threaded-w1.yaml --target rs-primary-local --calibrate
python -m harness.run_all --config config/threaded-w1.yaml --rounds 3
python -m harness.run_all --config config/threaded-w1.yaml --targets rs-secondary-causal-local
```

`--calibrate` first times 500 reads against a value that cannot be stale.
Anything below that p50 is under the harness's resolution.

`scripts/a1_alternate.sh` (chained against unchained) and
`scripts/a2_journal.sh`, `scripts/a2_reads.sh` (journal default against write
`j` and majority read paths) reconfigure the set between sweeps.

## Which runs are analysed: `runs/VALID.csv`

Analysis scripts never glob `measurements/`. They select rows from
`runs/VALID.csv`, so a run that is not listed cannot be analysed.

| column | meaning |
|---|---|
| `run_id` | folder name under `measurements/` |
| `config` | `experiment.name` from the run's manifest |
| `topology` | sync sources at the start of the run: `star`, or an explicit map such as `27018<27020 27019<27020 27020<27017` |
| `journal_default` | `writeConcernMajorityJournalDefault` at the start of the run |
| `power` | `AC` or `battery`, from `pmset -g batt` |
| `include` | `yes` or `no`; the only column meant for hand editing |

```bash
python -m harness.runs build    # append rows for new run folders, from their provenance
python -m harness.runs check    # every row against its provenance; unlisted folders
python -m harness.runs list --config a1-chained --topology star
```

Every analysis script takes the same selection flags: `--run-id`, `--config`,
`--target`, `--topology`, `--journal-default`, `--power`, and
`--include-excluded`.

`void/` holds runs and merges that must not be analysed, with the reason for
each in `void/VOID.md`.

## Analysis

| script | output |
|---|---|
| `harness.analyse` | freshness curves P(fresh) by elapsed time from write start and from ack, with Wilson 95% intervals; headline numbers, errors, read latency, monotonic-read violations, lag in writes. One run goes to `<run>/derived/`, several to `measurements/merged/` |
| `harness.event_timings` | per write and per path, the interval in which the new value became visible; summaries, CDF and `TIMINGS.md` |
| `harness.ack_latency` | write acknowledgement latency distributions |
| `harness.visibility_chart` | bar chart of visibility time for three w:1 paths |
| `harness.describe_setup` | `SETUP.md` from the provenance of a set of runs |
| `harness.ryw_test`, `harness.ryw_figure` | direct read-your-writes test of causal sessions, raw `db.command` against `find_one` |

`event_timings` and `ack_latency` key a condition on w, j, journal default and
topology, so runs that differ in any of the four are never pooled.

```bash
python -m harness.analyse --run-id <run_id>
python -m harness.analyse --config threaded-w1 --topology star
python -m harness.event_timings --config a1-chained a1-unchained
python -m harness.ack_latency --config a2-jnone a2-jfalse a2-jtrue
```

Every run is joined on its own before anything is pooled: `perf_counter_ns`
values from different processes share no epoch.

## Config

One YAML file per condition. Never edit a config to change a condition; copy
it. The copy inside each run folder is the record of what was measured.

```yaml
experiment:
  name: threaded-w1            # becomes part of the run folder name
replica_set:
  name: rs0
  seed_hosts: ["127.0.0.1:27017", "127.0.0.1:27018", "127.0.0.1:27019", "127.0.0.1:27020"]
expect:                        # required; checked before the run folder exists
  topology: star               # or an explicit map, quoted
  journal_default: true
  chaining_allowed: false      # optional
workload:
  database: freshness
  collection: probe
  doc_key: probe-1
  payload_bytes: 14
  mode: isolated               # or concurrent
  duration_s: 180              # seconds of collection per run, warmup included
  warmup_s: 5                  # flagged as warmup, still written to disk
  write_interval_ms: 100       # must exceed write latency
  read_interval_us: 500        # slot gap per target
  write_concern: {w: 1, j: null, wtimeout_ms: 5000}
targets:
  - id: rs-secondary-causal-majority
    kind: replicaset           # or direct, with host:
    read_preference: secondary
    read_concern: majority     # local | majority | linearizable | omitted
    causal_consistency: true
    # max_time_ms: 5000        # required for linearizable, which must also be
                               # kind replicaset, read_preference primary, not causal
capture:
  compress_output: true
analysis:
  bucket_us: 500
  min_samples_per_bucket: 30
```

`config/baseline.yaml` and `config/w1.yaml` describe the v1 write-then-poll
harness and are rejected by the current collector.

## Run folder

```
measurements/<UTC stamp>__<experiment>__<target>[__r<round>]/
├── config.yaml                 verbatim copy of the config
├── manifest.json               run id, mode, clock anchor, expect, topology and power
│                               before/after, wire summary per client, totals,
│                               writer_fatal, reader_fatal, resolved config
├── writes.jsonl.gz             one record per write
├── reads.jsonl.gz              one record per read
├── checksums.txt               sha256 of every file
└── provenance/
    ├── client_env.json         python, pymongo, OS, CPU, power source, clock sync
    ├── git.json                commit, and whether the tree was dirty before the run
    ├── nodes.json              per mongod: buildInfo, hostInfo, getCmdLineOpts, FCV,
    │                           journal default (from replSetGetConfig)
    ├── topology_before.json    each member's syncSourceHost, chainingAllowed,
    ├── topology_after.json     journal default, and the topology label
    ├── repl_before.json        replSetGetStatus, replSetGetConfig, hello
    ├── repl_after.json
    ├── server_status_*.json    trimmed serverStatus
    └── calibration.json        read round-trip distribution, if --calibrate
```

Raw files are committed. `derived/` is regenerated by `analyse.py` and
gitignored.

## Data schema

### `writes.jsonl`

| field | meaning |
|---|---|
| `seq` | value written into the probe document |
| `t_sched_ns` | the slot this write was scheduled for |
| `missed_slots` | write slots skipped just before this write |
| `t_start_ns`, `t_ack_ns` | `perf_counter_ns` just before sending and just after the ack |
| `latency_ns` | `t_ack_ns - t_start_ns` |
| `operationTime` | `{t, i}` from the reply |
| `write_concern` | the write concern document sent |
| `ok`, `n`, `nModified`, `writeConcernError`, `error` | outcome |
| `warmup` | written within `warmup_s` of the start |

### `reads.jsonl`

| field | meaning |
|---|---|
| `target` | read path id |
| `t_sched_ns` | the slot this read was scheduled for |
| `missed_slots` | slots skipped just before this read |
| `t_start_ns`, `t_end_ns` | `perf_counter_ns` just before `find_one` and just after it returned |
| `latency_ns` | `t_end_ns - t_start_ns` |
| `observed_seq`, `observed_wall_ns` | from the document returned, or null |
| `operationTime` | `{t, i}` from this read's own reply |
| `causal_waited_for_seq` | causal targets: the write whose timestamps the session was advanced to |
| `read_concern_sent` | `readConcern.level` as sent |
| `after_cluster_time_sent` | `readConcern.afterClusterTime` as sent, `{t, i}` or null |
| `error`, `warmup` | as above |

No freshness verdicts are stored. Freshness is a derived judgement made in the
analysis scripts, so changing its definition never requires re-collecting.

### Clock

All `*_ns` values come from `time.perf_counter_ns()` in one client process, so
writer and reader times share a monotonic clock with no skew. `manifest.json`
records an anchor pair mapping that clock to wall time. This holds only while
writer and reader share a process on one host.

## Harness versions

| `harness_version` | design |
|---|---|
| none (v1) | write, wait for the ack, then poll every target in shuffled sweeps. All v1 runs are in `void/` |
| 2 | independent writer and reader threads. Non-causal targets read through a raw `find` sent with `db.command`; causal targets through `find_one` after the fix in 0dc45cb |
| 3 | every read through `find_one`; command listener with abort on missing `afterClusterTime`; reply `operationTime`; slot skipping for reads and writes; `expect` check; topology and power provenance |

v2 and v3 read latencies come from different driver paths and are not directly
comparable.
