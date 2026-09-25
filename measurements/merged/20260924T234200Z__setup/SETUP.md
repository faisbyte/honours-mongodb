# Hardware and execution setup

Generated 2026-09-24 23:42 UTC from 18 run directories.

## Topology

All four `mongod` processes and the measurement client run on one physical machine and talk over loopback (127.0.0.1). There is no real network between replicas, so replication delay here reflects process scheduling, oplog fetching and applying, and disk, not network distance.

The client is a single Python process. Writer and reader run on separate threads inside it, so every timestamp comes from one monotonic clock (`time.perf_counter_ns`) and write and read times are directly comparable with no clock skew.

## Host hardware

| Property | Value |
|---|---|
| Hostname | MacBook-Pro-27.local |
| CPU | Apple M2 Pro |
| Logical cores | 12 |
| Memory | 32 GB |
| Architecture | arm64 |
| OS | macOS-27.0-arm64-arm-64bit |
| Storage | APPLE SSD AP1024Z, SSD, Disk Image |
| Power source at run time | Now drawing from 'Battery Power' |

## Software

| Component | Version |
|---|---|
| MongoDB | 8.3.7 |
| Feature compatibility version | 8.3 |
| Python | 3.12.7 |
| PyMongo | 4.18.0 |

## MongoDB nodes

| Host | Bind IP | dbPath | Storage engine | WT cache (GB) |
|---|---|---|---|---|
| 127.0.0.1:27017 | 127.0.0.1 | /Users/faisalnaveed/mongo-rs/n0 | wiredTiger (default) | default |
| 127.0.0.1:27018 | 127.0.0.1 | /Users/faisalnaveed/mongo-rs/n1 | wiredTiger (default) | default |
| 127.0.0.1:27019 | 127.0.0.1 | /Users/faisalnaveed/mongo-rs/n2 | wiredTiger (default) | default |
| 127.0.0.1:27020 | 127.0.0.1 | /Users/faisalnaveed/mongo-rs/n3 | wiredTiger (default) | default |

WiredTiger cache configured on the primary: 15.50 GB. No explicit cache limit is set, so each of the four processes sizes its cache as if it owned the machine.

## Replica set configuration

| Member | Priority | Votes | Hidden | secondaryDelaySecs |
|---|---|---|---|---|
| 127.0.0.1:27017 | 10.0 | 1 | False | 0 |
| 127.0.0.1:27018 | 1.0 | 1 | False | 0 |
| 127.0.0.1:27019 | 1.0 | 1 | False | 0 |
| 127.0.0.1:27020 | 1.0 | 1 | False | 0 |

| Setting | Value |
|---|---|
| Replica set name | rs0 |
| Members | 4 |
| Majority (of voting members) | 3 |
| writeConcernMajorityJournalDefault | True |
| heartbeatIntervalMillis | 2000 |
| electionTimeoutMillis | 10000 |
| chainingAllowed | True |

Note: `writeConcernMajorityJournalDefault` only applies when a write concern does not set `j` explicitly.

## Executions

One row per run directory. In isolated mode each run measures one read path with its own writer, so read paths are never sampled at the same instant.

| Run | Experiment | Mode | Target | w | j | Duration (s) | Write every (ms) | Read every (µs) | Primary | Writes | Reads |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 20260917T050155Z | threaded-wmaj | isolated | direct-27017 | majority |  | 180 | 200 | 500 | 127.0.0.1:27017 | 904 | 361000 |
| 20260917T050458Z | threaded-wmaj | isolated | direct-27018 | majority |  | 180 | 200 | 500 | 127.0.0.1:27017 | 904 | 361087 |
| 20260917T050802Z | threaded-wmaj | isolated | direct-27019 | majority |  | 180 | 200 | 500 | 127.0.0.1:27017 | 904 | 361063 |
| 20260917T051105Z | threaded-wmaj | isolated | direct-27020 | majority |  | 180 | 200 | 500 | 127.0.0.1:27017 | 904 | 361083 |
| 20260917T051409Z | threaded-wmaj | isolated | rs-primary-local | majority |  | 180 | 200 | 500 | 127.0.0.1:27017 | 904 | 361058 |
| 20260917T051712Z | threaded-wmaj | isolated | rs-secondary-local | majority |  | 180 | 200 | 500 | 127.0.0.1:27017 | 904 | 360953 |
| 20260917T052015Z | threaded-wmaj | isolated | rs-secondary-majority | majority |  | 180 | 200 | 500 | 127.0.0.1:27017 | 904 | 360998 |
| 20260917T053429Z | threaded-w1 | isolated | direct-27017 | 1 |  | 180 | 100 | 500 | 127.0.0.1:27017 | 1807 | 361027 |
| 20260917T053733Z | threaded-w1 | isolated | direct-27018 | 1 |  | 180 | 100 | 500 | 127.0.0.1:27017 | 1806 | 360971 |
| 20260917T054036Z | threaded-w1 | isolated | direct-27019 | 1 |  | 180 | 100 | 500 | 127.0.0.1:27017 | 1807 | 361019 |
| 20260917T054339Z | threaded-w1 | isolated | direct-27020 | 1 |  | 180 | 100 | 500 | 127.0.0.1:27017 | 1806 | 360995 |
| 20260917T054642Z | threaded-w1 | isolated | rs-primary-local | 1 |  | 180 | 100 | 500 | 127.0.0.1:27017 | 1806 | 360998 |
| 20260917T054946Z | threaded-w1 | isolated | rs-secondary-local | 1 |  | 180 | 100 | 500 | 127.0.0.1:27017 | 1806 | 360998 |
| 20260917T055249Z | threaded-w1 | isolated | rs-secondary-majority | 1 |  | 180 | 100 | 500 | 127.0.0.1:27017 | 1807 | 361020 |
| 20260917T111403Z | threaded-w1 | isolated | rs-secondary-causal-local | 1 |  | 180 | 100 | 500 | 127.0.0.1:27017 | 1806 | 360836 |
| 20260917T111706Z | threaded-w1 | isolated | rs-secondary-causal-majority | 1 |  | 180 | 100 | 500 | 127.0.0.1:27017 | 1806 | 360915 |
| 20260917T115025Z | threaded-wmaj | isolated | rs-secondary-causal-local | majority |  | 180 | 200 | 500 | 127.0.0.1:27017 | 904 | 360915 |
| 20260917T115328Z | threaded-wmaj | isolated | rs-secondary-causal-majority | majority |  | 180 | 200 | 500 | 127.0.0.1:27017 | 904 | 360940 |

## Measurement resolution

Round-trip time of a read that cannot be stale, measured before each run. Effects smaller than this are below what the harness can resolve.

| Run | Target | p50 (µs) | p99 (µs) |
|---|---|---|---|
| 20260917T050155Z | direct-27017 | 104 | 194 |
| 20260917T050458Z | direct-27018 | 100 | 191 |
| 20260917T050802Z | direct-27019 | 112 | 158 |
| 20260917T051105Z | direct-27020 | 106 | 236 |
| 20260917T051409Z | rs-primary-local | 102 | 194 |
| 20260917T051712Z | rs-secondary-local | 115 | 263 |
| 20260917T052015Z | rs-secondary-majority | 109 | 188 |
| 20260917T053429Z | direct-27017 | 104 | 258 |
| 20260917T053733Z | direct-27018 | 107 | 152 |
| 20260917T054036Z | direct-27019 | 127 | 335 |
| 20260917T054339Z | direct-27020 | 108 | 192 |
| 20260917T054642Z | rs-primary-local | 95 | 169 |
| 20260917T054946Z | rs-secondary-local | 111 | 189 |
| 20260917T055249Z | rs-secondary-majority | 115 | 252 |
| 20260917T111403Z | rs-secondary-causal-local | 114 | 278 |
| 20260917T111706Z | rs-secondary-causal-majority | 120 | 278 |
| 20260917T115025Z | rs-secondary-causal-local | 114 | 223 |
| 20260917T115328Z | rs-secondary-causal-majority | 123 | 285 |

## Consistency checks across runs

- MongoDB version: 8.3.7 (consistent)
- Hostname: MacBook-Pro-27.local (consistent)
- Replica set members: 4 (consistent)
- Primary: 127.0.0.1:27017 (consistent)
