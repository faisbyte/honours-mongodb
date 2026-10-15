# Void runs

Run folders and merged outputs moved out of `measurements/` because they must
not be analysed. They are kept, unmodified, as a record. Nothing in `harness/`
reads from this folder: the analysis scripts take their run list from
`runs/VALID.csv`, which does not list any of these.

## Causal reads before the fix

The collector read every target, causal ones included, through a hand-built
`find` sent with `Database.command`. In PyMongo 4.18 `Database.command` never
passes a read concern to the command runner, so the session's
`afterClusterTime` was never added to the command and the server had nothing
to wait for. These causal targets measured an ordinary secondary read that
happened to carry a session. The fix (reading through `Collection.find_one`)
was committed in 0dc45cb; the re-runs that replace these are
`20260917T111403Z`, `111706Z`, `115025Z` and `115328Z`.

| Folder | Reason |
|---|---|
| `20260917T052319Z__threaded-wmaj__rs-secondary-causal-majority` | Pre-fix causal read. Shows no read-your-writes violations only because a w:"majority" ack already implies the majority commit point; the session played no part. |
| `20260917T052622Z__threaded-wmaj__rs-secondary-causal-local` | Pre-fix causal read, as above. |
| `20260917T055552Z__threaded-w1__rs-secondary-causal-majority` | Pre-fix causal read. 38,140 non-warmup reads returned a seq older than the one the session had been advanced to. |
| `20260917T055856Z__threaded-w1__rs-secondary-causal-local` | Pre-fix causal read. 358 non-warmup reads returned a seq older than the one the session had been advanced to. |

## Concurrent-mode runs

| Folder | Reason |
|---|---|
| `20260917T031746Z__threaded-w1` | All nine targets as threads in one process, which inflates read latency by roughly an order of magnitude. Causal targets are pre-fix. The manifest has no `mode`, so `analyse.py` cannot load it. |
| `20260917T032327Z__threaded-w1` | Same as above, with a 20 s duration and a 5 ms read interval (a smoke-sized run). |

## Smoke runs that produced no usable data

| Folder | Reason |
|---|---|
| `20260917T031437Z__smoke` | Aborted before any data was written: only `config.yaml` and the "before" provenance exist, no manifest. |
| `20260917T031516Z__smoke` | 0 writes recorded. The config mixed v1 keys (`cycles`, `poll_window_ms`) with v2 keys and had no `write_interval_ms`, so the writer thread died at start; this predates `writer_fatal` being recorded in the manifest. |

## v1 harness runs (write-then-poll)

Collected with the v1 collector, which issued a write, waited for the
acknowledgement and only then polled every target in shuffled sweeps. Reads
during the write window were never taken, the number of sweeps per cycle
depended on read latency, and causal sessions were never advanced with the
writer's timestamps. The manifests have no `mode`, and the current
`analyse.py` cannot load them.

| Folder | Reason |
|---|---|
| `20260908T063432Z__smoke` | v1 smoke run. |
| `20260908T063813Z__smoke` | v1 smoke run. |
| `20260908T065544Z__baseline` | v1 run, w:"majority". |
| `20260908T071248Z__baseline-w1` | v1 run, w:1. |
| `20260910T035527Z__baseline-w1` | v1 run, w:1. |

## Merged outputs built on void runs

| Folder | Reason |
|---|---|
| `merged/20260917T062004Z__threaded-w1` | Includes the pre-fix causal runs `055552Z` and `055856Z`. Superseded by `measurements/merged/20260917T112457Z__threaded-w1`. |
| `merged/20260917T063958Z__threaded-w1` | Same source runs as `062004Z`. |
| `merged/20260917T093715Z__threaded-wmaj` | Includes the pre-fix causal runs `052319Z` and `052622Z`. Superseded by `measurements/merged/20260917T115925Z__threaded-wmaj`. |

## Not void, but note

The two superseding merges (`112457Z`, `115925Z`) combine seven paths
collected while 27018 and 27019 synced from 27020 (chained) with two causal
paths collected later on a star topology. `runs/VALID.csv` records the
topology of every run so this can be split.
