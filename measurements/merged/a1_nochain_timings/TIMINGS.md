# Event timings

Generated 2026-10-01 04:54 UTC from 4 runs, 7021 write-by-path observations.

## Write acknowledgement

Time from sending a write to receiving its acknowledgement (ms).

| condition | n | 0.01 | 0.1 | 0.5 | 0.9 | 0.99 | 0.999 |
|---|---|---|---|---|---|---|---|
| w:1, j:None | 7025 | 0.178 | 0.204 | 0.537 | 0.614 | 0.733 | 1.038 |

## When a write becomes visible, per read path

For each write, the new value became visible on a path somewhere between the last read that still saw the old value and the first read that saw the new one. Figures below are the upper bound of that interval, which is the conservative choice. `from_ack` values can be negative: that means the value was already visible before the writer received its acknowledgement.

`fresh_at_first_read` is the share of writes where even the first read after the write already saw the new value. For those, the true visibility time is earlier than anything this harness can observe, so their bounds say only 'no later than the first read'.

`resolution_p50_ms` is the typical width of the visibility interval, the harness's time resolution for that path.

| condition | target | writes | fresh_at_first_read | censored | vis_from_start_p50_ms | vis_from_start_p90_ms | vis_from_start_p99_ms | vis_from_ack_p50_ms | vis_from_ack_p99_ms | resolution_p50_ms | regressions_per_write |
|---|---|---|---|---|---|---|---|---|---|---|---|
| w:1, j:None | direct-27017 | 1755 | 0.952 | 0.0 | 0.622 | 0.712 | 0.854 | 0.082 | 0.351 | 0.682 | 0.0 |
| w:1, j:None | direct-27018 | 1756 | 0.52 | 0.0 | 0.695 | 1.105 | 1.207 | 0.149 | 0.671 | 0.598 | 0.0 |
| w:1, j:None | direct-27019 | 1755 | 0.524 | 0.0 | 0.667 | 1.092 | 1.187 | 0.124 | 0.635 | 0.637 | 0.0 |
| w:1, j:None | direct-27020 | 1755 | 0.481 | 0.0 | 0.661 | 1.106 | 1.242 | 0.178 | 0.702 | 0.499 | 0.0 |

## Read round-trip time

| condition | target | p50_ms | p90_ms | p99_ms |
|---|---|---|---|---|
| w:1, j:None | direct-27017 | 0.116 | 0.128 | 0.201 |
| w:1, j:None | direct-27018 | 0.116 | 0.128 | 0.2 |
| w:1, j:None | direct-27019 | 0.117 | 0.128 | 0.192 |
| w:1, j:None | direct-27020 | 0.118 | 0.132 | 0.212 |

## Using these as simulation inputs

These roughly correspond to the four latency distributions in the WARS model of Bailis et al. (PBS, VLDB 2012):

- write ack latency: the `ack_latency` table, per write concern
- propagation to each replica: visibility on the `direct-*` paths under `w:1`, measured from write start
- read request and response: the read round-trip table
- majority commit point delay (MongoDB-specific, no WARS equivalent): visibility on `rs-secondary-majority` under `w:1`

`visibility_per_write.csv` holds one row per write per path, so a simulation can sample the empirical distributions directly instead of fitting a parametric model.

## Limits

- In isolated mode each read path is measured in its own run, so these files do not give the joint distribution of visibility across replicas for the same write. A simulation that assumes replicas are independent can use them; one that needs correlation between replicas cannot.
- All replicas share one machine and loopback networking. Timings reflect scheduling, oplog apply and disk, not network distance.
- Visibility is bounded, not measured exactly. See resolution.
