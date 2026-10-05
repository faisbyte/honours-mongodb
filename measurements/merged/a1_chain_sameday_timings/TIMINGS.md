# Event timings

Generated 2026-10-01 05:13 UTC from 4 runs, 7022 write-by-path observations.

## Write acknowledgement

Time from sending a write to receiving its acknowledgement (ms).

| condition | n | 0.01 | 0.1 | 0.5 | 0.9 | 0.99 | 0.999 |
|---|---|---|---|---|---|---|---|
| w:1, j:None | 7026 | 0.193 | 0.297 | 0.549 | 0.673 | 1.02 | 2.302 |

## When a write becomes visible, per read path

For each write, the new value became visible on a path somewhere between the last read that still saw the old value and the first read that saw the new one. Figures below are the upper bound of that interval, which is the conservative choice. `from_ack` values can be negative: that means the value was already visible before the writer received its acknowledgement.

`fresh_at_first_read` is the share of writes where even the first read after the write already saw the new value. For those, the true visibility time is earlier than anything this harness can observe, so their bounds say only 'no later than the first read'.

`resolution_p50_ms` is the typical width of the visibility interval, the harness's time resolution for that path.

| condition | target | writes | fresh_at_first_read | censored | vis_from_start_p50_ms | vis_from_start_p90_ms | vis_from_start_p99_ms | vis_from_ack_p50_ms | vis_from_ack_p99_ms | resolution_p50_ms | regressions_per_write |
|---|---|---|---|---|---|---|---|---|---|---|---|
| w:1, j:None | direct-27017 | 1755 | 0.768 | 0.0 | 0.645 | 0.777 | 1.032 | 0.097 | 0.391 | 0.372 | 0.0 |
| w:1, j:None | direct-27018 | 1756 | 0.296 | 0.0 | 0.853 | 1.191 | 1.746 | 0.521 | 1.04 | 0.661 | 0.0 |
| w:1, j:None | direct-27019 | 1756 | 0.138 | 0.0 | 1.125 | 1.255 | 2.736 | 0.554 | 1.699 | 0.669 | 0.0 |
| w:1, j:None | direct-27020 | 1755 | 0.205 | 0.0 | 1.107 | 1.184 | 1.512 | 0.547 | 0.874 | 0.678 | 0.0 |

## Read round-trip time

| condition | target | p50_ms | p90_ms | p99_ms |
|---|---|---|---|---|
| w:1, j:None | direct-27017 | 0.123 | 0.167 | 0.318 |
| w:1, j:None | direct-27018 | 0.122 | 0.162 | 0.301 |
| w:1, j:None | direct-27019 | 0.122 | 0.162 | 0.453 |
| w:1, j:None | direct-27020 | 0.12 | 0.153 | 0.395 |

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
