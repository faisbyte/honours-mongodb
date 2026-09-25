# Event timings

Generated 2026-09-24 23:42 UTC from 18 runs, 23700 write-by-path observations.

## Write acknowledgement

Time from sending a write to receiving its acknowledgement (ms).

| condition | n | 0.01 | 0.1 | 0.5 | 0.9 | 0.99 | 0.999 |
|---|---|---|---|---|---|---|---|
| w:1, j:None | 15807 | 0.192 | 0.229 | 0.539 | 0.624 | 0.897 | 1.412 |
| w:majority, j:None | 7911 | 5.07 | 9.017 | 9.058 | 13.533 | 15.544 | 28.558 |

## When a write becomes visible, per read path

For each write, the new value became visible on a path somewhere between the last read that still saw the old value and the first read that saw the new one. Figures below are the upper bound of that interval, which is the conservative choice. `from_ack` values can be negative: that means the value was already visible before the writer received its acknowledgement.

`fresh_at_first_read` is the share of writes where even the first read after the write already saw the new value. For those, the true visibility time is earlier than anything this harness can observe, so their bounds say only 'no later than the first read'.

`resolution_p50_ms` is the typical width of the visibility interval, the harness's time resolution for that path.

| condition | target | writes | fresh_at_first_read | censored | vis_from_start_p50_ms | vis_from_start_p90_ms | vis_from_start_p99_ms | vis_from_ack_p50_ms | vis_from_ack_p99_ms | resolution_p50_ms | regressions_per_write |
|---|---|---|---|---|---|---|---|---|---|---|---|
| w:1, j:None | direct-27017 | 1756 | 0.86 | 0.0 | 0.628 | 0.723 | 0.882 | 0.086 | 0.27 | 0.33 | 0.0 |
| w:1, j:None | direct-27018 | 1755 | 0.056 | 0.0 | 1.117 | 1.234 | 1.65 | 0.574 | 1.114 | 0.648 | 0.0 |
| w:1, j:None | direct-27019 | 1756 | 0.028 | 0.0 | 1.137 | 1.438 | 1.711 | 0.592 | 1.156 | 0.665 | 0.0 |
| w:1, j:None | direct-27020 | 1755 | 0.557 | 0.0 | 0.681 | 1.132 | 1.266 | 0.162 | 0.681 | 0.567 | 0.0 |
| w:1, j:None | rs-primary-local | 1755 | 0.836 | 0.0 | 0.627 | 0.741 | 0.964 | 0.085 | 0.299 | 0.322 | 0.0 |
| w:1, j:None | rs-secondary-causal-local | 1755 | 0.505 | 0.0 | 0.69 | 1.119 | 1.504 | 0.167 | 0.69 | 0.674 | 0.0 |
| w:1, j:None | rs-secondary-causal-majority | 1755 | 0.003 | 0.0 | 8.917 | 12.826 | 14.536 | 8.488 | 13.987 | 8.622 | 0.0 |
| w:1, j:None | rs-secondary-local | 1755 | 0.199 | 0.0 | 1.118 | 1.266 | 1.653 | 0.574 | 1.126 | 0.682 | 0.0011 |
| w:1, j:None | rs-secondary-majority | 1756 | 0.0 | 0.0006 | 12.601 | 13.755 | 15.613 | 12.053 | 15.056 | 0.642 | 0.0 |
| w:majority, j:None | direct-27017 | 878 | 0.946 | 0.0 | 0.634 | 0.729 | 0.826 | -8.42 | -7.861 | 0.29 | 0.0 |
| w:majority, j:None | direct-27018 | 878 | 0.018 | 0.0 | 1.133 | 1.602 | 1.76 | -8.837 | -7.425 | 0.663 | 0.0 |
| w:majority, j:None | direct-27019 | 878 | 0.034 | 0.0 | 1.125 | 1.33 | 1.714 | -8.448 | -7.435 | 0.655 | 0.0 |
| w:majority, j:None | direct-27020 | 878 | 0.666 | 0.0 | 0.686 | 1.157 | 1.253 | -8.416 | -7.837 | 0.677 | 0.0 |
| w:majority, j:None | rs-primary-local | 878 | 0.934 | 0.0 | 0.631 | 0.718 | 0.839 | -8.455 | -8.238 | 0.388 | 0.0 |
| w:majority, j:None | rs-secondary-causal-local | 878 | 0.524 | 0.0 | 0.737 | 1.167 | 1.263 | -8.001 | -3.879 | 0.685 | 0.0 |
| w:majority, j:None | rs-secondary-causal-majority | 878 | 0.0 | 0.0 | 9.119 | 13.09 | 13.605 | 0.079 | 0.573 | 0.661 | 0.0 |
| w:majority, j:None | rs-secondary-local | 878 | 0.267 | 0.0 | 1.115 | 1.2 | 1.67 | -8.37 | -7.489 | 0.67 | 0.0 |
| w:majority, j:None | rs-secondary-majority | 878 | 0.0 | 0.0 | 9.581 | 14.612 | 16.091 | 0.077 | 0.576 | 0.653 | 0.0 |

## Read round-trip time

| condition | target | p50_ms | p90_ms | p99_ms |
|---|---|---|---|---|
| w:1, j:None | direct-27017 | 0.12 | 0.154 | 0.253 |
| w:1, j:None | direct-27018 | 0.118 | 0.138 | 0.227 |
| w:1, j:None | direct-27019 | 0.119 | 0.154 | 0.245 |
| w:1, j:None | direct-27020 | 0.119 | 0.135 | 0.225 |
| w:1, j:None | rs-primary-local | 0.12 | 0.151 | 0.246 |
| w:1, j:None | rs-secondary-causal-local | 0.139 | 0.153 | 0.213 |
| w:1, j:None | rs-secondary-causal-majority | 0.138 | 0.147 | 0.255 |
| w:1, j:None | rs-secondary-local | 0.134 | 0.163 | 0.257 |
| w:1, j:None | rs-secondary-majority | 0.133 | 0.16 | 0.26 |
| w:majority, j:None | direct-27017 | 0.116 | 0.128 | 0.178 |
| w:majority, j:None | direct-27018 | 0.117 | 0.143 | 0.227 |
| w:majority, j:None | direct-27019 | 0.118 | 0.135 | 0.209 |
| w:majority, j:None | direct-27020 | 0.118 | 0.134 | 0.209 |
| w:majority, j:None | rs-primary-local | 0.118 | 0.135 | 0.202 |
| w:majority, j:None | rs-secondary-causal-local | 0.139 | 0.153 | 0.216 |
| w:majority, j:None | rs-secondary-causal-majority | 0.138 | 0.148 | 0.191 |
| w:majority, j:None | rs-secondary-local | 0.132 | 0.147 | 0.219 |
| w:majority, j:None | rs-secondary-majority | 0.132 | 0.145 | 0.211 |

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
