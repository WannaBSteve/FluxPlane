# Query and feature-update scheduling

## Why scheduling belongs to the method

A prediction can be semantically correct and still be wrong in use if its features are stale. FluxECR therefore treats query latency, update freshness and throughput as a joint decision rather than optimizing one after the others.

Fixed pools reserve one worker for queries and one for feature refreshes. Dynamic borrowing lets queries use an idle refresh worker, but only while the refresh deadline remains feasible. Candidate policies are scored with constrained weighted Nash social welfare:

```text
maximize  2 log(query utility) + 2 log(freshness utility) + log(throughput utility)
subject to feature-deadline rate >= 0.99
```

The logarithms penalize a policy that makes one objective excellent by collapsing another. The hard constraint prevents a low-latency policy from winning by starving updates.

## Isolated Compose result

The workload uses 100,000 rows, two workers, 20 queries and 6 delayed feature refreshes per run. Fixed pools and dynamic borrowing are each repeated three times with the same CPU reservation and result digest.

| Policy | Query mean | Query p95 | Throughput | Refreshes within 2 s |
| --- | ---: | ---: | ---: | ---: |
| Fixed pools | 7,003.1 ms | 12,386.7 ms | 1.950 jobs/s | 18/18 |
| Dynamic borrowing | **4,931.7 ms** | **7,414.9 ms** | **3.199 jobs/s** | **18/18** |
| Relative change | **−29.6%** | **−40.1%** | **+64.1%** | unchanged |

Across both policies, 36/36 refreshes meet the two-second deadline. Throughput per reserved CPU rises by the same 64.1% because both runs reserve 1,000 millicores.

## Small local replay

A ten-round 10,000-row replay confirms the latency mechanism: fixed pools give 65.575 ms mean and 96.945 ms p95; immediate borrowing gives 43.835 ms mean and 61.840 ms p95, reductions of 33.2% and 36.2%. All refreshes meet the deadline. Its throughput is nearly unchanged, so the resource-efficiency claim comes from the isolated Compose experiment above.

Raw measurements:

- [Isolated Compose workload](../../docs/benchmarks/mixed-compose-100k-20261109.json)
- [Ten-round local replay and Nash scores](../../docs/benchmarks/scheduler-tuning-mean-local-20261122.json)
