# Engine Benchmark Results

Analyzed from raw benchmark runs in `bench/results/`.

## Workload Composition

- **insert**: 98% limit / 2% market, no cancels
- **cancel**: 50% limit / 50% cancel, valid handles only
- **cross**: 45% limit / 55% market
- **mixed**: 70% limit / 20% market / 10% cancel; limit orders further
  split 65% GTC / 34% IOC / 1% FOK; 50 synthetic participants, so
  self-trade prevention is exercised

## Environment

- Hardware: Intel Xeon E3-1270 v6 @ 3.8GHz (4 cores / 8 threads), single NUMA
  node
- Governor: performance (all cores)
- Pinning: producer=core 0, matcher=core 1, logger=core 2; all are
  non-SMT-sibling cores on the same NUMA node
- Validity: `inbound_full=0` and `outbound_full=0` across every run in this
  document, so no ring backpressure occurred at any tested rate

## cancel

Requested send rate: 100,000 orders/s.

Runs: 8

Latency samples: 1,000,000 completed measured requests per run; 100,000 warmup commands excluded.

| Metric | Runs | Median | Min | Max |
|---|---:|---:|---:|---:|
| response_latency_ns.p50 | 8 | 346.5 | 337 | 349 |
| response_latency_ns.p99 | 8 | 419 | 417 | 425 |
| response_latency_ns.p99.9 | 8 | 6345.5 | 5085 | 7496 |

## cancel_1m

Requested send rate: 1,000,000 orders/s.

Runs: 8

Latency samples: 1,000,000 completed measured requests per run; 100,000 warmup commands excluded.

| Metric | Runs | Median | Min | Max |
|---|---:|---:|---:|---:|
| response_latency_ns.p50 | 8 | 307 | 305 | 311 |
| response_latency_ns.p99 | 8 | 617.5 | 570 | 849 |
| response_latency_ns.p99.9 | 8 | 3699.5 | 2310 | 11301 |

## cross

Requested send rate: 100,000 orders/s.

Runs: 8

Latency samples: 1,000,000 completed measured requests per run; 100,000 warmup commands excluded.

| Metric | Runs | Median | Min | Max |
|---|---:|---:|---:|---:|
| response_latency_ns.p50 | 8 | 368 | 366 | 371 |
| response_latency_ns.p99 | 8 | 790.5 | 787 | 2147 |
| response_latency_ns.p99.9 | 8 | 6216 | 5369 | 7546 |

## cross_1m

Requested send rate: 1,000,000 orders/s.

Runs: 8

Latency samples: 1,000,000 completed measured requests per run; 100,000 warmup commands excluded.

| Metric | Runs | Median | Min | Max |
|---|---:|---:|---:|---:|
| response_latency_ns.p50 | 8 | 344 | 342 | 349 |
| response_latency_ns.p99 | 8 | 1009 | 984 | 1065 |
| response_latency_ns.p99.9 | 8 | 8205.5 | 2341 | 11821 |

## insert

Requested send rate: 100,000 orders/s.

Runs: 8

Latency samples: 1,000,000 completed measured requests per run; 100,000 warmup commands excluded.

| Metric | Runs | Median | Min | Max |
|---|---:|---:|---:|---:|
| response_latency_ns.p50 | 8 | 344 | 343 | 348 |
| response_latency_ns.p99 | 8 | 654 | 634 | 1560 |
| response_latency_ns.p99.9 | 8 | 147939 | 140626 | 160163 |

## insert_1m

Requested send rate: 1,000,000 orders/s.

Runs: 8

Latency samples: 1,000,000 completed measured requests per run; 100,000 warmup commands excluded.

| Metric | Runs | Median | Min | Max |
|---|---:|---:|---:|---:|
| response_latency_ns.p50 | 8 | 312 | 310 | 316 |
| response_latency_ns.p99 | 8 | 142379 | 136858 | 158399 |
| response_latency_ns.p99.9 | 8 | 289518 | 281919 | 309370 |

## mixed

Requested send rate: 100,000 orders/s.

Runs: 8

Latency samples: 1,000,000 completed measured requests per run; 100,000 warmup commands excluded.

| Metric | Runs | Median | Min | Max |
|---|---:|---:|---:|---:|
| response_latency_ns.p50 | 8 | 359.5 | 358 | 363 |
| response_latency_ns.p99 | 8 | 944 | 936 | 2133 |
| response_latency_ns.p99.9 | 8 | 5302 | 4692 | 7872 |

## mixed_1m

Requested send rate: 1,000,000 orders/s.

Runs: 8

Latency samples: 1,000,000 completed measured requests per run; 100,000 warmup commands excluded.

| Metric | Runs | Median | Min | Max |
|---|---:|---:|---:|---:|
| response_latency_ns.p50 | 8 | 342 | 341 | 348 |
| response_latency_ns.p99 | 8 | 997.5 | 975 | 1028 |
| response_latency_ns.p99.9 | 8 | 9859.5 | 2414 | 12102 |

## mixed_2m

Requested send rate: 2,000,000 orders/s.

Runs: 8

Latency samples: 1,000,000 completed measured requests per run; 100,000 warmup commands excluded.

| Metric | Runs | Median | Min | Max |
|---|---:|---:|---:|---:|
| response_latency_ns.p50 | 8 | 322 | 319 | 328 |
| response_latency_ns.p99 | 8 | 1442 | 1349 | 1885 |
| response_latency_ns.p99.9 | 8 | 15109 | 3565 | 24378 |

## Saturation throughput

`mixed`'s closed-loop saturation throughput was measured at **8,082,237 orders/s**.

Measured via `engine_bench --closed-loop`: the producer sends commands as fast as the
inbound ring accepts them, with per-operation latency sampling disabled — closed-loop
numbers reflect maximum sustained throughput only and are not valid latency measurements.
Throughput is computed as measured command count divided by elapsed wall-clock time,
after a 100,000-command warmup phase.

This figure is used as the denominator when expressing open-loop send rates as a
percentage of capacity (e.g. 1,000,000/s ≈ 12.4%, 2,000,000/s ≈ 24.7% of saturation).

## Known Anomaly: Insert-Heavy Tail

`insert` and `insert_1m` show a p99/p99.9 tail, up to about 290µs, far larger
than any other workload at the same rate, and the effect grows with rate. At
100,000 orders/s it only appears at p99.9; at 1,000,000 orders/s it has moved up
to p99. No other workload shows this pattern. Hypothesis: insert-heavy traffic
rarely cancels, so pool occupancy grows monotonically over the run rather than
reaching steady-state churn the way cancel, cross, and mixed do. This has not
yet been directly confirmed via pool-occupancy inspection.
