# Ring Benchmark Record

Standalone benchmark for the ring buffer used to pass messages between two
threads. This is a component-level check, not the final end-to-end performance
claim for the engine.

| | |
|---|---|
| Date | 2026-07-16 |
| Commit | `f4eae8f4adbb19579f97dbf5c4450bf815127ff5` |
| Build | preset `release`, `-O3 -march=native -DNDEBUG` |
| Environment | local WSL2 run |
| Run type | single run |

## Ring Buffer Comparison

One producer thread pushes sequence-numbered payloads. One consumer thread pops
them and verifies exact FIFO order. The benchmark compares the production padded
SPSC ring against an unpadded version and two mutex-protected baselines.

```bash
cmake --preset release
cmake --build build/release --target spsc_ring_bench
build/release/bench/spsc_ring_bench
```

| Variant | Messages | Seconds | Messages/s |
|---|---:|---:|---:|
| Padded SPSC ring | 100,000,000 | 0.254392 | 393,094,337 |
| Unpadded SPSC ring | 100,000,000 | 1.387946 | 72,048,913 |
| Mutex preallocated ring | 100,000,000 | 15.014110 | 6,660,401 |
| Mutex deque | 100,000,000 | 15.593874 | 6,412,775 |

Result: all variants preserved sequence order. The numbers are a local
wall-clock comparison showing the rough cost of removing cache-line padding and
replacing the single-producer/single-consumer ring with mutex-protected queues.
They are not intended as final publishable throughput numbers.

Takeaway: in this local run, the padded SPSC ring was about 5x faster than the
unpadded version and about 60x faster than the mutex-protected baselines. The
exact ratios are environment-dependent, but the direction is the useful result:
cache-line padding and the lock-free SPSC design both mattered.
