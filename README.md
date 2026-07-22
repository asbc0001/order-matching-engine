# Order Book & Matching Engine

A low-latency, single-instrument order matching engine written in C++20 for Linux.

A matching engine is the part of an exchange that takes in buy and sell orders,
matches them against each other by price and time priority, and reports the
resulting trades. Orders that do not trade right away rest in an order book,
arranged by price, until a later order matches them or they are cancelled.

The project is built around a few key pieces: a lock-free handoff between
threads, a matching path that takes no locks and allocates no memory, and a
benchmark setup that tries to measure latency honestly. The matcher is checked
against a separate, simpler reference implementation. A TCP gateway and a
market-data view sit on top as later add-ons, outside that core.

## What it does

- Handles limit, market, and cancel orders, plus immediate-or-cancel (IOC),
  fill-or-kill (FOK), and self-trade prevention.
- Matches on price-time priority: the best price trades first, and orders at the
  same price trade oldest first.
- Replays deterministically, so the same input always produces the same trades
  and the same final book, which keeps the engine easy to test and its benchmarks
  repeatable.
- Reaches a median order-to-response latency of roughly 310 to 370 nanoseconds
  across the measured workloads, with 99th-percentile latency near a microsecond
  in the main benchmark runs (methodology and results below).

## How it's built

The engine runs as three threads joined by two lock-free queues, pinned to
separate cores in benchmark and `--cores` runs:

```
  producer thread
    reads a trace file or TCP client, submits orders
        |
        |  inbound ring (SPSC, lock-free)
        v
  matching thread
    owns the book; matches, rests, or cancels
        |
        |  outbound ring (SPSC, lock-free)
        v
  logger thread
    records results: in-memory counters, event log, or sockets
```

Everything follows from a single rule: only the matching thread ever touches the
book. It alone owns the order pool and the price-level state, so there are no
locks anywhere in the matching logic. There is nothing to lock against, because
no other thread can reach that data in the first place. The only things that
cross between threads are two ring buffers that carry messages in and out. Each
is a single-producer, single-consumer (SPSC) ring: exactly one thread writes it
and one thread reads it, and that restriction is what lets the handoff stay
lock-free using only lightweight atomic loads and stores, with no mutexes and no
compare-and-swap.

Because of that rule, the matching path takes no locks, makes no system calls,
and allocates no memory. Everything it uses (the order pool, the price levels,
the ring buffers) is a fixed size, reserved once when the engine starts.

### Key design decisions

These are the choices that make the engine fast:

**An array-and-bitmap book instead of a sorted map.** Price levels are stored in
a flat array indexed directly by price, and a bitmap marks which levels are
occupied. Finding the best price is a hardware bit-scan over the bitmap rather
than a walk through a tree of pointers scattered across memory. This trades a
larger fixed memory footprint for far fewer cache misses on every operation.

**A generational-handle object pool instead of a hash map.** Every resting order
lives in a fixed-size pool. Its handle packs a slot index together with a
"generation" number that increases each time the slot is reused. Cancelling an
order is a single array index plus one comparison, with no hashing. The same
check also rejects stale handles (cancelling an already-filled order),
double-cancels, and forged handles from untrusted input, all through one
mechanism.

**Cache-line-padded SPSC ring buffers.** The two ring buffers place their
producer and consumer indices on separate 64-byte cache lines so the two threads
never fight over the same line. This "false sharing" is the single biggest
performance trap in naive ring buffers; the component benchmark below shows it
costs roughly a 5x slowdown when removed.

**An explicit codec at every file and socket boundary.** In-memory structs are
never written to disk or the network directly, because raw struct bytes would
leak padding and internal timing fields and tie the file format to one compiler.
A small codec serializes named fields at fixed offsets and validates everything
on the way back in. The network wire format reuses this codec directly.

**Packing the book for dense scans.** The book data that only the matching thread
touches, the price levels and the occupancy bitmap, is kept small and dense, so a
scan across nearby prices pulls in as few cache lines as possible. The `Order`
struct is the deliberate exception: even though only one thread touches it, each
order is padded to a full cache line. Orders are reached by a pointer jump from a
handle rather than scanned in sequence, so giving each one its own line means
resolving a handle is a single memory fetch.

## Building

Requires a C++20 compiler (tested on GCC 13 and Clang 18) and CMake 3.20+.

Targets Linux on x86-64. The code assumes a 64-byte cache line, uses GCC/Clang
bit-scan builtins, and the release preset builds with `-march=native`, so it is
not portable to Apple Silicon or other ARM machines as written. The benchmark
numbers below come from the specific machine noted in that section.

```bash
# Release build (optimized)
cmake --preset release
cmake --build build/release -j

# Run the full test suite
ctest --preset release
```

A `debug` preset adds AddressSanitizer and UndefinedBehaviorSanitizer, and a
`tsan` preset runs the queue stress test under ThreadSanitizer:

```bash
cmake --preset debug && cmake --build build/debug -j && ctest --preset debug
```

## Trying it out

The quickest way to see the engine work is demo mode, which replays a scenario
and draws the book after each event. Scenarios are plain text, one command per
line:

```text
LIMIT <BID|ASK> <price> <qty> [GTC|IOC|FOK] [PARTICIPANT=<id>]
MARKET <BID|ASK> <qty>
CANCEL <handle>
```

`GTC` (rest any remainder) is the default when the time-in-force token is left
off, and an omitted participant means self-trade prevention does not apply to
that command. A small example:

```text
LIMIT BID 100 10
LIMIT BID 99 5
LIMIT ASK 105 8
LIMIT ASK 106 3
MARKET BID 4
```

Convert it to the binary command format and replay it in demo mode:

```bash
build/release/tools/text_to_saved_commands scenario.txt scenario.commands
build/release/tools/run_engine scenario.commands --demo --yield
```

The final market buy crosses the book and partially fills the resting ask at
105:

```text
EVENT Fill seq=5 side=Bid price=105 qty=4 handle=0
ASKS
  105 | 4 | ####
  106 | 3 | ###
BIDS
  100 | 10 | ##########
  99 | 5 | #####
```

`run_engine` has other output modes too: `--memory` drains the events to
in-memory counters, `--log events.log` writes a deterministic event log, and
`--book final.book` dumps the final book state, while `--cores A,B,C` pins the
producer, matcher, and logger threads. For larger inputs,
`tools/generate_saved_commands` builds a repeatable workload from a seed, with
`--insert-heavy`, `--cancel-heavy`, `--cross-heavy`, and `--mixed` modes, which
are the same traces the benchmarks use.

There is also a TCP mode (an extension; see the inventory below) using
`tools/tcp_server` and `tools/client`, which serves multiple clients over local
sockets, with a separate "spectator" role that streams market-data updates. See
[`tools/README.md`](tools/README.md) for the full command-line reference.

## Performance

Latency was measured with the harness in [`bench/engine_bench.cpp`](bench/engine_bench.cpp)
(driven by [`bench/run_engine_bench.sh`](bench/run_engine_bench.sh)) on a
dedicated cloud VM (Intel Xeon E3-1270 v6, 3.8 GHz, single NUMA node) with the
CPU governor set to `performance` and all three threads pinned to separate
physical cores. The VM was used rather than the WSL2 environment used for
development because reliable latency numbers need real governor control and
pinning to physical cores, which WSL2 does not provide (it exposes only
virtual-CPU affinity). Each workload replays a pre-generated trace file (for
example `traces/mixed.commands`, produced by `tools/generate_saved_commands`).
Each configuration was run 8 times; the table reports the median across runs. The
raw per-run output is stored under [`bench/results/`](bench/results/), and the
full write-up is in [`docs/engine-benchmark-results.md`](docs/engine-benchmark-results.md).

The generator is open-loop: it sends at a fixed rate and does not slow down
when the engine does. This is the standard defense against coordinated
omission, the measurement flaw where a generator that stalls alongside the
system hides exactly the worst-case latencies it should be exposing. Every run
reported here passed its validity check (no queue backpressure occurred at any
tested rate).

All figures are nanoseconds, end-to-end (from order submitted to response
recorded).

**At 100,000 orders/second:**

| Workload | p50 | p99 | p99.9 |
|---|---:|---:|---:|
| insert (mostly resting limits) | 344 | 654 | 147,939 |
| cancel (50% cancels, valid handles) | 347 | 419 | 6,346 |
| cross (heavy market-order matching) | 368 | 791 | 6,216 |
| mixed (limits, markets, cancels, IOC/FOK, self-trade) | 360 | 944 | 5,302 |

**Mixed workload as the send rate rises:**

| Rate | p50 | p99 | p99.9 |
|---|---:|---:|---:|
| 100,000/s | 360 | 944 | 5,302 |
| 1,000,000/s | 342 | 998 | 9,860 |
| 2,000,000/s | 322 | 1,442 | 15,109 |

Maximum sustained throughput was measured separately in closed-loop mode
(the producer sends as fast as the queue accepts, with per-operation latency
sampling turned off, since closed-loop latency numbers are not meaningful) at
about 8.08 million orders/second. Against that ceiling, the latency runs
above are modestly loaded: 1M/s is roughly 12% of saturation and the 2M/s run is
around 25%.

One caveat: the insert-only workload shows a much larger tail than the
others (its p99.9 reaches about 290µs at 1M/s). The likely cause is that a
pure-insert stream never cancels anything, so the number of live orders grows for
the entire run instead of settling into steady add/remove churn like the other
workloads; that growing working set produces more cache misses over time. This is
a current hypothesis, not yet confirmed by direct pool-occupancy inspection.

### Component benchmark: the queue

The lock-free queue was also benchmarked on its own in
[`bench/spsc_ring_bench.cpp`](bench/spsc_ring_bench.cpp) (one producer, one
consumer, 100 million messages, verifying strict FIFO order), with the run
recorded in [`docs/ring-benchmark-record.md`](docs/ring-benchmark-record.md).
This is a component-level comparison run locally, not the engine's end-to-end
number:

| Queue variant | Messages/second |
|---|---:|
| Lock-free ring (cache-line padded) | 393,000,000 |
| Same ring, padding removed | 72,000,000 |
| Mutex-protected preallocated ring | 6,700,000 |
| Mutex-protected `std::deque` | 6,400,000 |

The direction is the useful result: cache-line padding accounts for roughly a 5x
gap, and the lock-free design roughly a 60x gap over a conventional locked queue.
Exact ratios are environment-dependent.

## Correctness

The main correctness check runs the engine side by side with a much simpler
reference implementation and compares every event the two produce.

The reference ([`test/reference_book.hpp`](test/reference_book.hpp)) is a sorted
`std::map` of double-ended queues: slow, but simple enough to read and trust. It
follows the same matching rules as the real engine while sharing none of its
internals: no pool, no bitmap, no handle scheme. The differential fuzzer
([`test/differential_fuzz_test.cpp`](test/differential_fuzz_test.cpp)) generates
random order streams, feeds the identical sequence to both, and compares their
events one operation at a time. It leans on the awkward cases deliberately: many
orders stacked at a single price, orders at the very edges of the price band, and
bursts of cancels against orders that are genuinely live. When the two disagree,
it prints the seed and operation number so the exact run can be replayed.

The engine can also audit its own state, walking every price level, every resting
order, and the free list to confirm nothing has drifted out of sync. Auditing
after every operation is too slow to also run at full size, so it happens two
ways: a small build audits after every single operation (which keeps the pool
full and surfaces edge cases constantly), while a full-size build audits every
100,000 operations (which lets a run reach the states only a large book hits).
Both have run for 10 million operations without a mismatch; the details are in
[`docs/fuzz-test-record.md`](docs/fuzz-test-record.md).

The lock-free queue is checked on its own. It runs under ThreadSanitizer, but the
simpler reason it holds up is that each of its two indices has exactly one writer,
so there is no race to begin with. The sanitizer is there to catch a mistake in
that reasoning, not to stand in for it.

All of this runs in CI on every push: debug and release builds under both GCC and
Clang, plus a formatting check.

## Feature inventory

**The core matching engine.** This is the main deliverable; everything here is
either on the matching hot path or the machinery that tests and measures it:

- **Price-time priority matcher:** limit, market, and cancel, best price first
  and FIFO within a price ([`matcher.hpp`](include/orderbook/matcher.hpp)).
- **Fixed-capacity object pool** with generational handles: allocation-free
  order storage ([`pool.hpp`](include/orderbook/pool.hpp)).
- **Array-and-bitmap price levels:** direct-indexed levels with a bit-scan for
  the best price ([`book.hpp`](include/orderbook/book.hpp),
  [`bitmap.hpp`](include/orderbook/bitmap.hpp)).
- **Lock-free SPSC ring buffers:** the cross-thread handoff
  ([`spsc_ring.hpp`](include/orderbook/spsc_ring.hpp)).
- **Explicit codec:** validated fixed-offset format for files and the network
  wire ([`codec.hpp`](include/orderbook/codec.hpp),
  [`src/codec.cpp`](src/codec.cpp)).
- **IOC / FOK / self-trade prevention:** time-in-force modifiers and a
  participant-id check in the cross phase (the last features that touch the hot
  path).
- **Deterministic replay checks:** same trace in, identical event log and final
  book out.
- **Differential fuzz and reference tests:** the optimized engine checked against
  an independent reference book.
- **Benchmark harness:** open-loop (latency) and closed-loop (throughput)
  generators with latency histograms.
- **Demo renderer:** a `--demo` ASCII book view for watching the engine work.

**Extensions.** These were built on top of the finished core, after the benchmark
run. None of them touch the matching hot path, which is exactly why they could be
added without re-measuring the engine:

- **TCP gateway:** a multi-client epoll server and a client program, so the
  engine can be driven over sockets ([`tools/tcp_server.cpp`](tools/tcp_server.cpp)).
- **L2 market data:** price-level snapshots and live updates, rebuilt purely from
  the outbound event stream with no access to the live book
  ([`l2_book.hpp`](include/orderbook/l2_book.hpp)).

## Possible extensions

The scope was deliberately the matching engine core, with the network and
market-data layers kept as thin demonstrations on top. Each natural next step
builds on a seam the design already leaves open:

- **A fuller network front end.** The TCP gateway is a demo harness: it has no
  authentication or TLS, no client reconnect or session recovery, and no durable
  log to replay after a crash, so today it is meant for loopback or a trusted
  network. Adding those would turn it into a real front end. A latency-focused
  deployment would go further and swap the socket layer for kernel-bypass
  networking, which the engine is set up for, since the matching core sits behind
  the producer thread unchanged however that thread is fed.
- **Persistence and an audit log.** The outbound event stream already carries
  everything needed to reconstruct the book, so an append-only log or a durable
  store could be added as another consumer of that stream, the same way the L2
  view already is.
- **Multiple instruments.** One book and pool per instrument is a clean addition
  (books share nothing by design), needing only an instrument id in the codec and
  a dispatch step in the matching loop.
- **Multi-producer ingest.** Not needed today, since the epoll gateway feeds the
  ring from a single thread. If it ever were, the queue could be swapped for a
  multi-producer variant with the same push/pop shape, leaving the matching side
  untouched.

## Repository layout

```
include/orderbook/   the engine (headers): pool, book, bitmap, matcher,
                     ring buffer, codec, L2 view, TCP connection state
src/                 codec and text-parser implementations
tools/               command-line programs: trace generation, replay,
                     demo, TCP server and client
test/                unit tests, the reference book, the differential fuzzer,
                     and end-to-end pipeline tests
bench/               latency and throughput harness, plus analysis scripts
docs/                recorded benchmark and fuzz-test runs
```