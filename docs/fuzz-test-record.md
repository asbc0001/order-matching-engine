# Fuzz Test Record

Long fuzz test runs. CI runs a reduced version of these, so this is the record
of the long ones. Both invocations are re-runnable as written.

| | |
|---|---|
| Date | 2026-07-15 |
| Commit | `7709a9395a1d0fb9e2f9ae411959d6a30f5acc31` |
| Compiler | `g++ 13.3.0` (Ubuntu 13.3.0-6ubuntu2~24.04.1) |
| Build | preset `release-ci`, `-O3 -march=x86-64-v3 -DNDEBUG` |

## Matching Engine, Single-Threaded

Random operations are fed to the engine and to a simple reference book. Every
event is compared. On the audit interval, the engine's internals are also walked
in full.

Two runs are used because auditing after every operation and running at full
size are too expensive to do together.

**Small capacity** checks correctness with an audit after every operation, so a
failure points at the exact operation that caused it. The book is small enough
that the pool fills up constantly, which a full-size pool never does by accident.

**Production capacity** is the size the benchmarks will use. Audits drop to
every 100,000 operations, and in exchange the run reaches things only a big book
has: long bitmap scans, sparse gaps between price levels, and deep multi-level
fills.

### Small Capacity - PASS

```bash
time OB_FUZZ_OPS=1000000 OB_FUZZ_AUDIT_EVERY=1 \
  build/release-ci/test/differential_fuzz_test
```

| | |
|---|---|
| Capacity | 256 price levels, pool 32 |
| Seeds | 10 |
| Randomized ops | 1,000,000 per seed, **10,000,000 total** |
| Setup ops | 330 |
| Audits | every operation |
| Wall time | **7.002 s** |
| Result | `differential_fuzz_test OK` |

### Production Capacity - PASS

```bash
time OB_FUZZ_PROFILE=production OB_FUZZ_OPS=10000000 OB_FUZZ_AUDIT_EVERY=100000 \
  build/release-ci/test/differential_fuzz_test
```

| | |
|---|---|
| Capacity | 65,536 price levels, pool 1,048,576 |
| Seeds | 1, `0xC0FFEE` |
| Randomized ops | **10,000,000** |
| Setup ops | 1,048,577, fills the pool across the price band |
| Audits | every 100,000 operations |
| Wall time | **7.929 s** |
| Result | `differential_fuzz_test OK` |
