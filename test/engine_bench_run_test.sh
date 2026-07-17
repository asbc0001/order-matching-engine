#!/usr/bin/env bash
# Run the engine benchmark at a tiny scale so CI can catch a broken benchmark
# executable. This is not a performance measurement; the numbers from this test
# are intentionally too small and environment-dependent to quote.
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: engine_bench_run_test.sh <engine_bench>" >&2
  exit 2
fi

benchmark=$1

work_dir=$(mktemp -d /tmp/orderbook_engine_bench_run.XXXXXX)
trap 'rm -rf "$work_dir"' EXIT

output="$work_dir/engine_bench.out"

# Debug builds use sanitizers locally. Leak detection is disabled because
# LeakSanitizer can fail to start under WSL/Linux dev runs even when the program is fine.
export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=0}"

"$benchmark" 1000 100000 123 --yield >"$output"

for expected in "commands=1000" "latency_ns:" "waits:" "late_sends="; do
  if ! grep -q "$expected" "$output"; then
    echo "benchmark run output did not contain: $expected" >&2
    exit 1
  fi
done

echo "engine_bench_run_test OK"
