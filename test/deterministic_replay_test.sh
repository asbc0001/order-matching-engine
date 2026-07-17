#!/usr/bin/env bash
# Replay the same saved command file twice and require the event log and final
# book state to match exactly.
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: deterministic_replay_test.sh <generate_saved_commands> <run_engine>" >&2
  exit 2
fi

generator=$1
runner=$2

work_dir=$(mktemp -d /tmp/orderbook_deterministic_replay.XXXXXX)
trap 'rm -rf "$work_dir"' EXIT

commands="$work_dir/input.commands"
log_a="$work_dir/run_a.log"
log_b="$work_dir/run_b.log"
book_a="$work_dir/run_a.book"
book_b="$work_dir/run_b.book"

# Debug builds use sanitizers locally. Leak detection is disabled because
# LeakSanitizer can fail to start under WSL/Linux dev runs even when the program is fine.
export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=0}"

# One fixed saved-command file is replayed twice. Only timing/counter output may
# differ; the canonical event logs and final book state must be identical.
"$generator" "$commands" 200 777 >/dev/null
"$runner" "$commands" --log "$log_a" --book "$book_a" --yield >/dev/null
"$runner" "$commands" --log "$log_b" --book "$book_b" --yield >/dev/null

if ! cmp -s "$log_a" "$log_b"; then
  echo "canonical event logs differed" >&2
  exit 1
fi

if ! cmp -s "$book_a" "$book_b"; then
  echo "final book states differed" >&2
  exit 1
fi

echo "deterministic_replay_test OK"
