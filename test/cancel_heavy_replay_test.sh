#!/usr/bin/env bash
# Generate a cancel-heavy saved command file and require replay to accept every
# cancel handle.
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: cancel_heavy_replay_test.sh <generate_saved_commands> <run_engine>" >&2
  exit 2
fi

generator=$1
runner=$2

work_dir=$(mktemp -d /tmp/orderbook_cancel_heavy_replay.XXXXXX)
trap 'rm -rf "$work_dir"' EXIT

commands="$work_dir/cancel_heavy.commands"
log="$work_dir/cancel_heavy.log"

# Debug builds use sanitizers locally. Leak detection is disabled because
# LeakSanitizer can fail to start under WSL/Linux dev runs even when the program is fine.
export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=0}"

# The generator learns real handles before writing CANCEL commands. A replayed
# UnknownHandle reject would mean that handle chain broke.
"$generator" "$commands" 400 123 --cancel-heavy >/dev/null
"$runner" "$commands" --log "$log" --yield >/dev/null

if ! grep -q "event=AckCancel" "$log"; then
  echo "cancel-heavy replay did not accept any cancels" >&2
  exit 1
fi

if grep -q "reason=UnknownHandle" "$log"; then
  echo "cancel-heavy replay produced UnknownHandle rejects" >&2
  exit 1
fi

echo "cancel_heavy_replay_test OK"
