#!/usr/bin/env bash
# Convert a tiny hand-written scenario and require demo mode to render visible
# order-book output.
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: demo_mode_test.sh <text_to_saved_commands> <run_engine>" >&2
  exit 2
fi

converter=$1
runner=$2

work_dir=$(mktemp -d /tmp/orderbook_demo_mode.XXXXXX)
trap 'rm -rf "$work_dir"' EXIT

text_commands="$work_dir/demo.txt"
saved_commands="$work_dir/demo.commands"
demo_output="$work_dir/demo.out"

# Debug builds use sanitizers locally. Leak detection is disabled because
# LeakSanitizer can fail to start under WSL/Linux dev runs even when the program is fine.
export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=0}"

# This scenario rests one bid and one ask, then crosses the ask with a market
# buy. The demo should therefore show accepted orders, a fill, and both book
# sides in its terminal output.
cat >"$text_commands" <<'EOF'
LIMIT BID 100 10
LIMIT ASK 103 8
MARKET BID 4
EOF

"$converter" "$text_commands" "$saved_commands" >/dev/null
"$runner" "$saved_commands" --demo --yield >"$demo_output"

for expected in "EVENT AckNew" "EVENT Fill" "ASKS" "BIDS"; do
  if ! grep -q "$expected" "$demo_output"; then
    echo "demo output did not contain: $expected" >&2
    exit 1
  fi
done

echo "demo_mode_test OK"
