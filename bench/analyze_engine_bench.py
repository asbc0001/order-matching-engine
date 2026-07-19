#!/usr/bin/env python3
"""Summarize repeated engine_bench runs saved by run_engine_bench.sh.

The runner keeps the raw output from every run. This script reads those files
afterward and computes the median and spread for each reported metric.
"""

from __future__ import annotations

import argparse
import re
import statistics
import sys
from pathlib import Path


# These patterns match the plain-text lines printed by engine_bench. Keeping
# them strict makes format drift obvious instead of silently misreading a run.
DISTRIBUTION_RE = re.compile(
    r"^(?P<name>[a-z_]+_ns): p50=(?P<p50>\d+) p99=(?P<p99>\d+) "
    r"p99\.9=(?P<p999>\d+) max=(?P<max>\d+) samples=(?P<samples>\d+)$"
)
THROUGHPUT_RE = re.compile(
    r"^throughput: elapsed_ns=(?P<elapsed_ns>\d+) "
    r"commands_per_second=(?P<commands_per_second>\d+)$"
)
WAITS_RE = re.compile(
    r"^waits: inbound_full=(?P<inbound_full>\d+) inbound_empty=(?P<inbound_empty>\d+) "
    r"outbound_full=(?P<outbound_full>\d+) outbound_empty=(?P<outbound_empty>\d+)$"
)
LATE_SENDS_RE = re.compile(r"^late_sends=(?P<late_sends>\d+)$")
EXIT_RE = re.compile(r"^exit_code=(?P<exit_code>\d+)$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize run_XX.txt files from bench/run_engine_bench.sh."
    )
    parser.add_argument("result_dir", type=Path, help="directory containing run_XX.txt files")
    return parser.parse_args()


def spread(values: list[int]) -> float:
    """Return (max - min) / median. A zero median means spread is undefined."""
    median = statistics.median(values)
    if median == 0:
        return 0.0 if max(values) == min(values) else float("inf")
    return (max(values) - min(values)) / median


def format_spread(value: float) -> str:
    if value == float("inf"):
        return "inf"
    return f"{value * 100:.1f}%"


def format_median(value: float) -> str:
    # statistics.median returns int for odd-sized integer inputs on Python 3.10
    # and 3.11, so normalize before using float-only helpers.
    value = float(value)
    if value.is_integer():
        return str(int(value))
    return f"{value:.1f}"


def add_metric(metrics: dict[str, list[int]], name: str, value: int) -> None:
    metrics.setdefault(name, []).append(value)


def parse_run_file(path: Path, metrics: dict[str, list[int]]) -> bool:
    """Parse one raw run. Returns False if the run itself reported failure."""
    exit_code = None
    for line in path.read_text(encoding="utf-8").splitlines():
        if match := DISTRIBUTION_RE.match(line):
            name = match.group("name")
            for field, label in (("p50", "p50"), ("p99", "p99"), ("p999", "p99.9")):
                add_metric(metrics, f"{name}.{label}", int(match.group(field)))
            for field in ("max", "samples"):
                add_metric(metrics, f"{name}.{field}", int(match.group(field)))
            continue

        if match := THROUGHPUT_RE.match(line):
            for field in ("elapsed_ns", "commands_per_second"):
                add_metric(metrics, f"throughput.{field}", int(match.group(field)))
            continue

        if match := WAITS_RE.match(line):
            for field in ("inbound_full", "inbound_empty", "outbound_full", "outbound_empty"):
                add_metric(metrics, f"waits.{field}", int(match.group(field)))
            continue

        if match := LATE_SENDS_RE.match(line):
            add_metric(metrics, "late_sends", int(match.group("late_sends")))
            continue

        if match := EXIT_RE.match(line):
            exit_code = int(match.group("exit_code"))

    # A missing exit_code line usually means the runner was interrupted while
    # writing the file, so treat it as failed.
    return exit_code == 0


def print_table(metrics: dict[str, list[int]]) -> None:
    # Markdown keeps the output easy to paste into benchmark notes.
    print("| Metric | Runs | Median | Min | Max | Spread |")
    print("|---|---:|---:|---:|---:|---:|")
    for name in sorted(metrics):
        values = metrics[name]
        median = statistics.median(values)
        print(
            f"| {name} | {len(values)} | {format_median(median)} | {min(values)} | {max(values)} | "
            f"{format_spread(spread(values))} |"
        )


def main() -> int:
    args = parse_args()
    run_files = sorted(args.result_dir.glob("run_*.txt"))
    if not run_files:
        print(f"no run_*.txt files found in {args.result_dir}", file=sys.stderr)
        return 2

    metrics: dict[str, list[int]] = {}
    failed_runs = [path.name for path in run_files if not parse_run_file(path, metrics)]
    if failed_runs:
        print(f"failed runs present: {', '.join(failed_runs)}", file=sys.stderr)
        return 1
    if not metrics:
        print(f"no benchmark metrics found in {args.result_dir}", file=sys.stderr)
        return 1

    print(f"runs={len(run_files)}")
    print_table(metrics)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
