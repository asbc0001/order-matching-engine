# tools/

Small command-line tools for creating saved command files and running them
through the threaded engine.

A saved command file is a binary file containing encoded order commands. The
runtime replay path reads that fixed-size format instead of parsing text.

## text_to_saved_commands

Converts a hand-written text file into a saved command file.

Input grammar:

```text
LIMIT <BID|ASK> <price> <qty> [GTC|IOC|FOK] [PARTICIPANT=<id>]
MARKET <BID|ASK> <qty>
CANCEL <handle>
```

`GTC` is the default limit-order behavior when the token is omitted.
`PARTICIPANT=0` or an omitted participant means no participant was assigned,
so self-trade prevention is not applied to that command.

Usage:

```bash
build/debug/tools/text_to_saved_commands input.txt output.commands
```

This is useful for small readable scenarios, especially demo runs.

## generate_saved_commands

Creates a repeatable saved command file from a seed.

Usage:

```bash
build/debug/tools/generate_saved_commands output.commands 1000 123
```

Cancel-heavy mode:

```bash
build/debug/tools/generate_saved_commands output.commands 1000 123 --cancel-heavy
```

The generator runs commands through the matcher while writing the file. That
lets later generated cancels use real handles assigned by earlier accepted
orders.

## run_engine

Replays a saved command file through the threaded engine:

```text
producer thread -> inbound ring -> matching thread -> outbound ring -> logger
```

Usage:

```bash
build/debug/tools/run_engine input.commands --memory
build/debug/tools/run_engine input.commands --log events.log
build/debug/tools/run_engine input.commands --demo --yield
build/debug/tools/run_engine input.commands --log events.log --book final.book
```

Useful options:

```text
--memory              drain events and print counters only
--log <file>          write a deterministic event log
--demo                render a compact terminal view of the book
--book <file>         write the final book state after the matcher exits
--yield               yield while waiting instead of tight spinning
--cores A,B,C         pin producer, matcher, and logger threads to CPU IDs
```

## engine_bench

Runs the threaded engine with a generated workload at a requested send rate.
This is a benchmark harness, not the final performance report.

Usage:

```bash
build/debug/bench/engine_bench [commands] [rate_per_second] [seed] [--yield]
```

Example:

```bash
build/debug/bench/engine_bench 100000 100000 123 --yield
```

The output reports event counts, latency percentiles, wait counters, and late
sends. Treat local/debug numbers as a functionality check; final benchmark
numbers should come from a dedicated release run on suitable hardware.
