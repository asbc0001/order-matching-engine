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

Benchmark workload modes:

```bash
build/debug/tools/generate_saved_commands output.commands 1000 123 --insert-heavy
build/debug/tools/generate_saved_commands output.commands 1000 123 --cancel-heavy
build/debug/tools/generate_saved_commands output.commands 1000 123 --cross-heavy
build/debug/tools/generate_saved_commands output.commands 1000 123 --mixed
```

The generator runs commands through the matcher while writing the file. That
lets later generated cancels use real handles assigned by earlier accepted
orders.

`--insert-heavy` mostly creates resting limit orders. `--cancel-heavy` creates
valid cancels from real handles. `--cross-heavy` builds resting liquidity and
then consumes it with market orders. `--mixed` includes limits, markets,
cancels, IOC/FOK, and participant IDs; it uses a larger participant pool so
self-trade prevention is represented without turning most crossing orders into
self-trade rejects.

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

## tcp_server and client

Runs the threaded engine behind local TCP sockets. Trading clients send text
commands through `client`; the client encodes them into the fixed-size binary
records expected by the server.

Start the server:

```bash
build/debug/tools/tcp_server 9001 --clients 2 --yield
```

By default the server listens only on `127.0.0.1`. To accept connections from
other machines, bind explicitly:

```bash
build/debug/tools/tcp_server 9001 --bind 0.0.0.0 --clients 2 --yield
```

Use external binding only on a trusted network. This development server has no
authentication or TLS.

In another terminal, watch public L2 market-data updates:

```bash
build/debug/tools/client --spectator 127.0.0.1 9001
```

In a third terminal, send trading commands:

```bash
build/debug/tools/client 127.0.0.1 9001
```

Example trader input:

```text
LIMIT BID 100 10
LIMIT ASK 105 4
MARKET BID 2
```

Normal client mode prints private execution responses such as `AckNew`, `Fill`,
or `Reject`. Spectator mode sends `SPECTATOR` once, prints the current L2
snapshot, then prints live L2 update lines such as:

```text
SNAPSHOT_BEGIN seq=0
SNAPSHOT_END seq=0
L2 seq=1 side=Bid price=100 qty=10
```

Protocol overview:

- The server binds to `127.0.0.1` by default and assigns a new participant id to
  each accepted trading connection.
- Trading clients send fixed-size binary command records; `client` hides that
  by parsing the text grammar above and encoding each command.
- Trading clients receive fixed-size binary event records, decoded by `client`
  into readable `AckNew`, `Fill`, `Reject`, and `AckCancel` lines.
- Spectator clients send `SPECTATOR` as their first line, then receive a text
  L2 snapshot followed by live text updates. Each snapshot and update carries
  an L2 sequence number so a client can detect missed update lines. Spectators
  do not submit orders.
- Malformed binary commands close that client connection. Valid rejected orders
  stay connected and return a `Reject` event.
- Slow readers are disconnected once their pending response buffer exceeds the
  configured cap, so one client cannot block the server.
