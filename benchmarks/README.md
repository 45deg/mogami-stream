# Workload benchmarks

Run from the repository root:

```sh
pnpm bench --json=/tmp/mogami-bench.json
pnpm bench:compare --json=/tmp/mogami-compare.json
pnpm bench:compare --case=slow-sink --samples=11 --seed=43 --json=/tmp/slow-sink.json
```

`pnpm bench` compares `mogami-stream` against small native JavaScript reference implementations that satisfy the same behavioral contract. `pnpm bench:compare` adds third-party libraries from the `benchmarks/compare` workspace where an equivalent API exists. Libraries that cannot express a workload's contract (such as bounded read-ahead or ordered streaming) are omitted for that workload rather than emulated with unbounded buffers or out-of-order output.

## Workloads and contracts

| Case              | Workload & contract                                                                                                         | Implementations in comparison mode                              |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------- |
| `records-stream`  | Parse JSON records, filter active records, project numeric values, and consume in input order without buffering all outputs | Native async generator, Mogami, IxJS, iter-tools                |
| `records-batch`   | Same transformation, collecting all results into an ordered array before consumption                                        | Native arrays, Mogami, IxJS, iter-tools, RxJS                   |
| `io-serial`       | Perform simulated variable-latency I/O one item at a time and consume in order                                              | Native async generator, Mogami, IxJS, iter-tools                |
| `io-batch`        | Run simulated I/O with bounded concurrency and collect all results into an ordered array                                    | Native worker pool, Mogami, p-map, p-limit, p-queue, p-all      |
| `io-stream`       | Run simulated I/O with bounded concurrency and read-ahead, consuming incrementally in input order                           | Mogami, pMapIterable                                            |
| `slow-sink`       | Same bounded stream, allocating a 4 KiB `Buffer` per result and awaiting a simulated slow sink write for each item          | Mogami, pMapIterable                                            |
| `early-stop`      | Transform and read a fixed prefix of a lazy source, then break; must pull only the prefix and close the source              | Native async generator, Mogami, IxJS, iter-tools                |
| `cancel-inflight` | Fill all concurrency slots, trigger cancellation, and wait for source cleanup and all in-flight tasks to settle             | Native worker pool + `AbortController`, Mogami `StreamIterator` |

The native async generator and worker pool serve as minimal baseline implementations. The `cancel-inflight` case compares task-specific cancellation paths (an external `AbortController` for the native worker pool vs. `StreamIterator.cancel()` for Mogami). RxJS is included only in `records-batch`, where the output is collected into an array, to avoid comparing push-based subscription against pull-based async iteration in streaming workloads.

Inputs are pre-built before timing starts:

- Record inputs are JSON strings containing an ID, amount, and `active` flag.
- I/O inputs are numeric IDs. Every fifth item waits `4 * latency-ms` while all others wait `latency-ms`, creating out-of-order completions that exercise ordered delivery.
- `early-stop` and `cancel-inflight` sources are lazy async generators whose `finally` block waits `latency-ms` to simulate asynchronous resource cleanup.
- `slow-sink` waits `sink-ms` per item and verifies each 4 KiB `Buffer`, testing whether read-ahead stays bounded when downstream consumption is slower than upstream production.

## Verification

Every warmup and measured sample verifies exact output IDs, ordering, values, and item counts. At completion, the harness also asserts that the source was closed and no mapper calls remain active:

- I/O workloads verify that active concurrency never exceeded `concurrency`.
- `early-stop` verifies that no extra items were pulled beyond `stop-after`.
- Bounded streaming workloads (`io-stream`, `slow-sink`) verify that observed read-ahead stayed within `capacity + 1` (accounting for the one item already handed to the sink).
- `cancel-inflight` waits until all `concurrency` slots have entered the mapper before cancelling, then verifies that all pending timers aborted and the source closed cleanly.
- Each execution runs under a timeout watchdog (`timeout-ms`).

## Timing and metrics

Each `(workload, library)` pair runs in a dedicated child process, executed sequentially in a seeded random order (`--seed`). Module imports and input data generation happen before timing. Each child process runs `--warmup` unrecorded iterations followed by `--samples` measured iterations. When Node runs with `--expose-gc` (enabled by default in the npm scripts), a manual GC runs before each iteration outside the timed window.

Each timed sample covers pipeline creation, iteration, mapper execution, result collection (for batch cases), sink consumption, per-item verification counters, and source cleanup. Process startup and final summary assertions happen outside the timed window.

JSON reports record the following metrics per case:

- `elapsedMs`: median, minimum, maximum, and raw wall-clock samples (ms).
- `cpuMs`: process CPU time spent during the sample window (ms).
- `firstItemMs`: time until the sink receives the first item (for batch cases, this includes building the full array; `null` for empty results or `cancel-inflight`).
- `stopToClosedMs`: time from stopping a prefix read or issuing cancellation until source cleanup finishes.
- `maxActive`: peak concurrent mapper invocations observed (`0` for synchronous record transformations).
- `maxAhead`: peak difference between items pulled from the source and items delivered to the sink (`null` when filtering changes item counts).
- `maxReady`: peak completed mapper results waiting to be received by the sink.

## CLI options

Pass options as `--name=value`:

| Option        | Default | Description                                                                  |
| ------------- | ------: | ---------------------------------------------------------------------------- |
| `items`       |   10000 | Input size for `records-*` and `early-stop`                                  |
| `io-items`    |     128 | Input size for `io-*`, `slow-sink`, and `cancel-inflight` (`>= concurrency`) |
| `concurrency` |       8 | Concurrent mapper limit (`io-serial` always uses `1`)                        |
| `capacity`    |      16 | Stream read-ahead capacity (`>= concurrency`; `io-serial` uses `1`)          |
| `latency-ms`  |       2 | Base simulated I/O and cleanup delay in milliseconds (positive integer)      |
| `sink-ms`     |       2 | Simulated write delay per item in `slow-sink` (positive integer)             |
| `stop-after`  |     100 | Number of items read in `early-stop` (capped at `items`)                     |
| `warmup`      |       2 | Warmup iterations per process (`>= 0`)                                       |
| `samples`     |       7 | Measured iterations per process (`>= 1`)                                     |
| `seed`        |      42 | PRNG seed used to shuffle execution order                                    |
| `timeout-ms`  |   30000 | Per-iteration watchdog timeout in milliseconds                               |
| `case`        |     all | Run a single workload by name, or `all`                                      |
| `json`        |    none | File path to write the full JSON report (the parent directory must exist)    |
