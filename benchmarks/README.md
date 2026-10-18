# Workload benchmarks

Run from the repository root:

```sh
pnpm bench --json=/tmp/mogami-bench.json
pnpm bench:compare --json=/tmp/mogami-compare.json
pnpm bench:compare --case=slow-sink --samples=11 --seed=43 --json=/tmp/slow-sink.json
```

`bench` compares Mogami with native JavaScript where a small reference implementation expresses the same contract. `bench:compare` adds applicable APIs from the pinned comparison workspace. Unsupported combinations are omitted, not emulated with buffering or reordered output. There is no overall winner or aggregate score.

## Workloads and matching contracts

| Case              | Work and required result                                                                                            | Implementations in comparison mode                               |
| ----------------- | ------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| `records-stream`  | Parse JSON records, retain active records, project numeric values, consume in input order without collecting output | Native async generator, Mogami, IxJS, iter-tools                 |
| `records-batch`   | Same transformation, materialize an ordered result array before consumption                                         | Native arrays, Mogami, IxJS, iter-tools, RxJS                    |
| `io-serial`       | Await variable-latency simulated I/O one item at a time; consume in order                                           | Native async generator, Mogami, IxJS, iter-tools                 |
| `io-batch`        | Run I/O with the configured concurrency, collect an ordered result array                                            | Native worker pool, Mogami, p-map, p-limit, p-queue, p-all       |
| `io-stream`       | Run I/O with bounded concurrency and read-ahead; consume incrementally in input order                               | Mogami, pMapIterable                                             |
| `slow-sink`       | Same bounded stream, allocate a fresh 4 KiB Buffer per result, await a simulated sink write per item                | Mogami, pMapIterable                                             |
| `early-stop`      | Transform and read a prefix of a lazy source, then break; read exactly the requested prefix and close the source    | Native async generator, Mogami, IxJS, iter-tools                 |
| `cancel-inflight` | Fill every mapper slot, explicitly cancel, await source closure and all in-flight work                              | Native worker pool + AbortController, Mogami controlled iterator |

The native worker pool and generator are intentionally small reference implementations, not alternate production libraries. The cancellation row compares task-specific cleanup paths: native external signal cancellation and Mogami's iterator cancellation. It does not establish equivalent error or cancellation APIs across libraries. RxJS is included only where the requested result is an array, so its synchronous push consumer is not ranked against a pull consumer in the streaming rows.

Inputs are constructed before timing. Record inputs are JSON strings with IDs, amounts, and an active flag. I/O inputs are IDs. Prefix and cancellation sources are lazy async generators whose `finally` block awaits `latency-ms` to model asynchronous resource cleanup; this wait is included in total and cleanup latency. Each fifth I/O item waits `4 * latency-ms`; all others wait `latency-ms`. This produces completion-order differences that exercise ordered delivery. These timers model waiting, not network or disk throughput. No external service or credentials are needed.

The slow sink awaits `sink-ms` per item and checks each result's Buffer. It demonstrates whether read-ahead stays bounded when consumption is slower than production. It does not measure a real socket, database, filesystem, or total process memory.

## Verification

Every warmup and measured run checks exact output IDs, positions, values, and counts. A reordered sequence with the same count and checksum fails. Every run also requires source closure and zero active mapper calls at the completion boundary. I/O runs check the observed maximum concurrency and completion count. Early-stop runs require exactly the requested number of source reads. Bounded-stream runs require observed read-ahead to stay within the configured capacity plus one item at the delivery boundary.

Cancellation starts only after a barrier confirms all concurrency slots have entered their mapper. The simulated I/O then has a long pending timer, which must be aborted. Mogami must reject the pending read with the cancellation reason and finish its `closed` report; any secondary errors must be Node timer AbortErrors caused by that same reason. Remaining work or a source that was not closed fails the run. Each run has a watchdog; timeout is a failure, not a timing sample.

## Timing and metrics

Each case/library pair gets a fresh child process. Jobs run sequentially in a seeded shuffled order. Imports and input construction happen before measurement. Each process performs the configured warmups, then records every sample. If Node was started with `--expose-gc` (the package scripts do this), explicit GC runs before each execution, outside its timing window. Natural GC during execution remains part of the result.

The measurement includes pipeline/task construction, source iteration, mapper work, result allocation and collection where applicable, consumption, exact per-item assertions, shared counter instrumentation, and awaited cleanup. Final aggregate assertions and process startup are excluded. Instrumentation is deliberately shared but has a cost; this is an end-to-end workload harness, not a nanosecond operator microbenchmark. Other dependency imports in comparison mode are outside timing, but can still affect process footprint and runtime optimization.

Reports contain:

- `elapsedMs`: median, minimum, maximum, and every raw wall-clock sample.
- `cpuMs`: process CPU consumed during the same window, not per-item CPU or parallel speedup.
- `firstItemMs`: time until the common sink receives its first result. For array-result rows this includes waiting for the entire array. Empty results and cancellation report `null`.
- `stopToClosedMs`: cleanup latency after the final prefix item or an explicit cancellation request.
- `maxActive`: maximum simultaneous mapper calls. Synchronous record/prefix transformations are not counted here.
- `maxAhead`: maximum source items pulled minus items received by the sink, including running and completed work. Batch rows can reach the full input size.
- `maxReady`: maximum completed mapper outputs minus items received by the sink. This counts results, not bytes or heap allocations.

Read-ahead counters are `null` for record pipelines because filtering changes the input/output cardinality. For bounded I/O streams, a result can leave a library's internal queue before the common sink increments its counter; the measured bound therefore allows `capacity + 1`. These counters describe application-visible work and do not inspect private library queues.

Reports include the actual installed package versions, runtime version, configuration, execution order, and methodology. They omit OS, architecture, CPU, and host identifiers. Timing results apply only to the runtime and environment in which they were measured. Sub-millisecond prefix/cancellation measurements and small differences are particularly noisy. Use repeated invocations with different seeds, inspect the raw spread, and keep the host otherwise idle. The suite does not claim statistical significance, isolate JIT effects completely, or measure peak heap, retained heap, event-loop responsiveness, real I/O, failures/retries, or unordered delivery.

## Options

All options use `--name=value`; duplicate and unknown options fail.

| Option        | Default | Meaning                                                                |
| ------------- | ------: | ---------------------------------------------------------------------- |
| `items`       |   10000 | Record input count and available prefix-source size                    |
| `io-items`    |     128 | Input count for I/O, slow sink, and cancellation; at least concurrency |
| `concurrency` |       8 | Mapper slots for concurrent cases; serial case always uses 1           |
| `capacity`    |      16 | Stream read-ahead setting; at least concurrency; serial case uses 1    |
| `latency-ms`  |       2 | Base simulated I/O wait, positive integer                              |
| `sink-ms`     |       2 | Wait per slow-sink write, positive integer                             |
| `stop-after`  |     100 | Prefix length, capped at items                                         |
| `warmup`      |       2 | Warmups per case/library; may be zero                                  |
| `samples`     |       7 | Measured executions per case/library                                   |
| `seed`        |      42 | Reproducible shuffle of process execution order                        |
| `timeout-ms`  |   30000 | Per-execution failure timeout                                          |
| `case`        |     all | Exact workload name from the table                                     |
| `json`        |    none | Destination for complete JSON report; parent directory must exist      |

The previous operator microbenchmarks and checksum-only cross-library suite have been replaced. Old case names, `--baseline`, and old JSON fields are not supported. To compare revisions, run the same workload configuration on each revision and compare reports using the same runtime and dependency installation; do not mix the two report schemas.

## Optimization records

[Sequential map/filter delivery](results/sequential-delivery/README.md) records the Promise/CPU diagnosis, unchanged guarantees, and before/after raw workload samples. `node benchmarks/profile-sync.mjs` is a separate allocation-count diagnostic; do not enable `async_hooks` when timing workloads.
