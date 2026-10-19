# Benchmarks

```sh
pnpm bench --json=/tmp/mogami-bench.json
pnpm bench --case=async-pipeline --items=100000
# Bun:
pnpm build
bun --expose-gc benchmarks/run.mjs --json=/tmp/mogami-bun.json
```

| Case                   | Workload                            | Libraries                              |
| ---------------------- | ----------------------------------- | -------------------------------------- |
| `async-map-serial`     | Async map, concurrency/capacity 1/1 | Mogami, IxJS, iter-tools, pMapIterable |
| `async-map-concurrent` | Bounded async map, ordered output   | Mogami, pMapIterable                   |
| `async-pipeline`       | 13 async map/filter stages          | Mogami, IxJS, iter-tools               |

Inputs are numeric IDs from an async generator. Map callbacks await `Promise.resolve()` and return `{ id, value: id * 2 }`. The pipeline projects IDs, then applies six increment/filter pairs, rejecting values divisible by 97.

These cases measure async iteration and operator overhead without timer or I/O waits. Immediately resolved work need not reach the concurrency limit. IxJS and iter-tools use sequential adapters.

## Measurement

Each workload/library pair runs in a fresh process using the parent's runtime. Jobs run sequentially in a seeded shuffled order. Imports, prepared inputs, warmups, and manual GC are outside timing. Samples include pipeline construction, consumption, per-item validation, and cleanup.

Every run checks output order, values, counts, source closure, and task completion. Async maps also check concurrency and read-ahead bounds (`capacity + 1`, including the item handed to the sink). Each run has a timeout watchdog.

JSON reports contain runtime/library versions, settings, raw samples, elapsed/CPU time, first-item latency, and observed concurrency/read-ahead. Read-ahead metrics are `null` for the filtering pipeline; these are not heap measurements.

## Options

Use `--name=value`:

| Option        | Default | Description                                           |
| ------------- | ------: | ----------------------------------------------------- |
| `items`       |   10000 | Input count                                           |
| `concurrency` |       8 | Mapper limit; serial map uses 1                       |
| `capacity`    |      16 | Read-ahead capacity (`>= concurrency`); serial uses 1 |
| `warmup`      |       2 | Unrecorded iterations (`>= 0`)                        |
| `samples`     |       7 | Measured iterations (`>= 1`)                          |
| `seed`        |      42 | Execution-order shuffle seed                          |
| `timeout-ms`  |   30000 | Per-iteration timeout                                 |
| `case`        |     all | Comma-separated case names, or `all`                  |
| `json`        |    none | Report path; parent directory must exist              |

## README chart

Requires Node.js, Bun, and [uv](https://docs.astral.sh/uv/). The plotting script declares Python >= 3.11 and a pinned Matplotlib dependency.

```sh
pnpm bench:svg
# Replot saved results; optionally preview as PNG:
pnpm bench:svg --reuse --png=/tmp/mogami-async.png
```

The default command builds, measures Node.js then Bun, and plots all three cases: 100,000 inputs, 5 warmups, 7 samples, seed 42, concurrency 8, capacity 16, and GC between runs. Measurement options above can override these defaults; `--svg` and `--results-dir` change output paths. `--reuse` skips measurement and cannot change its settings.

Raw samples are generated locally as `results/readme/node.json` and `bun.json` and excluded from Git. Only the README chart is tracked. Plotting checks matching settings, cases, library versions, and sample counts. Update the root README's environment caption when remeasuring.

## Representative waits and cleanup

[`representative.mjs`](representative.mjs) adds seeded timer jitter, a slow sink, retries, cooperative/uncooperative cancellation, concurrency 1,024 and a 10,000-start rate window. See [loads, raw samples, sampled-memory limits and the decision to defer queue changes](representative.md). It runs separately from the Promise-only overhead cases and README chart.
