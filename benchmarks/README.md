# Benchmarks

```sh
pnpm bench --json=/tmp/mogami-bench.json
pnpm bench --case=async-pipeline --items=100000
# Bun:
pnpm build
bun benchmarks/run.mjs --json=/tmp/mogami-bun.json
```

| Case                     | Workload                                                    | Libraries                              |
| ------------------------ | ----------------------------------------------------------- | -------------------------------------- |
| `async-map-serial`       | Async map, concurrency/capacity 1/1                         | Mogami, IxJS, iter-tools, pMapIterable |
| `async-map-concurrent`   | Bounded async map, ordered output                           | Mogami, pMapIterable                   |
| `async-pipeline`         | 13 async map/filter stages                                  | Mogami, IxJS, iter-tools               |
| `broadcast-balanced`     | Synchronous input, eight balanced consumers                 | Mogami                                 |
| `broadcast-lagging`      | Synchronous input, two consumers, one reading in batches    | Mogami                                 |
| `route-numeric-pair`     | Numeric routing to two consumers                            | Mogami                                 |
| `route-keyed-eight`      | Named routing to eight consumers                            | Mogami                                 |
| `route-guards-eight`     | Predicate routing to eight consumers                        | Mogami                                 |
| `route-async-source`     | Numeric routing with Promise-returning input                | Mogami                                 |
| `route-async-select`     | Numeric routing with a Promise-returning selector           | Mogami                                 |
| `route-ignored`          | Four predicates, ignoring half the input                    | Mogami                                 |
| `route-detached-default` | Named routing to a default after other branches detach      | Mogami                                 |
| `route-lagging`          | Two consumers, one yielding after each capacity-sized batch | Mogami                                 |

Inputs are numeric IDs. The `async-*` cases use an async generator. Map callbacks await `Promise.resolve()` and return `{ id, value: id * 2 }`. The pipeline projects IDs, then applies six increment/filter pairs, rejecting values divisible by 97. Broadcast uses synchronous input; the lagging consumer drains a full `capacity` batch at a time.

Route distributes IDs round-robin, except `route-ignored`, which drops unmatched IDs, and `route-detached-default`, which sends all IDs to the remaining default branch. Sources and selectors are synchronous except in their respective `route-async-*` cases. The lagging consumer yields with `setImmediate` after each `capacity` items.

These cases measure iteration and operator overhead without timer or I/O waits. Immediately resolved work need not reach the concurrency limit. IxJS and iter-tools use sequential adapters.

## Measurement

Each workload/library pair runs in a fresh process using the parent's runtime. Jobs run sequentially in a seeded shuffled order. Imports, prepared inputs, warmups, and manual GC are outside timing. Samples include pipeline construction, consumption, per-item validation, and cleanup.

Every run checks output order, values, counts, source closure, and task completion. Async maps also check concurrency and read-ahead bounds (`capacity + 1`, including the item handed to the sink). Broadcast and route check each branch's order, count, capacity, EOF, and completion report. Route also checks destination membership and checksums. Each run has a timeout watchdog.

JSON reports contain runtime/library versions, settings, raw samples, elapsed/CPU time, first-item latency, and observed concurrency/read-ahead. Broadcast's consumed count tracks the slowest branch; its `maxAhead` measures the source's lead over that branch. Route counts total delivered items and reports the largest per-branch backlog, including the coordinator's item, as `maxAhead`. Its samples also include per-branch delivered counts and checksums. Both use `null` for `maxReady`. Read-ahead metrics are `null` for the filtering pipeline; these are not heap measurements.

Normal GC (`--gc=none`) is the default, even when the runtime exposes `gc`. To request manual GC before each warmup and measured iteration, run `node --expose-gc benchmarks/run.mjs --gc=between` (or use Bun). Reports record the mode and manual call count. Compare changes under the same mode; keep manual-GC results separate.

## Options

Use `--name=value`:

| Option        | Default | Description                                                                                  |
| ------------- | ------: | -------------------------------------------------------------------------------------------- |
| `gc`          |    none | `none` or `between`; `between` requires `--expose-gc`                                        |
| `items`       |   10000 | Input count                                                                                  |
| `concurrency` |       8 | Mapper limit; serial map uses 1                                                              |
| `capacity`    |      16 | Read-ahead capacity (`>= concurrency`); serial uses 1; broadcast and route use it per branch |
| `warmup`      |       2 | Unrecorded iterations (`>= 0`)                                                               |
| `samples`     |       7 | Measured iterations (`>= 1`)                                                                 |
| `seed`        |      42 | Execution-order shuffle seed                                                                 |
| `timeout-ms`  |   30000 | Per-iteration timeout                                                                        |
| `case`        |     all | Comma-separated case names, or `all`                                                         |
| `json`        |    none | Report path; parent directory must exist                                                     |

## README chart

Requires Node.js, Bun, and [uv](https://docs.astral.sh/uv/). The plotting script declares Python >= 3.11 and a pinned Matplotlib dependency.

```sh
pnpm bench:svg
# Replot saved results; optionally preview as PNG:
pnpm bench:svg --reuse --png=/tmp/mogami-async.png
```

The default command builds, measures Node.js then Bun, and plots the three `async-*` cases: 100,000 inputs, 5 warmups, 7 samples, seed 42, concurrency 8, capacity 16, and normal GC. Use `pnpm bench:svg --gc=between` for a separate manual-GC run; the renderer exposes GC in its child runtimes. Measurement options above can override these defaults; `--svg` and `--results-dir` change output paths. `--reuse` skips measurement and cannot change its settings.

Raw samples are generated locally as `results/readme/node.json` and `bun.json` and excluded from Git. Only the README chart is tracked. Plotting labels the GC condition and rejects mixed conditions, including saved reports from before the explicit GC option. Plotting checks matching settings, cases, library versions, and sample counts. Update the root README's environment caption when remeasuring.

## Representative waits and cleanup

[`representative.mjs`](representative.mjs) adds seeded timer jitter, a slow sink, retries, cooperative/uncooperative cancellation, concurrency 1,024 and a 10,000-start rate window. It runs separately from the Promise-only overhead cases and README chart.
