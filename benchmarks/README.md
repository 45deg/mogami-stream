# Benchmarks

```sh
pnpm bench --json=/tmp/mogami-bench.json
pnpm bench --case=async-pipeline --items=100000
# Bun:
pnpm build
bun benchmarks/run.mjs --json=/tmp/mogami-bun.json
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

Normal GC (`--gc=none`) is the default, even when the runtime exposes `gc`. To request manual GC before each warmup and measured iteration, run `node --expose-gc benchmarks/run.mjs --gc=between` (or use Bun). Reports record the mode and manual call count. Compare changes under the same mode; keep manual-GC results separate.

## Options

Use `--name=value`:

| Option        | Default | Description                                           |
| ------------- | ------: | ----------------------------------------------------- |
| `gc`          |    none | `none` or `between`; `between` requires `--expose-gc` |
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

The default command builds, measures Node.js then Bun, and plots all three cases: 100,000 inputs, 5 warmups, 7 samples, seed 42, concurrency 8, capacity 16, and normal GC. Use `pnpm bench:svg --gc=between` for a separate manual-GC run; the renderer exposes GC in its child runtimes. Measurement options above can override these defaults; `--svg` and `--results-dir` change output paths. `--reuse` skips measurement and cannot change its settings.

Raw samples are generated locally as `results/readme/node.json` and `bun.json` and excluded from Git. Only the README chart is tracked. Plotting labels the GC condition and rejects mixed conditions, including saved reports from before the explicit GC option. Plotting checks matching settings, cases, library versions, and sample counts. Update the root README's environment caption when remeasuring.

## Representative waits and cleanup

[`representative.mjs`](representative.mjs) adds seeded timer jitter, a slow sink, retries, cooperative/uncooperative cancellation, concurrency 1,024 and a 10,000-start rate window. See [loads, raw samples, sampled-memory limits and the decision to defer queue changes](representative.md). It runs separately from the Promise-only overhead cases and README chart.

## Broadcast

`pnpm bench:broadcast --json=/tmp/broadcast.json` measures bounded fan-out with two or eight balanced consumers, Promise-returning input, a sibling that drains in batches of 64, and seven detached endpoints. Each run checks every branch's output order and count, per-branch read-ahead bounds, EOF, and completion reports.

Defaults are 100,000 inputs, three warmups, five measured samples and five independent processes per case. `--items`, `--warmup`, `--samples`, `--trials`, `--case` and `--json` override these settings. Each process has a 120-second watchdog. Reports include raw wall/CPU samples, runtime versions and CPU model; summaries show median/min/max of independent process medians. Timing includes construction, validation, consumption and cleanup with normal GC. Broadcast's fairness timer remains enabled.

To compare builds, copy the baseline's entire `dist` directory before rebuilding, then pass its entry point with `--baseline`:

```sh
pnpm build
cp -R dist /tmp/mogami-broadcast-baseline
# Make the change, then rebuild and compare:
pnpm bench:broadcast --baseline=/tmp/mogami-broadcast-baseline/index.js --json=/tmp/broadcast-comparison.json
# Run the same comparison on Bun after building:
bun benchmarks/broadcast.mjs --baseline=/tmp/mogami-broadcast-baseline/index.js --json=/tmp/broadcast-bun.json
```

`--module` selects the candidate entry point (default `dist/index.js`). Comparisons run each baseline/candidate pair sequentially and reverse their order on alternate trials. Cases also reverse order between trials. Runtime flags are forwarded to workers. Use separate runs for CPU profiling so sampling overhead stays out of the comparison; for example, `node --cpu-prof --cpu-prof-dir=/tmp benchmarks/broadcast.mjs --case=sync-eight --trials=1` creates profiles for the parent and its worker.

## Route

`pnpm bench:route --json=/tmp/route.json` measures numeric and named destinations, branch predicates, Promise-returning sources and selectors, ignored items, detached destinations with a default route, and a lagging consumer. Each run checks destination membership, per-branch order, counts, checksums, capacity bounds, EOF, and completion reports.

The defaults, CLI options, independent-process comparisons, watchdog, and wall/CPU reporting follow the [broadcast benchmark](#broadcast). Snapshot the baseline's complete `dist` directory before changing the implementation:

```sh
cp -R dist /tmp/mogami-route-baseline
# Make the change, then rebuild and compare:
pnpm bench:route --baseline=/tmp/mogami-route-baseline/index.js --json=/tmp/route-comparison.json
# Select one workload or run the built version on Bun:
node benchmarks/route.mjs --case=guards-eight --json=/tmp/route-guards.json
bun benchmarks/route.mjs --baseline=/tmp/mogami-route-baseline/index.js --json=/tmp/route-bun.json
```
