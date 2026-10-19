# Benchmarks

## Async iteration and operator overhead

![Node.js and Bun elapsed times for sequential async map, bounded ordered async map, and a 13-stage async pipeline. Lower is faster; each panel has its own scale.](/benchmarks.svg)

**Environment:** 100,000 inputs · Node.js v26.5.0 / Bun v1.4.3 · Apple M1 Pro · Median of 7 measured runs after 5 warmup runs (seconds; lower is faster; each panel has its own horizontal scale).

| Benchmark case         | Workload                                                                                     | Compared libraries                     |
| ---------------------- | -------------------------------------------------------------------------------------------- | -------------------------------------- |
| `async-map-serial`     | Sequential async map (`concurrency: 1`, `capacity: 1`)                                       | Mogami, IxJS, iter-tools, pMapIterable |
| `async-map-concurrent` | Bounded concurrent async map (`concurrency: 8`, `capacity: 16`, ordered output)              | Mogami, pMapIterable                   |
| `async-pipeline`       | 13-stage async `map` / `filter` pipeline (6 increment/filter pairs rejecting `x % 97 === 0`) | Mogami, IxJS, iter-tools               |

Inputs are numeric IDs yielded by an async generator, and mapper callbacks `await Promise.resolve()` before returning `{ id, value: id * 2 }`. These workloads isolate async iterator protocol and operator scheduling overhead without timer or network I/O waits.

Each library/workload pair executes in an isolated child process in a seeded shuffled order (`--seed=42`). Imports, input preparation, warmups, and manual GC occur outside timed regions. Every iteration validates output order, values, item counts, source closure, task completion, and concurrency/read-ahead bounds (`capacity + 1` including the item delivered to the consumer).

## Reproducing the benchmarks

```sh
pnpm install --frozen-lockfile

# Run the default benchmark suite (10,000 items) and write JSON results:
pnpm bench --json=/tmp/mogami-bench.json

# Run a single workload with 100,000 items:
pnpm bench --case=async-pipeline --items=100000

# Build, run 100,000-item suites on both Node.js and Bun, and render the SVG chart (requires Bun and uv):
pnpm bench:svg

# Re-render the chart from existing JSON results without re-running benchmarks:
pnpm bench:svg --reuse
```

See [`benchmarks/README.md`](https://github.com/45deg/mogami-stream/blob/main/benchmarks/README.md) in the repository for all CLI flags (`--items`, `--concurrency`, `--capacity`, `--warmup`, `--samples`, `--timeout-ms`) and JSON schema details.
