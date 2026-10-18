# Higher-order deduplication and cancellation review

Baseline commit: `afca7516f752c49a6cb6ac353cc7daa4dbb347b7` (measured on Node v24.22.0 on the same local machine, with no external I/O).

Summary of changes evaluated:

- Sequential flattening (`concurrency: 1` for `flatten`, `concat`, `flatMap`, and `concatMap`) shares a single internal `read` delivery loop, preserving the Promise boundary and awaited child cleanup.
- `project` reuses a single callback implementation for both `read` and `next()` entry points.
- `tap` checks for cancellation before invoking its side-effect callback.

## Measurement

Each `(case, version)` pair ran sequentially in a fresh child process with an explicit GC before each timed iteration. Timing covers pipeline construction, child-array allocation where applicable, consumption, and cleanup; output assertions run after the timer stops.

An initial pass used 5 warmups and 15 samples; a repeat pass used 8 warmups and 21 samples with reversed execution order. The table below shows median times (ms) from the repeat pass over 10,000 inputs. Full raw samples for both passes are recorded in [`results.json`](results.json).

| Case                | Before (ms) | After (ms) | Change |
| ------------------- | ----------: | ---------: | -----: |
| tap                 |        7.08 |       6.45 |  -8.9% |
| reduce              |        8.31 |       9.19 | +10.6% |
| flatMap             |       13.99 |      14.13 |  +1.0% |
| concatMap           |       16.12 |      15.34 |  -4.8% |
| flatten             |       16.99 |      15.80 |  -7.0% |
| concat              |       17.06 |      14.85 | -12.9% |
| flatMapEmpty        |       11.52 |      11.09 |  -3.7% |
| flatMapLarge        |        4.63 |       4.75 |  +2.7% |
| flatMapConcurrent   |       18.49 |      18.11 |  -2.1% |
| concatMapConcurrent |       20.91 |      21.42 |  +2.5% |

Single-item sequential `concatMap`, `flatten`, and `concat` improved in both passes. Short sub-10 ms runs (`tap` and `reduce`) fluctuated between passes (`tap`: +7.1% then -8.9%; `reduce`: +0.6% then +10.6%). Follow-up runs at 100,000 inputs (8 warmups, 21 samples, alternating order) stayed within typical run-to-run variance (`tap`: +8.4% / +1.4%, `reduce`: -0.3% / +2.4%, `flatMap`: +0.5% / +4.9%, `flatMapLarge`: +0.4% / +4.3%).

## Reproduction

Build both the baseline and current `dist/` outputs, then run `benchmarks/profile-higher-order.mjs` in separate processes:

```sh
node --expose-gc benchmarks/profile-higher-order.mjs /absolute/path/to/baseline/index.js flatMap
node --expose-gc benchmarks/profile-higher-order.mjs dist/index.js flatMap
BENCH_ITEMS=100000 node --expose-gc benchmarks/profile-higher-order.mjs dist/index.js tap
```

`profile-higher-order.mjs` accepts any case name from the table above and defaults to 10,000 inputs, 8 warmups, and 21 samples (with `concurrency: 8` for the concurrent cases).
