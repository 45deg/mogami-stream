# Higher-order deduplication and cancellation review

Baseline: `afca7516f752c49a6cb6ac353cc7daa4dbb347b7`. The candidate is the source change accompanying this report. Node v24.22.0 on the same local machine; no external I/O.

Sequential flattening shares the existing flatMap delivery loop, including its Promise boundary and awaited child cleanup. `project` uses one callback implementation for both reader entry points. `tap` checks cancellation before invoking effects.

## Measurement

Each case/version runs in a fresh process, sequentially, with explicit GC before each iteration and outside timing. Timing includes pipeline construction, child-array allocation where applicable, consumption, and cleanup. Output checks run after timing. The initial pass used 5 warmups and 15 samples; the repeat used 8 warmups and 21 samples and reversed each pair’s execution order. The table shows medians in milliseconds from the repeat, with 10,000 inputs. All samples, including the initial pass, are in [results.json](results.json).

| Case                | Before | After | Change |
| ------------------- | -----: | ----: | -----: |
| tap                 |   7.08 |  6.45 |  -8.9% |
| reduce              |   8.31 |  9.19 | +10.6% |
| flatMap             |  13.99 | 14.13 |  +1.0% |
| concatMap           |  16.12 | 15.34 |  -4.8% |
| flatten             |  16.99 | 15.80 |  -7.0% |
| concat              |  17.06 | 14.85 | -12.9% |
| flatMapEmpty        |  11.52 | 11.09 |  -3.7% |
| flatMapLarge        |   4.63 |  4.75 |  +2.7% |
| flatMapConcurrent   |  18.49 | 18.11 |  -2.1% |
| concatMapConcurrent |  20.91 | 21.42 |  +2.5% |

The single-item sequential concatMap, flatten, and concat cases improved in both passes. The small tap/reduce measurements varied substantially: tap changed from +7.1% in the initial pass to -8.9% in the repeat; reduce changed from +0.6% to +10.6%. These short runs do not establish a regression.

The 100,000-input confirmations used 8 warmups and 21 samples, then reversed execution order for a second pair. Changes were: tap +8.4% / +1.4%, reduce -0.3% / +2.4%, flatMap +0.5% / +4.9%, and a single large child +0.4% / +4.3%. No large, repeatable slowdown was identified; small regressions cannot be excluded by these local timings. This is a focused microbenchmark, not evidence for every workload or runtime.

## Reproduction

Save the built baseline distribution before applying the change, then build the candidate. Run each case against both distributions, using separate processes and alternating the order:

```sh
node --expose-gc benchmarks/profile-higher-order.mjs /absolute/path/to/baseline/index.js flatMap
node --expose-gc benchmarks/profile-higher-order.mjs dist/index.js flatMap
BENCH_ITEMS=100000 node --expose-gc benchmarks/profile-higher-order.mjs dist/index.js tap
```

The script supports the case names in the table and defaults to 10,000 inputs, 8 warmups, and 21 samples. The concurrent cases use eight children.

## Validation

- `pnpm check`: 208 Node tests, public type checks, distribution audit, formatting.
- `bun test test/*.test.mjs`: 208 passed.
- Chromium main thread, Dedicated Worker, and Service Worker: 23 contracts each.
- Local Cloudflare Workers/workerd: 23 contracts.
- Shared lifecycle tests cover all four higher-order operators, including Promise-valued items, no idle refill, cancellation, late failures, cleanup failure, and thousands of synchronously empty children.
- Cancellation regression coverage includes takeWhile, tap, reduce, and forEach.
