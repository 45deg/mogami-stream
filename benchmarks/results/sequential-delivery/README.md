# Sequential map/filter optimization

The sequential `map` and `filter` stages now use the existing internal `read` delivery protocol instead of awaiting an upstream `next()` through every stage. This removes intermediate Promise wrappers while keeping the callback `await`, cancellation checkpoints, retry behavior, and scope operation tracking. The concurrent mapper and its ordering/capacity algorithm are unchanged.

## Diagnosis

A Node CPU profile of repeated JSON pipelines showed time concentrated in sequential mapping and the nested `next()` wrappers in the stream and filter stages. A separate `async_hooks` pass over 10,000 JSON records (6,666 outputs, collected with `toArray`) counted **196,715 Promise initializations before and 113,378 after**, a **42.4% reduction**. These are diagnostic counts, not timings or heap measurements. The diagnostic does not include the workload harness's common sink and assertions.

Reproduce on a built checkout, optionally supplying a saved baseline distribution:

```sh
pnpm build
node benchmarks/profile-sync.mjs
node benchmarks/profile-sync.mjs /absolute/path/to/baseline/dist/index.js
node --cpu-prof --cpu-prof-dir=/tmp benchmarks/profile-sync.mjs
```

The source-tree hashes and diagnostic conditions are recorded in [diagnosis.json](diagnosis.json). The baseline is the working-tree snapshot taken immediately before this optimization, including existing local changes; it is not Git HEAD.

## End-to-end results

Node v24.22.0, forced GC between runs, separate processes per case/implementation, warmup 5, samples 15, seed 42. JSON inputs: 10,000; I/O inputs: 128; concurrency 8; capacity 16; base I/O wait and slow-sink wait: 2 ms. All values below are median wall-clock **milliseconds**, including the common sink, per-item validation, instrumentation, and awaited cleanup. Negative changes mean less elapsed time.

| Workload          | Before |  After | Change |
| ----------------- | -----: | -----: | -----: |
| `records-stream`  |  14.10 |  11.02 | -21.9% |
| `records-batch`   |  14.12 |  11.51 | -18.5% |
| `io-serial`       | 469.51 | 486.48 |  +3.6% |
| `io-batch`        |  64.53 |  62.43 |  -3.3% |
| `io-stream`       |  63.44 |  65.90 |  +3.9% |
| `slow-sink`       | 313.46 | 311.75 |  -0.5% |
| `early-stop`      |   2.62 |   2.67 |  +1.8% |
| `cancel-inflight` |   2.63 |   2.62 |  -0.5% |

The timer-driven I/O and cleanup rows vary by a few percent, including rows whose implementation did not change. These measurements do not establish a performance improvement or regression in those cases. Both runs preserved the observed maximum concurrency of 8 and streaming read-ahead of 16, and all output/cleanup assertions passed. Exact order, bounds, and cancellation are also covered by deterministic tests; timing results alone do not prove these guarantees.

Full raw samples and native controls: [before.json](before.json), [after.json](after.json). Reproduce each revision with:

```sh
pnpm bench --warmup=5 --samples=15 --seed=42 --json=/tmp/result.json
```

## Larger input confirmation

100,000 JSON records, warmup 10, samples 21, seed 43. Streaming was measured before then after; batch was measured after then before, to check the opposite temporal order.

| Workload         | Before | After | Change |
| ---------------- | -----: | ----: | -----: |
| `records-stream` |  90.76 | 68.98 | -24.0% |
| `records-batch`  |  95.99 | 76.16 | -20.7% |

Raw samples and native controls: [stream before](before-stream-100k.json), [stream after](after-stream-100k.json), [batch before](before-batch-100k.json), [batch after](after-batch-100k.json).

```sh
pnpm bench --case=records-stream --items=100000 --warmup=10 --samples=21 --seed=43 --json=/tmp/stream.json
pnpm bench --case=records-batch --items=100000 --warmup=10 --samples=21 --seed=43 --json=/tmp/batch.json
```

The improvement repeated at both sizes, but the measurements are environment-specific and do not establish statistical significance. Native processing still has lower overhead.

## Correctness and runtime validation

- `pnpm check`: 193 Node tests, type checks, distribution audit, formatting.
- `pnpm test:bun`: 193 tests.
- Chromium main thread, Dedicated Worker, Service Worker: 23 shared contracts each.
- Local Cloudflare Workers/workerd: 23 shared contracts.
- Added deterministic tests for queued cancellation during filtering, cancellation between synchronous maps, draining a predicate's late failure, Promise-valued items, and ordered concurrent delivery with bounded read-ahead and no idle refill.
