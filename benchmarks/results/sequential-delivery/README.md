# Sequential map/filter optimization

Sequential `map` (`concurrency: 1, capacity: 1`) and `filter` stages use the internal callback-based `read` delivery path instead of allocating intermediate `next()` wrapper Promises at every stage boundary. Callback `await` points, cancellation checkpoints, retry behavior, and scope operation tracking remain unchanged, as does the concurrent mapper implementation.

## Diagnosis

CPU profiling of repeated JSON pipelines showed time concentrated in sequential `map` and nested `next()` Promise wrappers across `Stream` and `filter` stages. Counting Promise allocations via `async_hooks` over 10,000 JSON records (6,666 outputs collected with `toArray`) showed **196,715 Promise initializations before and 113,378 after**—a **42.4% reduction**.

To reproduce the allocation count or CPU profile on a built checkout:

```sh
pnpm build
node benchmarks/profile-sync.mjs
node benchmarks/profile-sync.mjs /absolute/path/to/baseline/dist/index.js
node --cpu-prof --cpu-prof-dir=/tmp benchmarks/profile-sync.mjs
```

Tree hashes and environment details are recorded in [`diagnosis.json`](diagnosis.json). Note that `async_hooks` should only be enabled for allocation counting, not during timed benchmark runs.

## End-to-end results

Measured on Node v24.22.0 with forced GC between runs, isolated child processes per case, 5 warmups, 15 samples, and seed 42 (JSON inputs: 10,000; I/O inputs: 128; concurrency: 8; capacity: 16; base I/O and slow-sink delay: 2 ms). Values below are median wall-clock times in milliseconds:

| Workload          | Before (ms) | After (ms) | Change |
| ----------------- | ----------: | ---------: | -----: |
| `records-stream`  |       14.10 |      11.02 | -21.9% |
| `records-batch`   |       14.12 |      11.51 | -18.5% |
| `io-serial`       |      469.51 |     486.48 |  +3.6% |
| `io-batch`        |       64.53 |      62.43 |  -3.3% |
| `io-stream`       |       63.44 |      65.90 |  +3.9% |
| `slow-sink`       |      313.46 |     311.75 |  -0.5% |
| `early-stop`      |        2.62 |       2.67 |  +1.8% |
| `cancel-inflight` |        2.63 |       2.62 |  -0.5% |

Raw samples and native baseline controls are in [`before.json`](before.json) and [`after.json`](after.json). Reproduce with:

```sh
pnpm bench --warmup=5 --samples=15 --seed=42 --json=/tmp/result.json
```

## Larger input confirmation

Confirmed with 100,000 JSON records (10 warmups, 21 samples, seed 43), alternating before/after order between streaming and batch runs:

| Workload         | Before (ms) | After (ms) | Change |
| ---------------- | ----------: | ---------: | -----: |
| `records-stream` |       90.76 |      68.98 | -24.0% |
| `records-batch`  |       95.99 |      76.16 | -20.7% |

Raw samples: [`before-stream-100k.json`](before-stream-100k.json), [`after-stream-100k.json`](after-stream-100k.json), [`before-batch-100k.json`](before-batch-100k.json), [`after-batch-100k.json`](after-batch-100k.json).

```sh
pnpm bench --case=records-stream --items=100000 --warmup=10 --samples=21 --seed=43 --json=/tmp/stream.json
pnpm bench --case=records-batch --items=100000 --warmup=10 --samples=21 --seed=43 --json=/tmp/batch.json
```
