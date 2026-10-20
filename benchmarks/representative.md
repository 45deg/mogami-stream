# Representative timer workloads

`representative.mjs` complements the Promise-only overhead benchmark with seeded timer delays, jitter, slow consumers, retries, cancellation, and larger concurrency/rate windows. These are synthetic stand-ins for asynchronous waits; they do not measure network, disk, or production service behavior.

```sh
pnpm build
node --expose-gc benchmarks/representative.mjs --json=/tmp/representative.json
# Single profile or measurement settings (defaults shown):
node --expose-gc benchmarks/representative.mjs --case=large-unordered --samples=3 --warmup=1 --seed=42 --sampleMs=5 --timeoutMs=15000 --json=/tmp/unordered.json
```

Options use `--name=value`; unknown/duplicate options and invalid numbers fail. Profile loads are explicit in [`lib/representative.mjs`](lib/representative.mjs), and every report embeds the full selected loads. Keep the same load and seed when comparing runs. `--timeoutMs` guards each iteration, and an independent parent watchdog kills stuck worker processes, including synchronous starvation.

## Workloads and comparisons

| Profile                | Load                                             | Behavior                                                                                                                  |
| ---------------------- | ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------- |
| `delayed-ordered`      | 2,048 inputs, concurrency 64, capacity 128       | Mapper timer 2–8 ms, deterministic jitter, ordered output                                                                 |
| `slow-sink`            | Same load and mapping                            | Sink awaits a 1 ms timer for every output                                                                                 |
| `large-unordered`      | 16,384 inputs, concurrency 1,024, capacity 2,048 | Mapper timer 2–8 ms, completion order; sink waits 2 ms every 128 outputs                                                  |
| `retry`                | 2,048 inputs, concurrency 64, capacity 128       | Ordered; every fifth ID fails on attempt 1, waits 2 ms and retries once with fresh deterministic 2–8 ms work              |
| `cancel-cooperative`   | Up to 4,096 inputs, concurrency 64, capacity 128 | 40–50 ms work; abort 5 ms after all 64 callbacks overlap; callback timers observe their signal; source cleanup waits 5 ms |
| `cancel-uncooperative` | Same cancellation load                           | Work timers ignore their signal; cleanup must wait for outstanding callbacks                                              |
| `large-rate-window`    | 30,000 inputs; count 10,000 per 100 ms           | Sequential `rateLimit`, including the final upstream read that reports done                                               |

The source is a lazy async generator of numeric IDs. Successful mapped outputs are `{ id, value: id * 2 }`. Delay is a pure function of seed, ID, and attempt; execution order cannot change it. Every output checks its value, ID range, uniqueness, and order when required. Successful runs check item/attempt totals, actual maximum overlap, source closure, drained mapper work, and bounds. Cancellation runs assert the primary reason, no delivered items, closed source, zero remaining callbacks, and the expected number of aborted timers. Rate admission is independently timestamped in the source; its oracle allows 1 ms skew because admission and observation occur at different times. The strict observed window maximum is also reported and can exceed the configured count at a boundary; it is not hidden by the tolerance.

Only `delayed-ordered`, `slow-sink`, and `retry` compare Mogami with `pMapIterable`. They use the same source, payload, timers, sink, seed, concurrency, validation, and numeric capacity/backpressure. Retry in the comparison callback implements the same one-retry policy while holding its mapper slot through the delay. Both adapters assert `read-ahead <= capacity + 1` (allowing for one item in the sink); because their exact slot accounting differs, the observed maxima are shown directly. Cancellation, unordered output, and rate windows are Mogami-only profiles. The JSON report records the installed `p-map` version (`7.1.1`) and its entrypoint SHA-256 alongside the workload parameters.

## Measurement and limits

Each scenario/library pair runs in a fresh process, executed sequentially in a seeded shuffled order. The default is one full warmup and three measured iterations with the same seed and optional manual GC before each iteration. Timing covers pipeline construction, consumption, validation, instrumentation, timer waits, and cleanup; module imports, process startup, oracle allocation, and GC happen outside the timed region.

`maxActive` counts callback attempts actually overlapping, rather than the configured limit. Retry slots remain occupied during backoff even though active-attempt count decreases. `maxAhead` is source values yielded minus values received by the sink; `maxReady` is successful callback completions minus sink receipts. Transition assertions enforce logical boundedness independently of the memory sampler. These external counters do not expose internal queue length, reserved pending reads, or exact retained bytes.

`memory.runtimeApi` records the runtime, version, `process.memoryUsage` method, per-field status, baseline, independently sampled peak, and peak-minus-baseline for `heapUsed`, `heapTotal`, RSS, external memory, and ArrayBuffers. The sampler runs every 5 ms by default and once after cleanup; timer starvation and GC can hide transient peaks, and fields may peak at different times. RSS includes runtime code, allocator behavior, libraries, and instrumentation, and need not fall after cleanup. Preallocated uniqueness and timestamp oracles scale with input count and are included in the baseline rather than retained stream outputs. Logical item counters (`maxActive`, `maxAhead`, `maxReady`) are the primary evidence of boundedness; sampled process memory is observational.

`memory.os` records RSS from `ps -o rss= -p PID` on macOS/Linux before the workload and after cleanup, outside elapsed-time measurement. Values are converted from KiB to bytes. These two observations are not OS peak measurements or simultaneous API/OS snapshots. An unavailable command or unsupported platform produces `status: "unavailable"`, a reason, and `rssBytes: null`.

API fields that are missing or invalid are `null` with `unavailable` status; reported zero remains zero. Bun's `external` and `arrayBuffers` values remain raw API reports with `unverified-accounting` status: controlled buffer allocations did not appear in these fields in the Bun 1.4.3 investigation. Do not infer zero buffer usage or aggregate them as Node-equivalent categories. Compare process RSS using the same OS method and sampling points; use retention tests separately to check whether closed executions release payloads.

For a fresh Node/Bun comparison with normal GC, run both commands without `--expose-gc`:

```sh
node benchmarks/representative.mjs --json=/tmp/representative-node.json
bun benchmarks/representative.mjs --json=/tmp/representative-bun.json
```

Cancellation measures from the actual abort callback to rejection of pending `next()` and, separately, to `iterator.closed`. The latter waits for outstanding work and the deliberately delayed source cleanup. A cancelled run delivers zero items, so its throughput is zero and should not be compared with completed workloads. These cases schedule cancellation only after full overlap and before any work timer should complete; assertions reject a run where that condition does not hold.

## Recorded run

See [`representative-results.json`](representative-results.json) for every sample, min/median/max, environment metadata, loads, seed, dependency fingerprint, execution order, logical maxima, memory observations, and worker diagnostics (recorded on 2026-10-20 JST).

Node.js v26.5.0, darwin arm64, Apple M1 Pro; seed 42; one warmup plus 3 measured iterations per pair; forced GC between iterations. Values below are median [min–max] for elapsed time, median throughput, and maxima across the three samples for counters and sampled process memory. MiB = 2²⁰ bytes.

| Profile / adapter                |      Elapsed ms [min–max] | Items/s | Active / ahead / ready | Sampled heapUsed / RSS MiB |
| -------------------------------- | ------------------------: | ------: | ---------------------: | -------------------------: |
| retry / mogami                   |    262.19 [260.43–264.48] |    7811 |         64 / 128 / 121 |               8.72 / 62.19 |
| slow-sink / p-map-iterable       | 2431.90 [2362.57–2472.40] |     842 |         64 / 129 / 128 |               7.01 / 58.27 |
| cancel-cooperative / mogami      |       11.51 [11.42–12.07] |       0 |            64 / 64 / 0 |               6.20 / 53.94 |
| retry / p-map-iterable           |    256.98 [254.24–261.04] |    7969 |         64 / 129 / 116 |               8.60 / 61.52 |
| cancel-uncooperative / mogami    |       50.79 [50.41–51.10] |       0 |           64 / 64 / 64 |               5.69 / 53.47 |
| large-rate-window / mogami       |    302.07 [301.52–302.31] |   99316 |              0 / 1 / 0 |               9.03 / 73.97 |
| delayed-ordered / p-map-iterable |    171.17 [169.21–172.23] |   11965 |          64 / 129 / 80 |               8.63 / 61.52 |
| large-unordered / mogami         |    321.03 [312.55–324.08] |   51036 |      1024 / 1024 / 916 |              19.35 / 88.64 |
| delayed-ordered / mogami         |    171.19 [170.77–171.34] |   11963 |          64 / 128 / 76 |               8.58 / 61.58 |
| slow-sink / mogami               | 2414.79 [2388.19–2480.40] |     848 |         64 / 128 / 127 |               7.09 / 58.50 |

| Cancellation profile | Abort → next settlement ms, median [min–max] | Abort → closed ms, median [min–max] |
| -------------------- | -------------------------------------------: | ----------------------------------: |
| cancel-cooperative   |                          0.446 [0.425–0.642] |                 6.020 [5.809–6.264] |
| cancel-uncooperative |                          0.142 [0.127–0.223] |              45.034 [44.598–45.037] |

The rate profile records 30,001 upstream starts including `done`. Its strict observed 100 ms window maximum was 10,000, and the independent 99 ms oracle stayed within 10,000. Because observation timestamps can cross a window boundary relative to admission, both the strict maximum and the 1 ms tolerance oracle are reported. Cooperative cancellation finishes in ~11.5 ms, so the 5 ms sampler captures only a few memory snapshots during that profile.

## Queue design notes

The current `completionOrder.shift()` and rate-window `starts.shift()` array queues are retained. Even when `large-unordered` reaches 1,024 concurrent callbacks and `large-rate-window` cycles through 10,000-entry windows, timer dispatch and callback execution dominate elapsed time. Replacing these arrays with a ring buffer or head-index queue would add compaction complexity without measurable gain unless a workload is specifically CPU-bound on queue shifts.

## Tests

Run `node --test --test-timeout=10000 test/representative-benchmarks.test.mjs` to verify CLI option validation, deterministic jitter, overlapping delayed/retry work across both adapters, cooperative and uncooperative cleanup, and terminal rate-limit admission.
