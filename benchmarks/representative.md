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

The source is a lazy async generator of numeric IDs. Successful mapped outputs are `{ id, value: id * 2 }`. Delay is a pure function of seed, ID, and attempt; execution order cannot change it. Every output checks its value, ID range, uniqueness, and order when required. Successful runs check item/attempt totals, actual maximum overlap, source closure, drained mapper work, and bounds. Cancellation runs assert the primary reason, no delivered items, closed source, zero remaining callbacks, and the expected number of aborted timers. Each rate admission occurs between a timestamp taken before the downstream request and another taken inside the source. For reads separated by `count` positions, the oracle rejects a window if the later source observation is less than `perMs` after the earlier request. This detects definite violations without assuming a fixed delay between admission and observation. The strict observed window maximum is also reported and can exceed the configured count when observations are delayed. Virtual-clock tests check exact admission boundaries separately.

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

See [`representative-results.json`](representative-results.json) for every sample, min/median/max, environment metadata, loads, seed, dependency fingerprint, execution order, logical maxima, memory observations, and worker diagnostics (recorded on 2026-10-29 JST).

Node.js v26.13.0, darwin arm64, Apple M1 Pro; seed 42; one warmup plus 3 measured iterations per pair; forced GC between iterations. Values below are median [min–max] for elapsed time, median throughput, and maxima across the three samples for counters and sampled process memory. MiB = 2²⁰ bytes.

| Profile / adapter                |      Elapsed ms [min–max] | Items/s | Active / ahead / ready | Sampled heapUsed / RSS MiB |
| -------------------------------- | ------------------------: | ------: | ---------------------: | -------------------------: |
| retry / mogami                   |    272.90 [266.42–301.59] |    7504 |         64 / 128 / 122 |               9.13 / 73.09 |
| slow-sink / p-map-iterable       | 2613.04 [2486.18–2632.27] |     784 |         64 / 129 / 128 |               7.40 / 68.86 |
| cancel-cooperative / mogami      |       11.85 [10.71–11.92] |       0 |            64 / 64 / 0 |               6.38 / 63.67 |
| retry / p-map-iterable           |    255.14 [254.71–259.10] |    8027 |         64 / 129 / 119 |               8.91 / 72.17 |
| cancel-uncooperative / mogami    |       50.38 [50.12–50.80] |       0 |           64 / 64 / 64 |               6.21 / 63.27 |
| large-rate-window / mogami       |    301.72 [301.14–302.61] |   99430 |              0 / 1 / 0 |               9.83 / 84.22 |
| delayed-ordered / p-map-iterable |    168.97 [168.43–170.19] |   12121 |          64 / 129 / 76 |               8.93 / 72.41 |
| large-unordered / mogami         |    312.88 [310.03–314.03] |   52365 |      1024 / 1024 / 825 |              20.67 / 97.39 |
| delayed-ordered / mogami         |    171.32 [170.13–172.22] |   11954 |          64 / 128 / 76 |               8.98 / 72.61 |
| slow-sink / mogami               | 2373.68 [2369.28–2376.09] |     863 |         64 / 128 / 127 |               7.47 / 69.00 |

| Cancellation profile | Abort → next settlement ms, median [min–max] | Abort → closed ms, median [min–max] |
| -------------------- | -------------------------------------------: | ----------------------------------: |
| cancel-cooperative   |                          0.258 [0.256–0.301] |                 5.564 [5.563–5.629] |
| cancel-uncooperative |                          0.151 [0.120–0.151] |              44.646 [44.009–45.354] |

The rate profile records 30,001 upstream starts including `done`. Its strict observed 100 ms window maximum ranged from 10,000 to 10,001. Cooperative cancellation finishes in ~11.9 ms, so the 5 ms sampler captures only a few memory snapshots during that profile.

## Queue design notes

Completion indices and rate-window timestamps use head indices with periodic compaction. Active child streams keep their rotation order in a linked list, so retiring a child does not move its siblings. The timer workloads above measure these queues together with timer dispatch and callback execution.

## Tests

Run `node --test --test-timeout=10000 test/representative-benchmarks.test.mjs` to verify CLI option validation, deterministic jitter, overlapping delayed/retry work across both adapters, cooperative and uncooperative cleanup, and terminal rate-limit admission.
