# Timing

Use timing operators to control read rates, output spacing, and time limits. All durations are in milliseconds. No waiting or timing begins until the stage is read.

## Choose an operator

| Operator                                                                       | Goal                             |
| ------------------------------------------------------------------------------ | -------------------------------- |
| [`rateLimit`](/mogami-stream/api/functions/rateLimit.html){target="_self"}     | Limit reads within a time window |
| [`spaceout`](/mogami-stream/api/functions/spaceout.html){target="_self"}       | Space values apart               |
| [`delayStart`](/mogami-stream/api/functions/delayStart.html){target="_self"}   | Delay the start                  |
| [`timeoutEach`](/mogami-stream/api/functions/timeoutEach.html){target="_self"} | Limit each read's wait           |
| [`deadline`](/mogami-stream/api/functions/deadline.html){target="_self"}       | Set a stage's overall time limit |
| [`bufferTime`](/mogami-stream/api/functions/bufferTime.html){target="_self"}   | Batch by time or count           |

## Limit upstream reads

`rateLimit({ count: 2, perMs: 1_000 })` allows at most two upstream read starts in any 1,000 ms window. Available slots can be used immediately, so this does not evenly space outputs.

<<< @/examples/timing/rate-limit.ts

Put `rateLimit` before `map` to pace its upstream reads. It does not directly schedule external calls inside the callback; for a request budget shared across retries and executions, use the [shared limiter recipe](./retry.md#sharing-a-request-budget-across-executions). Set simultaneous work separately with `map`'s `concurrency` option.

Each execution has its own limit. Failed reads and the final read that observes completion also count. Retry attempts inside `map` do not count; see [Retries](./retry.md#concurrency-cancellation-and-rate-limits) for limits that must include retries.

## Space outputs apart

`spaceout(500)` adds no delay to the first value, then ensures at least 500 ms between emissions. Time spent reading upstream or processing downstream counts toward the gap. If 500 ms has already elapsed, no additional wait is added.

<<< @/examples/timing/spacing.ts

With immediately available input and continuous downstream demand, approximate times are:

| Value | `rateLimit` read start | `spaceout` emission |
| ----- | ---------------------- | ------------------- |
| 1     | 0 ms                   | 0 ms                |
| 2     | 0 ms                   | 500 ms              |
| 3     | 1,000 ms               | 1,000 ms            |
| 4     | 1,000 ms               | 1,500 ms            |
| 5     | 2,000 ms               | 2,000 ms            |

`spaceout` reads the value from upstream before waiting to emit it. Use `rateLimit` when you need to limit how often upstream work starts.

## Delay only the start

`delayStart(500)` waits 500 ms after the first read request before opening upstream. It does not add a delay to every value.

<<< @/examples/timing/delay-start.ts

## Per-read timeouts and overall deadlines

`timeoutEach` measures only the wait for one upstream read and fails with `TimeoutError` at the deadline. Time between downstream requests does not count. When a read returns a value or completes the stream, monotonic elapsed time is checked even if the timer callback has not run. Results at or after the deadline are rejected. `timeoutEach(0)` fails on the first read without opening upstream.

`deadline` measures elapsed time from that stage's first read and fails with `DeadlineExceededError` at the deadline. Time continues to count while downstream processes a value.

<<< @/examples/timing/timeouts.ts

Values arrive 800 ms apart in this example, so each read fits within its 1,000 ms limit. The overall 1,500 ms deadline expires before the third value arrives. The source is replayable, so both executions start from the beginning.

The operator times its own upstream. Placing `timeoutEach` after `map` limits the wait for the next result from `map`. To limit every individual request running concurrently, also set a timeout on the I/O inside the callback.

A timeout signals upstream to stop. Pass the callback's `signal` to I/O, as in `fetch(url, { signal })`. Terminal consumers wait for cleanup, so I/O that ignores cancellation can delay settlement beyond the deadline. See [Cancellation and cleanup](./lifecycle.md).

## Batch by time or count

`bufferTime` emits an array when `windowMs` elapses after its first item arrives or when it reaches `maxItems`.

<<< @/examples/timing/buffer.ts

This example reaches the two-item limit for each full batch and emits the remaining item on normal completion. Empty arrays are never emitted. Errors and cancellation discard the unfinished batch.

If an upstream read is still pending when the window expires, only that read is retained. No further reads or batches accumulate until downstream requests the next batch. Use [`chunks`](../operators.md#operator-chunks) to batch only by count.

Actual times follow the host runtime's timer precision. `rateLimit`'s `perMs` and `bufferTime`'s `windowMs` must be positive; other durations must be finite and non-negative. `count` and `maxItems` must be positive safe integers.
