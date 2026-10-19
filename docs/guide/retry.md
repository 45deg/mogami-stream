# Retries

Use the `retry` option on `map` or `mapSettled` to retry individual failed items.

<<< @/examples/retry.ts

`retries: 3` allows up to four attempts including the initial call. `shouldRetry` selects which errors to retry, and `delayMs` sets the wait. The example waits 200, 400, and 800 ms based on the failed attempt number.

An item's `index` stays fixed while `attempt` increases from 1. `shouldRetry` supports synchronous or asynchronous decisions; returning `false` ends retrying. `delayMs` can also be a fixed number of milliseconds.

## Concurrency, cancellation, and rate limits

Retry decisions and backoff keep the item's concurrency slot occupied. Cancellation aborts backoff. Upstream read errors and cleanup errors are never retried.

An upstream `rateLimit` limits upstream reads separately for each execution, including reads that fail or observe completion. It does not count retry attempts inside `map`, or combine separate executions. If every external call must share a request budget, put a shared limiter inside the processing function.

## Sharing a request budget across executions

This application recipe admits one operation at a time with a minimum start interval. Create one limiter for the request budget and reuse it across executions. Each retry calls `limiter.run` again and joins the same queue:

<<< @/examples/shared-rate-limiter.ts#usage

The two executions share a minimum 100 ms start interval and at most eight waiting callers. `processJob` must start its I/O immediately and pass `signal` to that I/O. The limiter controls starts; it does not wait for I/O to finish before admitting another call. Each execution's `map` concurrency still limits its own in-flight items.

### Limiter contract

- Waiting callers are admitted in FIFO order. A retry joins the tail; it has no priority over another execution. Cancelled callers are removed without changing the remaining order.
- `maxWaiting` bounds only callers awaiting admission, excluding the granted caller and running I/O. Overflow rejects with `LimiterQueueFullError`, which the example does not retry. Zero allows only immediate admission.
- There is no burst credit. The first call can start immediately; idle time and delayed timers never allow catch-up bursts. The interval is measured with `performance.now()` and checked again when a timer fires.
- Cancellation while queued rejects with `signal.reason`, removes the listener, and frees queue space without consuming a permit. Cancellation after a grant is checked again before calling the operation. That granted permit remains consumed, as do permits for failed operations.
- No timer remains when the queue empties, and a granted caller no longer has a queue abort listener. Once I/O starts, its own signal handling governs cancellation.

<<< @/examples/shared-rate-limiter.ts#limiter

`intervalMs` must be finite and positive; `maxWaiting` must be a non-negative safe integer. The limiter belongs to the application, is not exported by `mogami-stream`, and shares state only among callers holding that instance in the same JavaScript realm. Separate processes or workers need coordination outside this recipe.

### API choice

This tested recipe is the current choice. It makes the queue, cancellation, and no-burst policy explicit without expanding the stream API into a general request scheduler. A built-in limiter would also need decisions about provider-specific windows, burst budgets, and shared ownership. Keep `rateLimit` for per-execution upstream read limits; use this helper when this fixed-interval policy matches the external service's request budget.

## After the final failure

`map` stops the execution with the last error. `mapSettled` emits `{ status: "rejected", reason }` and continues processing other items. Upstream failures and cancellation still stop either execution.

[RetryOptions](/mogami-stream/api/interfaces/RetryOptions.html){target="_self"} · [MapOptions](/mogami-stream/api/interfaces/MapOptions.html){target="_self"}
