# Retries

Use the `retry` option on `map` or `mapSettled` to retry individual failed items.

<<< @/examples/retry.ts

`retries: 3` allows up to four attempts including the initial call. `shouldRetry` selects which errors to retry, and `delayMs` sets the wait. The example waits 200, 400, and 800 ms based on the failed attempt number.

An item's `index` stays fixed while `attempt` increases from 1. `shouldRetry` supports synchronous or asynchronous decisions; returning `false` ends retrying. `delayMs` can also be a fixed number of milliseconds.

## Concurrency, cancellation, and rate limits

Retry decisions and backoff keep the item's concurrency slot occupied. Cancellation aborts backoff. Upstream read errors and cleanup errors are never retried.

An upstream `rateLimit` does not count retry attempts inside `map`. If every attempt must share a request budget, use a shared limiter inside the processing function.

## After the final failure

`map` stops the execution with the last error. `mapSettled` emits `{ status: "rejected", reason }` and continues processing other items. Upstream failures and cancellation still stop either execution.

[RetryOptions](/mogami-stream/api/interfaces/RetryOptions.html){target="_self"} · [MapOptions](/mogami-stream/api/interfaces/MapOptions.html){target="_self"}
