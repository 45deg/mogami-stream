# Error handling

Choose whether a failure should stop execution, let other items continue, or trigger a retry.

| Policy                                                               | API                             |
| -------------------------------------------------------------------- | ------------------------------- |
| Stop when processing an item fails                                   | `map`                           |
| Receive each item's success or failure and continue with other items | `mapSettled`                    |
| Retry a failed item                                                  | `retry` on `map` / `mapSettled` |
| Switch to a fallback source when upstream fails                      | `catchError`                    |

## Stop or continue after an item fails {#item-errors}

`map` stops execution when its callback fails. `mapSettled` emits `{ status: "rejected", reason }` and continues processing other items. Upstream failures and cancellation still stop either execution.

<<< @/examples/operators/mapSettled.ts

In this example, the item with input 2 fails while the other items continue processing.

## Retry failed items {#retry}

Use the `retry` option on `map` or `mapSettled` to retry individual failed items.

<<< @/examples/retry.ts

`retries: 3` allows up to four attempts including the initial call. `shouldRetry` selects which errors to retry, and `delayMs` sets the wait. The example waits 200, 400, and 800 ms based on the failed attempt number.

An item's `index` stays fixed while `attempt` increases from 1. `shouldRetry` supports synchronous or asynchronous decisions; returning `false` ends retrying. `delayMs` can also be a fixed number of milliseconds.

### Concurrency, cancellation, and rate limits

Retry decisions and backoff keep the item's concurrency slot occupied. Cancellation aborts backoff. Upstream read errors and cleanup errors are never retried.

An upstream `rateLimit` limits upstream reads separately for each execution, including reads that fail or observe completion. It does not count retry attempts inside `map`, or combine separate executions. If every external call must share a request budget, put a shared limiter inside the processing function.

See the [shared rate limiter recipe](../recipes/shared-rate-limiter.md) to share a start interval and queue across executions. Retries join the same queue.

### After the final failure

Once retrying stops, `map` stops execution with the last error. `mapSettled` emits a `rejected` outcome for that item and continues with the others.

## Switch to a fallback source {#catch-error}

[`catchError(handler)`](/mogami-stream/api/functions/catchError.html){target="_self"} recovers from an upstream acquisition or processing failure by switching once to a fallback source:

<<< @/examples/operators/catchError.ts

1. When upstream fails, `catchError` first waits for all in-flight upstream work and upstream `finalize` callbacks to finish.
2. It then invokes `handler(error, { signal, acquire })`, which may return a fallback source or a promise of one. Resources acquired during recovery follow the same [resource ownership contract as `defer`](./lifecycle.md#owning-resources-during-asynchronous-setup).
3. `catchError` intentionally **does not catch**:
   - Execution-level cancellation (`cancel()`, `return()`, or external `AbortSignal`),
   - Errors thrown during upstream cleanup (`return()` or `finalize`),
   - Errors thrown by `handler` or by the fallback source itself (chain a second `catchError` downstream if the fallback also needs recovery),
   - Errors originating downstream of `catchError`.

## Inspect cleanup errors and termination {#cleanup-errors}

`iterator.closed` never rejects. Once all upstream iterators and `finalize` callbacks have finished, it resolves to a discriminated union:

```ts
type CloseReport =
  | {
      readonly kind: "completed";
      readonly suppressedErrors: readonly unknown[];
    }
  | {
      readonly kind: "cancelled";
      readonly reason: unknown;
      readonly suppressedErrors: readonly unknown[];
    }
  | {
      readonly kind: "failed";
      readonly reason: unknown;
      readonly suppressedErrors: readonly unknown[];
    };
```

- **`reason`** (on `"cancelled"` and `"failed"` reports) holds the primary cancellation reason or the first unhandled error that triggered termination.
- **`suppressedErrors`** collects any secondary errors thrown by in-flight operations or cleanup callbacks after the primary termination outcome was already decided.

These rules also apply to release callbacks registered with `acquire`. A release failure cannot replace an existing cancellation or primary failure and appears in `suppressedErrors`. Without an earlier cancellation or failure, a cleanup error makes the execution fail and becomes `reason`; it is not also added to `suppressedErrors`.

Errors matching the primary reason via `Object.is` are excluded. Repeated secondary errors are recorded once using `Array.includes` equality (SameValueZero): object identity for objects, with repeated `NaN` and `undefined` values deduplicated. Errors remain in first-observed order.

Built-in terminal consumers (`toArray`, `forEach`, `reduce`, `find`, `some`, `every`) always wait for cleanup before settling and prioritize the primary processing error over subsequent cleanup errors. Awaiting `iterator.closed` by itself only observes teardown; it does not trigger cancellation. Call [`return()` or `cancel()`](./lifecycle.md#stopping-an-execution) when stopping manual iteration early.

See the [terminal result and report recipe](../recipes/terminal-report.md) to receive results together with secondary errors.

[RetryOptions](/mogami-stream/api/interfaces/RetryOptions.html){target="_self"} · [MapOptions](/mogami-stream/api/interfaces/MapOptions.html){target="_self"} · [catchError](/mogami-stream/api/functions/catchError.html){target="_self"} · [CloseReport](/mogami-stream/api/types/CloseReport.html){target="_self"}
