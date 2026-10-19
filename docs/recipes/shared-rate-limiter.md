# Sharing a request budget across executions

To share a request budget across multiple executions and retry attempts, create a single limiter instance and call `limiter.run` inside `map`:

<<< @/examples/shared-rate-limiter.ts#usage

The two executions share a minimum 100 ms start interval and at most eight waiting callers.

`processJob` must start its I/O immediately and pass `signal` to that I/O. The limiter paces start times; it does not wait for in-flight I/O to finish before admitting the next call. Each execution's `map` concurrency still bounds its own in-flight items.

## Queue and cancellation

- Waiting callers are admitted in FIFO order. A retry joins the tail of the queue without priority over other executions, and cancelled callers are removed without disturbing the remaining order.
- `maxWaiting` bounds callers awaiting admission (excluding the granted caller and running I/O). Exceeding it rejects with `LimiterQueueFullError`, which the example does not retry. Setting `maxWaiting: 0` allows only immediate admission.
- No burst credit accumulates: the first call starts immediately, and idle periods or delayed timers never trigger catch-up bursts. Intervals are measured with `performance.now()` and verified when the timer fires.
- Cancellation while queued rejects with `signal.reason`, removes the abort listener, and frees the queue slot without consuming a permit. Cancellation is checked again just before invoking a granted operation; once granted, the permit remains consumed even if the caller aborts or the operation fails.
- No timer remains active once the queue empties, and granted callers have their queue abort listener removed before I/O begins.

## Limiter implementation

<<< @/examples/shared-rate-limiter.ts#limiter

`intervalMs` must be finite and positive; `maxWaiting` must be a non-negative safe integer. State is shared only among callers holding the same limiter instance in memory; coordinating across separate processes or worker threads requires an external store.

Use this pattern when an external service enforces a minimum start interval across callers. For per-execution upstream read pacing without retries, use [`rateLimit`](../guide/timing.md). See [Retries](../guide/errors.md#retry) for retry configuration.
