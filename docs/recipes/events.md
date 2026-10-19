# WebSocket and SSE

Use `fromCallback` and `fromEvent` to receive events as streams. These examples keep at most 64 queued values and drop the oldest when the queue is full.

<<< @/examples/events.ts

The WebSocket adapter handles messages, errors, and closure, then removes its listeners during cleanup. The SSE adapter receives messages until the supplied signal aborts. The caller owns and closes each connection.

Use `overflow: "error"` to fail on overflow, or `"drop-newest"` to discard incoming values instead.

## Observing dropped values

Pass a synchronous `onDrop(info)` callback to count values discarded by overflow in `fromCallback` or `fromEvent`. Each execution allocates a single frozen `PushDropInfo` object on its first drop:

- `count` is a live cumulative count of intentionally dropped values, saturating at `Number.MAX_SAFE_INTEGER`. Retaining `info` keeps the current count readable even after the execution closes; it is a live counter rather than a snapshot of one notification.
- `overflow` identifies `"drop-oldest"` (one buffered value evicted) or `"drop-newest"` (one incoming value rejected). The default `"error"` policy, buffer clears on failure or cancellation, normal completion, and calls after termination do not increment `count` or trigger `onDrop`.
- `signal` belongs to the owning execution, allowing concurrent subscriptions to be distinguished. Counters are tracked independently per execution.

### Reentrant calls

`onDrop` runs after the queue update is committed. If a reentrant producer call inside `onDrop` triggers another drop, `info.count` increments immediately without recursively invoking `onDrop` or queueing extra notifications; read `info.count` after the call to inspect the updated total. The next non-reentrant drop notifies normally. No dropped values, history, or deferred notifications are retained, and no counter is allocated when `onDrop` is omitted.

### Errors and cancellation

A synchronous throw inside `onDrop` fails the execution, clears buffered values, and runs cleanup without escaping `observer.next()`. If the execution was already cancelled or failed, that earlier reason remains primary and the new error is recorded in `closed.suppressedErrors`. `onDrop` is synchronous: TypeScript requires an `undefined` return type, and any runtime return value (including a Promise) is ignored and not awaited. Meanwhile, `observer.next()` still returns its original acceptance decision even if `onDrop` replaces the item, completes, fails, or cancels the execution; subsequent producer calls observe the termination and return `false`.

[fromCallback](/mogami-stream/api/functions/fromCallback.html){target="_self"} · [fromEvent](/mogami-stream/api/functions/fromEvent.html){target="_self"}
