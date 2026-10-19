# Cancellation and cleanup

## Stopping an execution

Acquiring a [`StreamIterator`](/mogami-stream/api/interfaces/StreamIterator.html){target="_self"} with [`iterate(source, { signal })`](/mogami-stream/api/functions/iterate.html){target="_self"} gives you explicit control over how an execution stops and how its teardown is observed:

| Method / Property         | Behavior                                                                                                                               |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `iterator.cancel(reason)` | Requests an immediate stop and aborts stage `AbortSignal`s. Any pending `next()` rejects right away without waiting for cleanup.       |
| `iterator.return()`       | Requests a stop, aborts stage `AbortSignal`s, and waits for all upstream cleanup and finalizers to complete before resolving.          |
| `iterator.throw(error)`   | Terminates the execution with `error` and awaits cleanup (does not forward `.throw()` into upstream generators).                       |
| `iterator.closed`         | Promise that always resolves with a [`CloseReport`](/mogami-stream/api/types/CloseReport.html){target="_self"} once teardown finishes. |

Only one `next()` call may be pending on a `StreamIterator` at a time; calling `next()` concurrently rejects with `TypeError`. When an execution fails or is cancelled, `next()` rejects once with the primary reason, and all subsequent `next()` calls resolve to `{ done: true, value: undefined }`.

<<< @/examples/lifecycle.ts

### Cooperative cancellation via `AbortSignal`

JavaScript async iterators cannot forcibly preempt an arbitrary `await`. When `cancel()`, `return()`, `take(n)`, or an external `AbortSignal` stops a stage, `mogami-stream` aborts the `signal` passed into `defer` and operator callbacks (`map`, `filter`, `forEach`, etc.).

Always pass that `signal` into cancellable APIs (`fetch`, `fs.readFile`, database queries, timers). If an upstream read or callback ignores `signal` and never settles, `return()`, `closed`, and terminal consumers will continue waiting for that operation to finish before completing teardown.

## Owning resources during asynchronous setup

`defer` factories and `catchError` handlers receive a `ResourceContext` with `signal` and `acquire(acquire, release)`. Use `acquire` when setup opens a cursor, session, file, or another resource before returning its source:

<<< @/examples/resource-ownership.ts

Call and await `context.acquire` while the factory or handler is running. The acquisition callback receives the stage's signal. If the stage has already stopped when acquisition would start, the callback is skipped. If acquisition succeeds, the stage registers `release(resource)` **before checking cancellation**, so a resource arriving after cancellation is still released. In that case the `acquire` promise rejects with the cancellation reason and the source is not opened.

The stage calls each registered release exactly once, after its child sources close and its in-flight work settles, on completion, failure, or early termination. Releases run in reverse registration order and are awaited by `closed` and terminal consumers. A blocked acquisition or release therefore keeps teardown pending. The context cannot start new acquisitions after the factory or handler settles.

Acquisition failure fails the owning stage; an upstream `defer` failure can be handled by `catchError`. The acquisition callback remains responsible for any partially allocated resources when it throws. A release failure follows the cleanup error contract: it cannot replace an existing cancellation or primary failure, and appears in `suppressedErrors`. Without an earlier failure or cancellation, a release failure makes the execution fail.

Ownership applies to each successful `acquire` call. The returned source borrows that resource; it must not release it again in its iterator's `return()` or `finally`. The example's `rows` generator closes independently, and the registered callback closes the cursor afterward. Do not register the same resource twice. To adapt an already-open iterator, register its `return()` as the release callback and expose its `next()` through a borrowing source instead of returning that same iterator directly.

Ordinary returned sources retain their existing contract: the library owns an iterator only after opening the source. A plain source returned after cancellation is neither inspected nor opened nor closed, because obtaining its iterator could start unrelated work. Resources allocated directly by a factory or handler remain that callback's responsibility until ownership is explicitly registered. A lazy generator can instead acquire inside its own `try/finally` on the first read, as shown in [Getting started](./getting-started.md#replayability-from-vs-defer).

## Inspecting `CloseReport`

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

Built-in terminal consumers (`toArray`, `forEach`, `reduce`, `find`, `some`, `every`) always wait for cleanup before settling and prioritize the primary processing error over subsequent cleanup errors. Awaiting `iterator.closed` by itself only observes teardown—it does not trigger cancellation.

## Finalizer order and `take(n)` teardown

[`finalize(effect)`](/mogami-stream/api/functions/finalize.html){target="_self"} registers a synchronous or asynchronous teardown callback on a stage:

- The callback runs **at most once per execution**, and only if the stage was actually started (i.e., received at least one `next()` read).
- It runs **after** all upstream child stages have closed and all in-flight operations in the stage have settled.
- When multiple `finalize` stages are nested in a pipeline, they execute **from inner (upstream) stages outward (downstream)**.

`take(n)` begins closing its upstream stage as soon as the `n`th item arrives—before delivering that `n`th item to downstream—while keeping downstream callback signals active so downstream operators can still process the final item.

## Recovery boundaries with `catchError`

[`catchError(handler)`](/mogami-stream/api/functions/catchError.html){target="_self"} recovers from an upstream acquisition or processing failure by switching once to a fallback source:

1. When upstream fails, `catchError` first waits for all in-flight upstream work and upstream `finalize` callbacks to finish.
2. It then invokes `handler(error, { signal, acquire })`, which may return a fallback source or a promise of one. Resources acquired during recovery follow the same ownership contract as `defer` setup above.
3. `catchError` intentionally **does not catch**:
   - Execution-level cancellation (`cancel()`, `return()`, or external `AbortSignal`),
   - Errors thrown during upstream cleanup (`return()` or `finalize`),
   - Errors thrown by `handler` or by the fallback source itself (chain a second `catchError` downstream if the fallback also needs recovery),
   - Errors originating downstream of `catchError`.

[StreamIterator](/mogami-stream/api/interfaces/StreamIterator.html){target="_self"} · [CloseReport](/mogami-stream/api/types/CloseReport.html){target="_self"} · [finalize](/mogami-stream/api/functions/finalize.html){target="_self"} · [catchError](/mogami-stream/api/functions/catchError.html){target="_self"}
