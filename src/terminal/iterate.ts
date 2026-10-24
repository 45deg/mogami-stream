import { open, type Source } from "../core/stream.js";
import { Scope, type ExecutionHost } from "../core/scope.js";
import { defaultScheduler, type Scheduler } from "../core/scheduler.js";
import { cancellationReason } from "../internal/errors.js";
import { deferred, done } from "../internal/deferred.js";

/**
 * Options for executing a stream pipeline.
 */
export interface ExecutionOptions {
  /** External `AbortSignal` to cancel the execution. If already aborted, the source is never opened. */
  readonly signal?: AbortSignal | undefined;
}
/**
 * Summary of how a stream execution terminated and whether any secondary errors occurred during teardown.
 */
export type CloseReport = {
  /** Distinct secondary processing or cleanup errors, excluding the primary reason. Repeated values are deduplicated with SameValueZero equality (as in Array.includes). */
  readonly suppressedErrors: readonly unknown[];
} & (
  | { readonly kind: "completed" }
  | {
      readonly kind: "cancelled";
      /** Cancellation reason passed to `cancel()` or `signal.abort()` (may be `undefined`). */
      readonly reason: unknown;
    }
  | {
      readonly kind: "failed";
      /** Primary error that caused the execution or its cleanup to fail (may be `undefined`). */
      readonly reason: unknown;
    }
);
/**
 * An `AsyncIterableIterator` with explicit cancellation control and a `closed` lifecycle promise.
 * Completion covers work owned by this iterator. A branch iterator owns only its endpoint;
 * shared source work and cleanup belong to its `BranchGroup`.
 */
export interface StreamIterator<T> extends AsyncIterableIterator<
  T,
  unknown,
  undefined
> {
  /** Immediately requests cancellation without waiting for cleanup. Track teardown completion via {@link closed}. */
  cancel(reason?: unknown): void;
  /** Stops the owned execution and waits for its cleanup before resolving `{ done: true, value: undefined }`. Rejects with the primary reason if the execution failed, including cleanup-only failure. After cancellation, resolves even if cleanup fails; inspect closed for suppressed errors. */
  return(value?: unknown): Promise<IteratorResult<T, unknown>>;
  /** Requests failure without forwarding the throw into the upstream iterator and rejects immediately, without waiting for cleanup. Preserves an existing failure or cancellation reason; await closed for teardown completion. */
  throw(error?: unknown): Promise<IteratorResult<T, unknown>>;
  /** Resolves with a {@link CloseReport} once owned upstream iterators and finalizers have finished. Branch iterators resolve when their endpoint finishes, without waiting for shared source cleanup. Never rejects, and awaiting it does not trigger cancellation. */
  readonly closed: Promise<CloseReport>;
}

/** Internal test seam: Scheduler is deliberately absent from the package entry point. */
export function iterateWithScheduler<T>(
  source: Source<T>,
  options: ExecutionOptions,
  scheduler: Scheduler,
): StreamIterator<T> {
  const completion = deferred<CloseReport>();
  let wakePending: (() => void) | undefined;
  const suppressedErrors: unknown[] = [];
  let kind: CloseReport["kind"] | undefined;
  let reason: unknown;
  let notified = false;
  let pending = false;
  let cleanupFinished = false;
  let closing: Promise<void> | undefined;
  let reader: ReturnType<typeof open<T>> | undefined;

  const host: ExecutionHost = {
    scheduler,
    fail(error) {
      stop("failed", error);
    },
    suppress(error) {
      if (
        (kind === "failed" || kind === "cancelled") &&
        Object.is(error, reason)
      )
        return;
      if (kind === undefined || kind === "completed") {
        kind = "failed";
        reason = error;
        root.stop(error);
        wakePending?.();
        void close();
      } else if (!suppressedErrors.includes(error)) {
        suppressedErrors.push(error);
      }
    },
  };
  const root = new Scope(host);
  const signal = options.signal;
  const abort = () => stop("cancelled", signal!.reason);
  signal?.addEventListener("abort", abort, { once: true });

  function close(): Promise<void> {
    closing ??= (async () => {
      await root.close();
      // Closed iterators no longer need stage buffers or acquired inputs.
      reader = undefined;
      signal?.removeEventListener("abort", abort);
      kind ??= "completed";
      cleanupFinished = true;
      completion.resolve(
        kind === "completed"
          ? { kind, suppressedErrors: [...suppressedErrors] }
          : { kind, reason, suppressedErrors: [...suppressedErrors] },
      );
    })();
    return closing;
  }

  function stop(nextKind: CloseReport["kind"], error: unknown): void {
    if (kind !== undefined) {
      if (
        nextKind === "failed" &&
        (kind === "completed" || !Object.is(error, reason)) &&
        !Object.is(error, root.signal.reason)
      )
        host.suppress(error);
      return;
    }
    kind = nextKind;
    reason = error;
    root.stop(error);
    wakePending?.();
    void close();
  }

  function stoppedResult(): IteratorResult<T, unknown> {
    if (!notified && (kind === "failed" || kind === "cancelled")) {
      notified = true;
      throw reason;
    }
    return done();
  }

  const iterator: StreamIterator<T> = {
    closed: completion.promise,
    [Symbol.asyncIterator]() {
      return this;
    },
    cancel(error = cancellationReason()) {
      stop("cancelled", error);
    },
    next() {
      if (pending)
        return Promise.reject(
          new TypeError("Only one next() may be pending at a time."),
        );
      if (signal?.aborted && kind === undefined) abort();
      if (kind !== undefined) {
        try {
          return Promise.resolve(stoppedResult());
        } catch (error) {
          return Promise.reject(error);
        }
      }
      pending = true;
      return new Promise<IteratorResult<T, unknown>>((resolve, reject) => {
        let settled = false;
        const settle = (result: IteratorResult<T, unknown>) => {
          // A cancelled read may finish after a later public next().
          if (settled) return;
          settled = true;
          pending = false;
          wakePending = undefined;
          try {
            resolve(kind === undefined ? result : stoppedResult());
          } catch (error) {
            reject(error);
          }
        };
        const failed = (error: unknown) => {
          if (kind === undefined) stop("failed", error);
          settle(done());
        };
        // Register before opening: source acquisition may synchronously stop us.
        wakePending = () => settle(done());
        try {
          reader ??= open(source, root);
          if (settled) return;
          reader.read((result) => {
            if (settled) return;
            try {
              if (kind === undefined && result.done) {
                // Keep wakePending active while normal exhaustion awaits cleanup.
                void close().then(() => settle(result));
              } else settle(result);
            } catch (error) {
              failed(error);
            }
          }, failed);
        } catch (error) {
          failed(error);
        }
      });
    },
    async return() {
      if (kind === undefined) {
        kind = "completed";
        root.stop(cancellationReason());
        wakePending?.();
      }
      await close();
      if (kind === "failed") throw reason;
      return done();
    },
    async throw(error) {
      if (cleanupFinished) throw kind === "completed" ? error : reason;
      stop("failed", error);
      notified = !pending;
      throw reason;
    },
  };
  if (signal?.aborted) abort();
  return iterator;
}

/**
 * Creates a controlled iterator for `source` with explicit cancellation and a `closed` completion report.
 *
 * Execution is lazy and starts on the first `next()` call. Only one `next()` call may be pending at a time. If the execution fails or is cancelled, `next()` rejects once with the reason, and subsequent `next()` calls resolve to `{ done: true, value: undefined }`. Call `return()` or `cancel()` when stopping manual iteration early; awaiting `closed` by itself does not stop the stream.
 * @category Consumption
 */
export function iterate<T>(
  source: Source<T>,
  options: ExecutionOptions = {},
): StreamIterator<T> {
  return iterateWithScheduler(source, options, defaultScheduler);
}
