import { open, type Source } from "../core/stream.js";
import { Scope, type ExecutionHost } from "../core/scope.js";
import { defaultScheduler, type Scheduler } from "../core/scheduler.js";
import { cancellationReason } from "../internal/errors.js";
import { deferred, done } from "../internal/deferred.js";

/**
 * Options for one execution, including an optional external cancellation signal.
 */
export interface ExecutionOptions {
  /** External cancellation signal; an already aborted signal prevents input acquisition. */
  readonly signal?: AbortSignal | undefined;
}
/**
 * Cleanup outcome, preserving the primary stop reason and additional failures.
 */
export interface CloseReport {
  /** Completed, explicitly cancelled, or failed, including cleanup failure. */
  readonly kind: "completed" | "cancelled" | "failed";
  /** Primary failure or cancellation reason; absent for normal completion. */
  readonly reason?: unknown;
  /** Additional processing and cleanup failures observed after the primary reason. */
  readonly suppressedErrors: readonly unknown[];
}
/**
 * Controlled iterator whose closed promise resolves after all owned cleanup finishes.
 */
export interface StreamIterator<T> extends AsyncIterableIterator<T> {
  /** Request a stop immediately; cleanup continues through closed. */
  cancel(reason?: unknown): void;
  /** Request a stop and await cleanup; may reject on failure. */
  return(value?: unknown): Promise<IteratorResult<T>>;
  /** Stop with an error without injecting it upstream or waiting for cleanup. */
  throw(error?: unknown): Promise<IteratorResult<T>>;
  /** Always resolves after cleanup, including when cleanup fails. Observing it does not stop execution. */
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
      if (kind === "failed" && Object.is(error, reason)) return;
      if (!suppressedErrors.includes(error)) suppressedErrors.push(error);
      if (kind === undefined || kind === "completed") {
        kind = "failed";
        reason = error;
        root.stop(error);
        wakePending?.();
        void close();
      }
    },
  };
  const root = new Scope(host);
  const abort = () => stop("cancelled", options.signal!.reason);
  options.signal?.addEventListener("abort", abort, { once: true });

  function close(): Promise<void> {
    closing ??= (async () => {
      await root.close();
      options.signal?.removeEventListener("abort", abort);
      kind ??= "completed";
      cleanupFinished = true;
      completion.resolve({
        kind,
        ...(kind === "failed" || kind === "cancelled" ? { reason } : {}),
        suppressedErrors: [...suppressedErrors],
      });
    })();
    return closing;
  }

  function stop(nextKind: CloseReport["kind"], error: unknown): void {
    if (kind !== undefined) {
      if (
        nextKind === "failed" &&
        !Object.is(error, reason) &&
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

  function stoppedResult(): IteratorResult<T> {
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
      if (options.signal?.aborted && kind === undefined) abort();
      if (kind !== undefined) {
        try {
          return Promise.resolve(stoppedResult());
        } catch (error) {
          return Promise.reject(error);
        }
      }
      pending = true;
      return new Promise<IteratorResult<T>>((resolve, reject) => {
        let settled = false;
        const settle = (result: IteratorResult<T>) => {
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
  if (options.signal?.aborted) abort();
  return iterator;
}

/**
 * Acquire a controlled, lazy iterator with cancellation and a cleanup report.
 *
 * Only one next() may be pending. Failure or cancellation rejects one read; later reads return done. Request return() or cancel() when abandoning manual iteration; awaiting closed alone does not request a stop.
 * @category Consumption
 */
export function iterate<T>(
  source: Source<T>,
  options: ExecutionOptions = {},
): StreamIterator<T> {
  return iterateWithScheduler(source, options, defaultScheduler);
}
