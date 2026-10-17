import { open, type Source } from "../core/stream.js";
import { Scope, type ExecutionHost } from "../core/scope.js";
import { defaultScheduler, type Scheduler } from "../core/scheduler.js";
import { cancellationReason } from "../internal/errors.js";
import { deferred, done } from "../internal/deferred.js";

export interface ExecutionOptions {
  readonly signal?: AbortSignal | undefined;
}
export interface CloseReport {
  readonly kind: "completed" | "cancelled" | "failed";
  readonly reason?: unknown;
  readonly suppressedErrors: readonly unknown[];
}
export interface StreamIterator<T> extends AsyncIterableIterator<T> {
  cancel(reason?: unknown): void;
  return(value?: unknown): Promise<IteratorResult<T>>;
  throw(error?: unknown): Promise<IteratorResult<T>>;
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
  let started = false;
  let closing: Promise<void> | undefined;
  let steps = 0;
  let reader: ReturnType<typeof open<T>> | undefined;

  const host: ExecutionHost = {
    scheduler,
    fail(error) {
      stop("failed", error);
    },
    suppress(error) {
      if (kind === "failed" && error === reason) return;
      if (!suppressedErrors.includes(error)) suppressedErrors.push(error);
      if (kind === undefined || kind === "completed") {
        kind = "failed";
        reason = error;
        root.stop(error);
        wakePending?.();
        void close();
      }
    },
    async checkpoint(signal) {
      if (++steps % 128 === 0) await scheduler.yield(signal);
    },
  };
  const root = new Scope(host);
  const abort = () => stop("cancelled", options.signal!.reason);
  options.signal?.addEventListener("abort", abort, { once: true });

  function close(): Promise<void> {
    closing ??= (async () => {
      await root.close();
      options.signal?.removeEventListener("abort", abort);
      completion.resolve({
        kind: kind ?? "completed",
        ...(kind === undefined ? {} : { reason }),
        suppressedErrors: [...suppressedErrors],
      });
    })();
    return closing;
  }

  function stop(nextKind: CloseReport["kind"], error: unknown): void {
    if (kind !== undefined) {
      if (
        nextKind === "failed" &&
        error !== reason &&
        error !== root.signal.reason
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
    async next() {
      if (pending)
        throw new TypeError("Only one next() may be pending at a time.");
      if (options.signal?.aborted && kind === undefined) abort();
      if (kind !== undefined) return stoppedResult();
      pending = true;
      const stopNotification = deferred<void>();
      wakePending = () => stopNotification.resolve();
      try {
        if (!started) {
          started = true;
          reader = open(source, root);
        }
        const read = reader!.next();
        // The reader observes its own failures; the race also handles synchronous open errors.
        const result = await Promise.race([
          read,
          stopNotification.promise.then(() => done()),
        ]);
        if (kind !== undefined) return stoppedResult();
        if (result.done) {
          await close();
          if (kind === "failed") return stoppedResult();
          kind = "completed";
        }
        return result;
      } catch (error) {
        if (notified) throw error;
        if (kind === undefined) stop("failed", error);
        return stoppedResult();
      } finally {
        pending = false;
        wakePending = undefined;
      }
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
      stop("failed", error);
      notified = !pending;
      throw reason;
    },
  };
  if (options.signal?.aborted) abort();
  return iterator;
}

export function iterate<T>(
  source: Source<T>,
  options: ExecutionOptions = {},
): StreamIterator<T> {
  return iterateWithScheduler(source, options, defaultScheduler);
}
