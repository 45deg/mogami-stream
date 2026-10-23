import { defaultScheduler } from "./scheduler.js";
import { Scope, type ExecutionHost } from "./scope.js";
import { open, type OwnedReader, type Source } from "./stream.js";
import { deferred, done, item } from "../internal/deferred.js";
import { cancellationReason } from "../internal/errors.js";
import type {
  CloseReport,
  ExecutionOptions,
  StreamIterator,
} from "../terminal/iterate.js";

export interface ForkOptions extends ExecutionOptions {
  /** Fixed number of one-shot branches; a positive safe integer. */
  readonly branches: number;
  /** Maximum queued values and reserved reads per branch; a positive safe integer. */
  readonly capacity: number;
}

export interface ForkBranch<T> extends AsyncIterable<T> {
  [Symbol.asyncIterator](): StreamIterator<T>;
  /** Acquires this endpoint once. The signal cancels only this branch. */
  iterate(options?: ExecutionOptions): StreamIterator<T>;
  /** Releases unread values immediately, without waiting for shared cleanup. */
  detach(): void;
  cancel(reason?: unknown): void;
  /** Endpoint completion, independent of siblings and shared source cleanup. */
  readonly closed: Promise<CloseReport>;
}

export interface ForkCloseReport {
  readonly source: CloseReport;
  readonly branches: readonly CloseReport[];
}

export interface Fork<T> {
  readonly branches: readonly ForkBranch<T>[];
  cancel(reason?: unknown): void;
  /** Detaches all endpoints and waits for source work and cleanup. Never rejects. */
  close(): Promise<ForkCloseReport>;
  /** Waits for every endpoint and shared cleanup; does not initiate a stop. */
  readonly closed: Promise<ForkCloseReport>;
}

type Endpoint<T> = {
  queue: Map<number, T>;
  head: number;
  tail: number;
  subscribed: boolean;
  notified: boolean;
  report?: CloseReport;
  completion: ReturnType<typeof deferred<CloseReport>>;
  pending?:
    | {
        resolve: (value: IteratorResult<T>) => void;
        reject: (error: unknown) => void;
      }
    | undefined;
  removeSignal?: (() => void) | undefined;
};

/**
 * Shares one lazy source execution across fixed, bounded, one-shot branches.
 * A full branch stops source reads; detach unused branches to release capacity.
 * Consume branches concurrently and inspect `closed` for shared cleanup failures.
 * Bounds and their product must be positive safe integers. Values are shared by reference.
 * @category Sources
 */
export function fork<T>(source: Source<T>, options: ForkOptions): Fork<T> {
  const { branches: count, capacity, signal } = options;
  if (
    !Number.isSafeInteger(count) ||
    count <= 0 ||
    !Number.isSafeInteger(capacity) ||
    capacity <= 0 ||
    !Number.isSafeInteger(count * capacity)
  ) {
    throw new RangeError(
      "branches, capacity, and their product must be positive safe integers.",
    );
  }
  const endpoints: Endpoint<T>[] = Array.from({ length: count }, () => ({
    queue: new Map(),
    head: 0,
    tail: 0,
    subscribed: false,
    notified: false,
    completion: deferred<CloseReport>(),
  }));
  const completion = deferred<ForkCloseReport>();
  let input: Source<T> | undefined = source;
  let reader: OwnedReader<T> | undefined;
  let phase: "idle" | "running" | "draining" | "stopping" | "closed" = "idle";
  let active = count;
  let flight = false;
  let scheduled = false;
  let reads = 0;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let closing = false;
  let sourceReport: CloseReport | undefined;
  let sourceKind: CloseReport["kind"] | undefined;
  let sourceReason: unknown;
  const suppressed: unknown[] = [];
  const host: ExecutionHost = {
    scheduler: defaultScheduler,
    fail: failSource,
    suppress: failSource,
  };
  const root = new Scope(host);
  const abort = () => stop("cancelled", signal!.reason);

  function finishGroup(): void {
    if (active || !sourceReport || phase === "closed") return;
    phase = "closed";
    signal?.removeEventListener("abort", abort);
    completion.resolve({
      source: sourceReport,
      branches: endpoints.map((b) => b.report!),
    });
  }

  function closeSource(): void {
    if (phase === "stopping") {
      if (timer !== undefined) clearTimeout(timer);
      timer = undefined;
      scheduled = false;
    }
    if (closing) return;
    closing = true;
    void root.close().then(() => {
      reader = undefined;
      input = undefined;
      sourceKind ??= "completed";
      sourceReport =
        sourceKind === "completed"
          ? { kind: sourceKind, suppressedErrors: [...suppressed] }
          : {
              kind: sourceKind,
              reason: sourceReason,
              suppressedErrors: [...suppressed],
            };
      finishGroup();
    });
  }

  function settleStopped(b: Endpoint<T>): void {
    const pending = b.pending;
    if (!pending) return;
    b.pending = undefined;
    if (!b.notified && b.report!.kind !== "completed") {
      b.notified = true;
      pending.reject(b.report!.reason);
    } else pending.resolve(done());
  }

  function end(
    b: Endpoint<T>,
    kind: CloseReport["kind"],
    reason?: unknown,
  ): void {
    if (b.report) return;
    b.report =
      kind === "completed"
        ? { kind, suppressedErrors: [] }
        : { kind, reason, suppressedErrors: [] };
    active--;
    b.queue.clear();
    b.head = b.tail = 0;
    b.removeSignal?.();
    b.removeSignal = undefined;
    settleStopped(b);
    b.completion.resolve(b.report!);
  }

  function stop(kind: CloseReport["kind"], reason?: unknown): void {
    if (phase === "closed" || phase === "stopping") return;
    phase = "stopping";
    sourceKind ??= kind;
    sourceReason = reason;
    // Commit every departure before abort handlers can reenter the group.
    for (const b of endpoints) end(b, kind, reason);
    root.stop(kind === "completed" ? cancellationReason() : reason);
    closeSource();
    finishGroup();
  }

  function failSource(error: unknown): void {
    if (sourceKind === "failed" || sourceKind === "cancelled") {
      if (!Object.is(error, sourceReason) && !suppressed.includes(error))
        suppressed.push(error);
      return;
    }
    sourceKind = "failed";
    sourceReason = error;
    // Cleanup can fail after a normal stop or after some endpoints completed.
    phase = "stopping";
    for (const b of endpoints) end(b, "failed", error);
    root.stop(error);
    closeSource();
  }

  function depart(
    b: Endpoint<T>,
    kind: CloseReport["kind"],
    reason?: unknown,
  ): void {
    if (b.report) return;
    end(b, kind, reason);
    if (!active && (phase === "idle" || phase === "running")) {
      // A failed last endpoint cancels the owned source rather than throwing into it.
      stop(kind === "completed" ? "completed" : "cancelled", reason);
    } else {
      wake();
      finishGroup();
    }
  }

  function wake(): void {
    if (scheduled || phase === "stopping" || phase === "closed") return;
    scheduled = true;
    // Bound synchronous-source bursts so timers and abort handlers get a turn.
    if (reads >= 1024) {
      reads = 0;
      timer = setTimeout(pump, 0);
    } else queueMicrotask(pump);
  }

  function pump(): void {
    scheduled = false;
    timer = undefined;
    if (phase === "stopping" || phase === "closed") return;
    for (const b of endpoints) {
      if (b.report) continue;
      if (b.pending && b.queue.size) {
        const value = b.queue.get(b.head)!;
        b.queue.delete(b.head);
        b.head = (b.head + 1) % capacity;
        const pending = b.pending;
        b.pending = undefined;
        pending.resolve(item(value));
      }
      if (phase === "draining" && !b.queue.size) end(b, "completed");
    }
    finishGroup();
    if (phase !== "idle" && phase !== "running") return;
    if (
      flight ||
      !active ||
      !endpoints.some((b) => !b.report && b.pending) ||
      endpoints.some((b) => !b.report && b.queue.size >= capacity)
    )
      return;
    // The single flight reserves one slot in each active endpoint. No other
    // producer can fill those slots, and departure immediately removes its slot.
    flight = true;
    phase = "running";
    reads++;
    root.operationStarted();
    try {
      reader ??= open(input!, root);
      root.check();
      reader.read(
        (result) => {
          flight = false;
          if (phase !== "running") return;
          if (result.done) {
            phase = "draining";
            // Empty endpoints terminate before source finalizers are joined.
            for (const b of endpoints)
              if (!b.report && !b.queue.size) end(b, "completed");
            closeSource();
          } else {
            for (const b of endpoints) {
              if (b.report) continue;
              b.queue.set(b.tail, result.value);
              b.tail = (b.tail + 1) % capacity;
            }
          }
          wake();
        },
        (error) => {
          flight = false;
          if (phase === "running") failSource(error);
        },
      );
    } catch (error) {
      flight = false;
      root.reportError(error);
    } finally {
      root.operationFinished();
    }
  }

  const handles: ForkBranch<T>[] = endpoints.map((b) => {
    const branch: ForkBranch<T> = {
      closed: b.completion.promise,
      detach: () => depart(b, "completed"),
      cancel: (reason = cancellationReason()) => depart(b, "cancelled", reason),
      [Symbol.asyncIterator]() {
        return branch.iterate();
      },
      iterate(options = {}) {
        if (b.subscribed)
          throw new TypeError(
            "A fork branch permits only one iterator acquisition.",
          );
        b.subscribed = true;
        const localSignal = options.signal;
        if (localSignal && !b.report) {
          const cancel = () => branch.cancel(localSignal.reason);
          b.removeSignal = () =>
            localSignal.removeEventListener("abort", cancel);
          localSignal.addEventListener("abort", cancel, { once: true });
          if (localSignal.aborted) cancel();
        }
        return {
          closed: branch.closed,
          [Symbol.asyncIterator]() {
            return this;
          },
          cancel: branch.cancel,
          next() {
            if (b.pending)
              return Promise.reject(
                new TypeError("Only one next() may be pending at a time."),
              );
            return new Promise<IteratorResult<T>>((resolve, reject) => {
              b.pending = { resolve, reject };
              if (b.report) settleStopped(b);
              else wake();
            });
          },
          async return() {
            branch.detach();
            if (b.report!.kind === "failed") throw b.report!.reason;
            return done();
          },
          async throw(error) {
            if (b.report)
              throw b.report.kind === "completed" ? error : b.report.reason;
            depart(b, "failed", error);
            b.notified = true;
            throw error;
          },
        };
      },
    };
    return branch;
  });
  const group: Fork<T> = {
    branches: Object.freeze(handles),
    closed: completion.promise,
    cancel: (reason = cancellationReason()) => stop("cancelled", reason),
    close() {
      stop("completed");
      return completion.promise;
    },
  };
  signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) abort();
  return group;
}
