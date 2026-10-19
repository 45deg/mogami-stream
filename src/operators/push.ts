import { create, type Read, type Stream } from "../core/stream.js";
import { done, item } from "../internal/deferred.js";
import { integer } from "../internal/errors.js";

/** Live, per-execution statistics for intentional overflow drops. No values are retained. */
export interface PushDropInfo {
  /** Cumulative drops, saturating at Number.MAX_SAFE_INTEGER; remains readable after close. */
  readonly count: number;
  /** Policy responsible for these drops. Failure and cancellation discards are excluded. */
  readonly overflow: "drop-oldest" | "drop-newest";
  /** Signal belonging to the execution that owns this count. */
  readonly signal: AbortSignal;
}

/** Queue limits for push sources, applied independently to each execution. */
export interface PushOptions {
  /** Maximum buffered values; must be a positive safe integer. */
  readonly capacity: number;
  /** Full-queue policy. Defaults to `error`; event producers cannot be blocked. */
  readonly overflow?: "error" | "drop-oldest" | "drop-newest";
  /**
   * Synchronous overflow notification after updating the queue. Receives one live
   * statistics object per execution. Reentrant drops update its count without
   * recursively notifying. Throws fail the execution (or are suppressed after
   * cancellation/failure). Synchronous only: return undefined. JavaScript return
   * values, including promises, are not inspected, awaited, or managed.
   */
  readonly onDrop?: (info: PushDropInfo) => undefined;
}

/** Callbacks supplied to a push producer for one subscription. */
export interface PushObserver<T> {
  /**
   * Returns false when this value is rejected or the subscription has ended.
   * Reports acceptance before onDrop runs: drop-oldest returns true even if the
   * hook subsequently drops that value, fails, or cancels the execution.
   */
  next(value: T): boolean;
  /** Fails the execution immediately, discarding buffered values. */
  error(reason: unknown): void;
  /** Ends production; buffered values are delivered before completion. */
  complete(): void;
  /** Aborts when the consumer stops the subscription. */
  readonly signal: AbortSignal;
}

/**
 * Converts a callback producer to a bounded stream. Subscribes on the first read
 * of each execution. The returned cleanup runs once on completion, failure, or
 * cancellation. Synchronous emissions during subscribe count toward capacity.
 * No producer calls are awaited: use an explicit drop policy for lossy events.
 * @category Sources
 */
export function fromCallback<T>(
  subscribe: (
    observer: PushObserver<T>,
  ) => void | (() => void | PromiseLike<void>),
  options: PushOptions,
): Stream<T> {
  const { capacity, overflow = "error", onDrop } = options;
  integer(capacity, "capacity");
  if (!["error", "drop-oldest", "drop-newest"].includes(overflow))
    throw new TypeError("Unknown push overflow policy.");
  if (onDrop !== undefined && typeof onDrop !== "function")
    throw new TypeError("onDrop must be a function.");
  return create<T>((scope) => {
    // Allocate only occupied slots, even when capacity is a large safe integer.
    const queue = new Map<number, T>();
    let head = 0;
    let size = 0;
    let ended = false;
    let dropCount = 0;
    let notifyingDrop = false;
    let dropInfo: PushDropInfo | undefined;
    const reportDrop = (policy: PushDropInfo["overflow"]) => {
      if (!onDrop) return;
      dropCount = Math.min(dropCount + 1, Number.MAX_SAFE_INTEGER);
      if (notifyingDrop) return;
      dropInfo ??= Object.freeze({
        get count() {
          return dropCount;
        },
        overflow: policy,
        signal: scope.signal,
      });
      notifyingDrop = true;
      try {
        onDrop(dropInfo);
      } catch (error) {
        // Preserve an earlier cancellation or failure as the primary reason.
        scope.reportError(error);
      } finally {
        notifyingDrop = false;
      }
    };
    let pending:
      | { receive: Parameters<Read<T>>[0]; reject: Parameters<Read<T>>[1] }
      | undefined;
    let cleanup: void | (() => void | PromiseLike<void>);
    const unsubscribe = () => {
      const callback = cleanup;
      cleanup = undefined;
      return callback?.();
    };
    const clear = () => {
      queue.clear();
      head = 0;
      size = 0;
    };
    const abort = () => {
      ended = true;
      clear();
      const waiting = pending;
      pending = undefined;
      waiting?.reject(scope.signal.reason);
    };
    scope.signal.addEventListener("abort", abort, { once: true });
    scope.finalizers.push(() => {
      scope.signal.removeEventListener("abort", abort);
      clear();
      return unsubscribe();
    });
    const observer: PushObserver<T> = {
      signal: scope.signal,
      next(value) {
        if (ended) return false;
        if (pending) {
          const { receive } = pending;
          pending = undefined;
          receive(item(value));
          return true;
        }
        const droppingOldest = size === capacity && overflow === "drop-oldest";
        if (size === capacity) {
          if (overflow === "drop-newest") {
            reportDrop("drop-newest");
            return false;
          }
          if (overflow === "error") {
            observer.error(new RangeError("Push source capacity exceeded."));
            return false;
          }
          queue.delete(head);
          head = (head + 1) % capacity;
          size--;
        }
        // Avoid adding two potentially large safe integers.
        const tail =
          size >= capacity - head ? size - (capacity - head) : head + size;
        queue.set(tail, value);
        size++;
        if (droppingOldest) reportDrop("drop-oldest");
        return true;
      },
      error(reason) {
        if (ended) return;
        ended = true;
        clear();
        scope.host.fail(reason);
      },
      complete() {
        if (ended) return;
        ended = true;
        const waiting = pending;
        pending = undefined;
        waiting?.receive(done());
      },
    };
    cleanup = subscribe(observer);
    scope.setCleanup(unsubscribe);
    const read: Read<T> = (receive, reject) => {
      if (size > 0) {
        const value = queue.get(head)!;
        queue.delete(head);
        head = (head + 1) % capacity;
        size--;
        receive(item(value));
      } else if (ended) receive(done());
      else pending = { receive, reject };
    };
    return { read, next: () => new Promise(read) };
  });
}

/**
 * Converts EventTarget events (including WebSocket events) to a bounded stream.
 * Each execution installs one listener on first demand and removes it on close.
 * Events do not complete automatically; use take(), cancellation, or fromCallback
 * when the source has a separate completion event.
 * @category Sources
 */
export function fromEvent<T extends Event = Event>(
  target: EventTarget,
  type: string,
  options: PushOptions,
): Stream<T> {
  return fromCallback<T>((observer) => {
    const listener = (event: Event) => {
      observer.next(event as T);
    };
    target.addEventListener(type, listener);
    return () => target.removeEventListener(type, listener);
  }, options);
}
