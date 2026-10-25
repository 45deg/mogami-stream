import { slotQueue } from "./slot-queue.js";
import { defaultScheduler } from "../core/scheduler.js";
import { Scope, type ExecutionHost } from "../core/scope.js";
import {
  open,
  type OwnedReader,
  type Source,
  type OperatorContext,
} from "../core/stream.js";
import type {
  Branch,
  BranchGroup,
  BranchGroupCloseReport,
} from "../core/branch.js";
import { deferred, done, item } from "./deferred.js";
import { cancellationReason } from "./errors.js";
import type { CloseReport } from "../terminal/iterate.js";
import type { BranchLayout } from "./branch-layout.js";
import { RouteError } from "../core/route-error.js";

export interface Routing<T> {
  select(value: T, context: OperatorContext): number | PromiseLike<number>;
  readonly defaultIndex: number;
  readonly fallback: "error" | "ignore";
}

type Endpoint<T> = {
  queue: ReturnType<typeof slotQueue<T>>;
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

/** Shared endpoint lifecycle for broadcast and single-destination routing. */
export function createBranchHub<T>(
  source: Source<T>,
  layout: BranchLayout,
  capacity: number,
  signal?: AbortSignal,
  routing?: Routing<T>,
): BranchGroup<readonly T[]> {
  const count = layout.keys.length;
  const endpoints: Endpoint<T>[] = Array.from({ length: count }, () => ({
    queue: routing ? new Map<number, T>() : slotQueue<T>(capacity),
    head: 0,
    tail: 0,
    subscribed: false,
    notified: false,
    completion: deferred<CloseReport>(),
  }));
  const completion = deferred<BranchGroupCloseReport<readonly T[]>>();
  let input: Source<T> | undefined = source;
  let reader: OwnedReader<T> | undefined;
  let phase: "idle" | "running" | "draining" | "stopping" | "closed" = "idle";
  let active = count;
  let flight = false;
  let classifying = false;
  let inputIndex = 0;
  let routed: { value: T; target: number; redirected: boolean } | undefined;
  let scheduled = false;
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
      branches: layout.shape(
        endpoints.map((b) => b.report!),
      ) as readonly CloseReport[],
    });
  }

  function closeSource(): void {
    if (phase === "stopping") {
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

  function takeQueued(b: Endpoint<T>): IteratorResult<T> {
    const value = b.queue.get(b.head)!;
    b.queue.delete(b.head);
    b.head = (b.head + 1) % capacity;
    return item(value);
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
    routed = undefined;
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
    routed = undefined;
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
    // Discard-only routes can synchronously reenter wake from an internal read.
    // Queue the pump to bound the stack and accept cancellation (route.test.mjs).
    queueMicrotask(pump);
  }

  function deliver(b: Endpoint<T>, value: T): void {
    if (b.pending && !b.queue.size) {
      const pending = b.pending;
      b.pending = undefined;
      pending.resolve(item(value));
    } else {
      b.queue.set(b.tail, value);
      b.tail = (b.tail + 1) % capacity;
    }
  }

  function deliverRouted(): void {
    if (!routed || phase !== "running") return;
    let target = endpoints[routed.target];
    if (!target || target.report) {
      if (!routed.redirected) {
        routed.redirected = true;
        routed.target = routing!.defaultIndex;
        target = endpoints[routed.target];
      }
      if (!target || target.report) {
        routed = undefined;
        if (routing!.fallback === "error")
          failSource(
            new RouteError("No active route is available for the item."),
          );
        return;
      }
    }
    if (target.queue.size >= capacity) return;
    const value = routed.value;
    routed = undefined;
    deliver(target, value);
  }

  function classify(value: T): void {
    classifying = true;
    root.operationStarted();
    const selected = (target: number) => {
      classifying = false;
      if (phase !== "running") return;
      routed = { value, target, redirected: false };
      deliverRouted();
      if (endpoints.some((b) => b.pending)) wake();
    };
    const rejected = (error: unknown) => {
      classifying = false;
      root.reportError(error);
    };
    try {
      const target = routing!.select(value, {
        index: inputIndex++,
        signal: root.signal,
      });
      if (typeof target === "number") selected(target);
      else {
        void Promise.resolve(target).then(
          (index) => {
            try {
              selected(index);
            } catch (error) {
              rejected(error);
            } finally {
              root.operationFinished();
            }
          },
          (error) => {
            try {
              rejected(error);
            } finally {
              root.operationFinished();
            }
          },
        );
        return;
      }
    } catch (error) {
      rejected(error);
    }
    root.operationFinished();
  }

  function pump(): void {
    scheduled = false;
    if (phase === "stopping" || phase === "closed") return;
    for (const b of endpoints) {
      if (b.report) continue;
      if (b.pending && b.queue.size) {
        const pending = b.pending;
        b.pending = undefined;
        pending.resolve(takeQueued(b));
      }
      if (phase === "draining" && !b.queue.size) end(b, "completed");
    }
    finishGroup();
    if (phase !== "idle" && phase !== "running") return;
    if (routed) deliverRouted();
    if (phase !== "idle" && phase !== "running") return;
    if (
      flight ||
      classifying ||
      routed ||
      !active ||
      !endpoints.some((b) => !b.report && b.pending) ||
      (!routing && endpoints.some((b) => !b.report && b.queue.size >= capacity))
    )
      return;
    // Broadcast reserves an endpoint slot in every active branch. Routing uses
    // its single coordinator slot until the destination can accept the item.
    flight = true;
    phase = "running";
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
          } else if (routing) {
            classify(result.value);
          } else {
            for (const b of endpoints) {
              if (!b.report) deliver(b, result.value);
            }
          }
          // Classification wakes on completion; do not pump once more while
          // an asynchronous selector is still pending.
          if (!classifying && endpoints.some((b) => b.pending)) wake();
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

  const handles: Branch<T>[] = endpoints.map((b) => {
    const branch: Branch<T> = {
      closed: b.completion.promise,
      detach: () => depart(b, "completed"),
      cancel: (reason = cancellationReason()) => depart(b, "cancelled", reason),
      [Symbol.asyncIterator]() {
        return branch.iterate();
      },
      iterate(options = {}) {
        if (b.subscribed)
          throw new TypeError(
            "A branch permits only one iterator acquisition.",
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
            if (b.queue.size) {
              const result = takeQueued(b);
              if (phase === "draining" && !b.queue.size) {
                end(b, "completed");
                finishGroup();
              }
              // A dequeue can unblock a sibling waiting behind this branch's capacity.
              if (routed || endpoints.some((endpoint) => endpoint.pending))
                wake();
              return Promise.resolve(result);
            }
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
  const group: BranchGroup<readonly T[]> = {
    branches: Object.freeze(layout.shape(handles)) as readonly Branch<T>[],
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
