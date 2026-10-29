import type { Scope } from "../core/scope.js";
import {
  create,
  open,
  type Operator,
  type OperatorContext,
  type Read,
} from "../core/stream.js";
import { done, item } from "./deferred.js";
import { integer } from "./errors.js";

/**
 * Context passed to {@link map} and {@link mapSettled} callbacks.
 */
export interface MapContext extends OperatorContext {
  /** One-based attempt number for the current item (starts at `1` on the initial call). */
  readonly attempt: number;
}

export interface BoundedMapOptions {
  /** Maximum number of items being read or mapped at the same time. Must be a positive safe integer. Defaults to `1`. */
  readonly concurrency?: number;
  /** Maximum number of items held in the stage at once (pending reads, active mappers, and buffered outputs). Must be a safe integer `>= concurrency`. Defaults to `concurrency`. */
  readonly capacity?: number;
  /** When `true`, emits results in input order; when `false`, emits results as they complete. Defaults to `true`. */
  readonly ordered?: boolean;
}

export type MapperRecovery<T, U> = (
  error: unknown,
  value: T,
  context: MapContext,
) => U;

/** One slot needs no result queue or background input pump. */
function sequentialMap<T, U>(
  makeMapper: (scope: Scope) => (value: T, context: MapContext) => U,
  makeRecovery?: (scope: Scope) => MapperRecovery<T, U>,
): Operator<T, Awaited<U>> {
  return (source) =>
    create((scope) => {
      const mapper = makeMapper(scope);
      const recover = makeRecovery?.(scope);
      const upstream = open(source, scope);
      let index = 0;
      // Keep stage delivery on the internal read path. Awaiting the callback
      // still gives cancellation a checkpoint and retains pending-work tracking.
      const read: Read<Awaited<U>> = (receive, reject) => {
        upstream.read(async (result) => {
          try {
            if (result.done) {
              receive(done());
              return;
            }
            scope.check();
            const context: MapContext = {
              index: index++,
              signal: scope.signal,
              attempt: 1,
            };
            const input = result.value;
            // Iterator result getters can synchronously cancel the execution.
            scope.check();
            let value: Awaited<U>;
            try {
              value = await mapper(input, context);
            } catch (error) {
              if (!recover) throw error;
              value = await recover(error, input, context);
            }
            receive(item(value));
          } catch (error) {
            reject(error);
          }
        }, reject);
      };
      return { read, next: () => new Promise(read) };
    });
}

/** A slot owns an item through mapping and consumption; mappers are created per execution. */
export function scopedMap<T, U>(
  makeMapper: (scope: Scope) => (value: T, context: MapContext) => U,
  options: BoundedMapOptions = {},
  makeRecovery?: (scope: Scope) => MapperRecovery<T, U>,
): Operator<T, Awaited<U>> {
  const concurrency = options.concurrency ?? 1;
  const capacity = options.capacity ?? concurrency;
  integer(concurrency, "concurrency");
  integer(capacity, "capacity");
  if (capacity < concurrency)
    throw new RangeError("capacity must be >= concurrency.");
  const ordered = options.ordered ?? true;
  if (concurrency === 1 && capacity === 1)
    return sequentialMap(makeMapper, makeRecovery);

  return (source) =>
    create((scope) => {
      const mapper = makeMapper(scope);
      const recover = makeRecovery?.(scope);
      const upstream = open(source, scope);
      const signal = scope.signal;
      const results = new Map<number, Awaited<U>>();
      let completionOrder: number[] = [];
      let completionHead = 0;
      let waiter:
        | {
            receive: Parameters<Read<Awaited<U>>>[0];
            reject: Parameters<Read<Awaited<U>>>[1];
          }
        | undefined;
      signal.addEventListener("abort", deliver, { once: true });
      scope.finalizers.push(() => {
        signal.removeEventListener("abort", deliver);
        // Late mapper results may arrive after stop; release them after work drains.
        results.clear();
        completionOrder.length = 0;
      });
      let demand = false;
      let pulling = false;
      let pumping = false;
      let inputDone = false;
      let running = 0;
      let nextInputIndex = 0;
      let nextOutputIndex = 0;
      let failure: { error: unknown } | undefined;

      const checkBounds = () => {
        const reserved = Number(pulling);
        if (
          reserved + running > concurrency ||
          reserved + running + results.size > capacity
        ) {
          throw new Error("Internal map capacity invariant violated.");
        }
      };

      function pump(): void {
        if (pumping || !demand) return;
        pumping = true;
        try {
          // A ready upstream stage may deliver synchronously. Refill iteratively
          // so a large capacity never turns those callbacks into recursion.
          while (
            demand &&
            !inputDone &&
            !pulling &&
            !failure &&
            !signal.aborted &&
            running < concurrency &&
            running + results.size < capacity
          ) {
            pulling = true;
            checkBounds();
            scope.operationStarted();
            upstream.read(acceptInput, rejectInput);
          }
        } finally {
          pumping = false;
        }
      }

      function acceptInput(result: IteratorResult<T>): void {
        void run(result);
      }

      function rejectInput(error: unknown): void {
        pulling = false;
        try {
          fail(error);
        } finally {
          scope.operationFinished();
        }
      }

      function fail(error: unknown): void {
        failure = { error };
        scope.reportError(error);
        deliver();
      }

      // One tracked operation owns the reservation through mapper completion.
      async function run(result: IteratorResult<T>): Promise<void> {
        pulling = false;
        try {
          if (signal.aborted) return;
          if (result.done) {
            inputDone = true;
            deliver();
            return;
          }
          const index = nextInputIndex++;
          running++;
          checkBounds();
          const context: MapContext = {
            index,
            signal,
            attempt: 1,
          };
          let value: Awaited<U>;
          try {
            const input = result.value;
            scope.check();
            try {
              const mapped = mapper(input, context);
              pump();
              value = await mapped;
            } catch (error) {
              if (!recover) throw error;
              const recovered = recover(error, input, context);
              pump();
              value = await recovered;
            }
          } finally {
            running--;
          }
          if (
            waiter &&
            (!ordered || index === nextOutputIndex) &&
            !failure &&
            !signal.aborted
          ) {
            // The next result can satisfy demand without entering the ready queue.
            const pending = waiter;
            waiter = undefined;
            demand = false;
            if (ordered) nextOutputIndex++;
            checkBounds();
            pending.receive(item(value));
          } else {
            results.set(index, value);
            if (!ordered) completionOrder.push(index);
            checkBounds();
            deliver();
          }
          pump();
        } catch (error) {
          fail(error);
        } finally {
          scope.operationFinished();
        }
      }

      function deliver(): void {
        if (!waiter) return;
        let result: IteratorResult<Awaited<U>>;
        try {
          scope.check();
          if (failure) throw failure.error;
          const index = ordered
            ? nextOutputIndex
            : completionOrder[completionHead];
          if (index !== undefined && results.has(index)) {
            result = item(results.get(index)!);
            results.delete(index);
            if (ordered) nextOutputIndex++;
            else if (completionOrder.length === 1) completionOrder.pop();
            else {
              completionHead++;
              if (completionHead >= completionOrder.length - completionHead) {
                completionOrder = completionOrder.slice(completionHead);
                completionHead = 0;
              }
            }
            checkBounds();
          } else if (inputDone && running === 0) result = done();
          else return;
        } catch (error) {
          const pending = waiter;
          waiter = undefined;
          demand = false;
          pending.reject(error);
          return;
        }
        const pending = waiter;
        waiter = undefined;
        demand = false;
        pending.receive(result);
      }

      const read: Read<Awaited<U>> = (receive, reject) => {
        waiter = { receive, reject };
        demand = true;
        pump();
        deliver();
      };
      return {
        read,
        next: () => new Promise<IteratorResult<Awaited<U>>>(read),
      };
    });
}
