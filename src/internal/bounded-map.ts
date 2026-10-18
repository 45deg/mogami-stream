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

export interface MapContext extends OperatorContext {
  /** One-based attempt, including when retry is omitted. */
  readonly attempt: number;
}

export interface BoundedMapOptions {
  readonly concurrency?: number;
  readonly capacity?: number;
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
      return {
        async next() {
          const result = await upstream.next();
          if (result.done) return done();
          scope.check();
          const context: MapContext = {
            index: index++,
            signal: scope.signal,
            attempt: 1,
          };
          let value: Awaited<U>;
          try {
            value = await mapper(result.value, context);
          } catch (error) {
            if (!recover) throw error;
            value = await recover(error, result.value, context);
          }
          return item(value);
        },
      };
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
      const results = new Map<number, Awaited<U>>();
      const completionOrder: number[] = [];
      let waiter:
        | {
            receive: Parameters<Read<Awaited<U>>>[0];
            reject: Parameters<Read<Awaited<U>>>[1];
          }
        | undefined;
      scope.signal.addEventListener("abort", deliver, { once: true });
      scope.finalizers.push(() =>
        scope.signal.removeEventListener("abort", deliver),
      );
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
        if (pumping) return;
        pumping = true;
        try {
          // A ready upstream stage may deliver synchronously. Refill iteratively
          // so a large capacity never turns those callbacks into recursion.
          while (
            demand &&
            !inputDone &&
            !pulling &&
            !failure &&
            !scope.signal.aborted &&
            running < concurrency &&
            running + results.size < capacity
          ) {
            pulling = true;
            checkBounds();
            scope.operationStarted();
            upstream.read(
              (result) => {
                void run(result);
              },
              (error) => {
                pulling = false;
                try {
                  fail(error);
                } finally {
                  scope.operationFinished();
                }
              },
            );
          }
        } finally {
          pumping = false;
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
          if (scope.signal.aborted) return;
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
            signal: scope.signal,
            attempt: 1,
          };
          let value: Awaited<U>;
          try {
            try {
              const mapped = mapper(result.value, context);
              pump();
              value = await mapped;
            } catch (error) {
              if (!recover) throw error;
              const recovered = recover(error, result.value, context);
              pump();
              value = await recovered;
            }
          } finally {
            running--;
          }
          results.set(index, value);
          if (!ordered) completionOrder.push(index);
          checkBounds();
          deliver();
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
          const index = ordered ? nextOutputIndex : completionOrder[0];
          if (index !== undefined && results.has(index)) {
            result = item(results.get(index)!);
            results.delete(index);
            if (ordered) nextOutputIndex++;
            else completionOrder.shift();
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
