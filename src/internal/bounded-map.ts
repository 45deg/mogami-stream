import {
  create,
  open,
  type Operator,
  type OperatorContext,
  type Read,
} from "../core/stream.js";
import { done, item } from "./deferred.js";
import { integer } from "./errors.js";

export interface MapOptions {
  readonly concurrency?: number;
  readonly capacity?: number;
  readonly ordered?: boolean;
}

/** One slot needs no result queue or background input pump. */
function sequentialMap<T, U>(
  mapper: (value: T, context: OperatorContext) => U,
): Operator<T, Awaited<U>> {
  return (source) =>
    create((scope) => {
      const upstream = open(source, scope);
      let index = 0;
      return {
        async next() {
          const result = await upstream.next();
          if (result.done) return done();
          scope.check();
          const value = await mapper(result.value, {
            index: index++,
            signal: scope.signal,
          });
          return item(value);
        },
      };
    });
}

/** A slot stays occupied from the input reservation until its output is consumed. */
export function boundedMap<T, U>(
  mapper: (value: T, context: OperatorContext) => U,
  options: MapOptions = {},
): Operator<T, Awaited<U>> {
  const concurrency = options.concurrency ?? 1;
  const capacity = options.capacity ?? concurrency;
  integer(concurrency, "concurrency");
  integer(capacity, "capacity");
  if (capacity < concurrency)
    throw new RangeError("capacity must be >= concurrency.");
  const ordered = options.ordered ?? true;
  if (concurrency === 1 && capacity === 1) return sequentialMap(mapper);

  return (source) =>
    create((scope) => {
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
          let value: Awaited<U>;
          try {
            const mapped = mapper(result.value, {
              index,
              signal: scope.signal,
            });
            pump();
            value = await mapped;
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
