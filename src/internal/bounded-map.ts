import {
  create,
  open,
  type Operator,
  type OperatorContext,
} from "../core/stream.js";
import { done, item } from "./deferred.js";
import { notifications } from "./notifications.js";
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
      const changed = notifications(scope);
      let demand = false;
      let pulling = false;
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
        if (!demand || inputDone || pulling || failure || scope.signal.aborted)
          return;
        if (running >= concurrency || running + results.size >= capacity)
          return;
        pulling = true;
        checkBounds();
        const pull = (async () => {
          const result = await upstream.next();
          pulling = false;
          if (scope.signal.aborted) return;
          if (result.done) {
            inputDone = true;
            changed.notify();
            return;
          }
          const index = nextInputIndex++;
          running++;
          checkBounds();
          const work = scope.observe(
            Promise.resolve().then(() => {
              scope.check();
              return mapper(result.value, { index, signal: scope.signal });
            }),
          );
          scope.observe(
            work.then(
              (value) => {
                running--;
                results.set(index, value);
                if (!ordered) completionOrder.push(index);
                checkBounds();
                changed.notify();
                pump();
              },
              (error) => {
                running--;
                failure = { error };
                changed.notify();
              },
            ),
          );
          pump();
        })();
        void scope.observe(pull).catch((error) => {
          pulling = false;
          failure = { error };
          changed.notify();
        });
      }

      return {
        async next() {
          demand = true;
          try {
            for (;;) {
              scope.check();
              const notification = changed.promise;
              pump();
              if (failure) throw failure.error;
              const index = ordered ? nextOutputIndex : completionOrder[0];
              if (index !== undefined && results.has(index)) {
                const value = results.get(index)!;
                results.delete(index);
                if (ordered) {
                  nextOutputIndex++;
                } else completionOrder.shift();
                checkBounds();
                return item(value);
              }
              if (inputDone && running === 0) return done();
              await notification;
            }
          } finally {
            demand = false;
          }
        },
      };
    });
}
