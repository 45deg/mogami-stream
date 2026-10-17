import {
  create,
  open,
  type Operator,
  type OperatorContext,
} from "../core/stream.js";
import { deferred, done, item } from "./deferred.js";
import { integer } from "./errors.js";

export interface MapOptions {
  readonly concurrency?: number;
  readonly capacity?: number;
  readonly ordered?: boolean;
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

  return (source) =>
    create((scope) => {
      const upstream = open(source, scope);
      const results = new Map<number, Awaited<U>>();
      const ready: number[] = [];
      let changed = deferred<void>();
      let demand = false;
      let pulling = false;
      let inputDone = false;
      let running = 0;
      let accepted = 0;
      let emitted = 0;
      let nextOrdered = 0;
      let failure: { error: unknown } | undefined;

      const notify = () => {
        const previous = changed;
        changed = deferred();
        previous.resolve();
      };
      scope.signal.addEventListener("abort", notify, { once: true });
      scope.finalizers.push(() =>
        scope.signal.removeEventListener("abort", notify),
      );
      const checkBounds = () => {
        const reserved = Number(pulling);
        if (
          reserved + running > concurrency ||
          reserved + accepted - emitted > capacity
        ) {
          throw new Error("Internal map capacity invariant violated.");
        }
      };

      function pump(): void {
        if (!demand || inputDone || pulling || failure || scope.signal.aborted)
          return;
        if (running >= concurrency || accepted - emitted >= capacity) return;
        pulling = true;
        checkBounds();
        const pull = (async () => {
          const result = await upstream.next();
          pulling = false;
          if (scope.signal.aborted) return;
          if (result.done) {
            inputDone = true;
            notify();
            return;
          }
          const index = accepted++;
          running++;
          checkBounds();
          const work = scope.observe(
            Promise.resolve().then(() => {
              scope.check();
              return mapper(result.value, { index, signal: scope.signal });
            }),
          );
          void work.then(
            (value) => {
              running--;
              results.set(index, value as Awaited<U>);
              ready.push(index);
              checkBounds();
              notify();
              pump();
            },
            (error) => {
              running--;
              failure = { error };
              notify();
            },
          );
          notify();
          pump();
        })();
        void scope.observe(pull).catch((error) => {
          pulling = false;
          failure = { error };
          notify();
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
              const index = ordered ? nextOrdered : ready[0];
              if (index !== undefined && results.has(index)) {
                const value = results.get(index)!;
                results.delete(index);
                if (ordered) {
                  nextOrdered++;
                  ready.splice(ready.indexOf(index), 1);
                } else ready.shift();
                emitted++;
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
