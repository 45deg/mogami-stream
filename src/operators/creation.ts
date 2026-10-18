import { create, open, type Source, Stream } from "../core/stream.js";
import { acquireIterator } from "../core/iterator.js";
import { done, item } from "../internal/deferred.js";

/**
 * Wrap an iterable without changing its replayability or awaiting Promise-valued items.
 *
 * Input acquisition is lazy until the first read. A single-use iterator remains single-use.
 * @example
 * ```ts
 * const values = await from([1, 2, 3]).toArray();
 * ```
 * @category Sources
 */
export function from<T>(source: Source<T>): Stream<T> {
  if (source instanceof Stream) return source;
  return create((scope) => acquireIterator(source, scope));
}

/**
 * Create a fresh input on the first read of each execution.
 *
 * The factory may return a PromiseLike of an iterable. Pass its signal to external I/O so cancellation can interrupt that work.
 * @category Sources
 */
export function defer<T>(
  factory: (context: {
    readonly signal: AbortSignal;
  }) => Source<T> | PromiseLike<Source<T>>,
): Stream<T> {
  return create((scope) => {
    let reader: ReturnType<typeof open<T>> | undefined;
    return {
      async next() {
        if (!reader) {
          const source = await scope.observe(
            Promise.resolve().then(() => {
              scope.check();
              return factory({ signal: scope.signal });
            }),
          );
          scope.check();
          reader = open(source, scope);
        }
        return reader.next();
      },
    };
  });
}

/**
 * Create a replayable stream containing the supplied values.
 * @category Sources
 */
export function of<T>(...values: T[]): Stream<T> {
  return from(values);
}
/**
 * Create a stream that completes without emitting a value.
 * @category Sources
 */
export function empty<T = never>(): Stream<T> {
  return from<T>([]);
}

/**
 * Emit safe integers from `start` toward, but excluding, `end`.
 *
 * A negative step produces a descending range. A step pointing away from the end produces an empty stream.
 * @throws RangeError if a bound or step is not a safe integer, or the step is zero.
 * @category Sources
 */
export function range(start: number, end: number, step = 1): Stream<number> {
  for (const [name, value] of [
    ["start", start],
    ["end", end],
    ["step", step],
  ] as const) {
    if (!Number.isSafeInteger(value))
      throw new RangeError(`${name} must be a safe integer.`);
  }
  if (step === 0) throw new RangeError("step must not be zero.");
  return create(() => {
    let current = start;
    let exhausted = false;
    return {
      async next() {
        if (exhausted || (step > 0 ? current >= end : current <= end))
          return done();
        const value = current;
        const next = current + step;
        // An unsafe sum is already beyond every valid bound in this direction.
        // Safe integer sums are exact, including steps that cross zero.
        if (!Number.isSafeInteger(next)) exhausted = true;
        else current = next;
        return item(value);
      },
    };
  });
}
