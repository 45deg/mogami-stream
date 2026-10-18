import { create, open, type Source, Stream } from "../core/stream.js";
import { acquireIterator } from "../core/iterator.js";
import { done, item } from "../internal/deferred.js";

/**
 * Wraps a synchronous or asynchronous iterable into a {@link Stream} without altering its replayability or unwrapping Promise values.
 *
 * The underlying iterator is not acquired until the first read. Single-use iterators remain single-use.
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
 * Creates a fresh source on the first read of each execution.
 *
 * The factory may return a source directly or a `PromiseLike` that resolves to one. Pass `context.signal` to any external I/O so cancellation can abort in-flight setup.
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
 * Creates a replayable stream that emits the given values in order.
 * @category Sources
 */
export function of<T>(...values: T[]): Stream<T> {
  return from(values);
}
/**
 * Creates a stream that completes immediately without emitting any values.
 * @category Sources
 */
export function empty<T = never>(): Stream<T> {
  return from<T>([]);
}

/**
 * Emits safe integers starting at `start` and stepping by `step` toward (but excluding) `end`.
 *
 * Use a negative `step` for a descending range. If `step` moves away from `end`, the stream completes immediately without emitting values.
 * @throws {RangeError} If `start`, `end`, or `step` is not a safe integer, or if `step` is `0`.
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
