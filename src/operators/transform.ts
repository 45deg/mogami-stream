import {
  create,
  open,
  type Operator,
  type OperatorContext,
  type Read,
} from "../core/stream.js";
import { done, item } from "../internal/deferred.js";
import { integer } from "../internal/errors.js";

/**
 * Sequentially await a mapper and emit only its non-nullish results.
 *
 * Null and undefined are omitted; false, zero, and empty strings are retained.
 * @category Transformation
 */
export function filterMap<T, U>(
  mapper: (value: T, context: OperatorContext) => U,
): Operator<T, NonNullable<Awaited<U>>> {
  return (source) =>
    create((scope) => {
      const upstream = open(source, scope);
      let index = 0;
      return {
        async next() {
          for (;;) {
            const result = await upstream.next();
            if (result.done) return done();
            scope.check();
            const value = await mapper(result.value, {
              index: index++,
              signal: scope.signal,
            });
            scope.check();
            if (value != null) return item(value);
          }
        },
      };
    });
}

/**
 * Omit consecutive values with the same key, comparing with Object.is.
 *
 * Defaults to the value itself. A supplied key selector is awaited sequentially
 * for every input; only the last emitted key is retained per execution.
 * @category Transformation
 */
export function dedupe<T, K = T>(
  keySelector?: (value: T, context: OperatorContext) => K,
): Operator<T, T> {
  return (source) =>
    create((scope) => {
      const upstream = open(source, scope);
      let index = 0;
      let hasPrevious = false;
      let previous: unknown;
      return {
        async next() {
          for (;;) {
            const result = await upstream.next();
            if (result.done) return done();
            scope.check();
            // Without a selector, Promise-valued items remain identity keys.
            const key = keySelector
              ? await keySelector(result.value, {
                  index: index++,
                  signal: scope.signal,
                })
              : result.value;
            scope.check();
            if (!hasPrevious || !Object.is(previous, key)) {
              hasPrevious = true;
              previous = key;
              return result;
            }
          }
        },
      };
    });
}

export function filter<T, S extends T>(
  predicate: (value: T, context: OperatorContext) => value is S,
): Operator<T, S>;
export function filter<T>(
  predicate: (
    value: T,
    context: OperatorContext,
  ) => boolean | PromiseLike<boolean>,
): Operator<T, T>;
/**
 * Keep values whose sequential, awaited predicate returns true.
 *
 * Synchronous type predicates narrow the output type.
 * @category Transformation
 */
export function filter<T>(
  predicate: (
    value: T,
    context: OperatorContext,
  ) => boolean | PromiseLike<boolean>,
): Operator<T, T> {
  return (source) =>
    create((scope) => {
      const upstream = open(source, scope);
      let index = 0;
      const read: Read<T> = (receive, reject) => {
        const accept = async (result: IteratorResult<T>): Promise<void> => {
          try {
            if (result.done) {
              receive(done());
              return;
            }
            if (
              await predicate(result.value, {
                index: index++,
                signal: scope.signal,
              })
            ) {
              receive(result);
            } else {
              // Keep an awaited checkpoint even for synchronous rejection:
              // queued cancellation must stop a long run of discarded items.
              scope.check();
              upstream.read(accept, reject);
            }
          } catch (error) {
            reject(error);
          }
        };
        upstream.read(accept, reject);
      };
      return { read, next: () => new Promise(read) };
    });
}

/**
 * Group values into arrays of at most `size` items.
 *
 * Emit a final partial array only on normal completion.
 * @throws RangeError if size is not a positive safe integer.
 * @category Transformation
 */
export function chunks<T>(size: number): Operator<T, T[]> {
  integer(size, "size");
  return (source) =>
    create((scope) => {
      const upstream = open(source, scope);
      return {
        async next() {
          const values: T[] = [];
          while (values.length < size) {
            const result = await upstream.next();
            if (result.done) return values.length ? item(values) : done();
            values.push(result.value);
          }
          return item(values);
        },
      };
    });
}

/**
 * Emit the updated accumulator after each sequential, awaited reduction.
 *
 * The initial value is not emitted and is used by reference. Empty inputs emit nothing; callback indexes restart for each execution.
 * @category Transformation
 */
export function scan<T, A>(
  reducer: (
    accumulator: A,
    value: T,
    context: OperatorContext,
  ) => A | PromiseLike<A>,
  initial: A,
): Operator<T, A> {
  return (source) =>
    create((scope) => {
      const upstream = open(source, scope);
      let accumulator = initial;
      let index = 0;
      return {
        async next() {
          const result = await upstream.next();
          if (result.done) return done();
          scope.check();
          accumulator = await reducer(accumulator, result.value, {
            index: index++,
            signal: scope.signal,
          });
          return item(accumulator);
        },
      };
    });
}
