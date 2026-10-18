import {
  create,
  open,
  type Stream,
  type Operator,
  type OperatorContext,
  type Read,
} from "../core/stream.js";
import { done, item } from "../internal/deferred.js";
import { integer } from "../internal/errors.js";

/**
 * Maps each value sequentially and emits only results that are neither `null` nor `undefined`.
 *
 * Other falsy values (`0`, `false`, `""`, `NaN`) are kept.
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
 * Drops consecutive values that produce the same key according to `Object.is`.
 *
 * When `keySelector` is omitted, values are compared directly (preserving Promise identity). When provided, `keySelector` is called and awaited sequentially for each item, and only the last emitted key is retained in memory.
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
      scope.finalizers.push(() => {
        previous = undefined;
      });
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

/**
 * Emits only values for which `predicate` returns `true`.
 *
 * Predicates run sequentially and may be asynchronous. Synchronous type guard predicates narrow the output stream type.
 * @category Transformation
 */
export function filter<T, S extends T>(
  predicate: (value: T, context: OperatorContext) => value is S,
): Operator<T, S>;
export function filter<T>(
  predicate: (
    value: T,
    context: OperatorContext,
  ) => boolean | PromiseLike<boolean>,
): Operator<T, T>;
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
 * Groups consecutive values into arrays of up to `size` items.
 *
 * If the stream completes normally with fewer than `size` buffered items, the remaining items are emitted as a final smaller array.
 * @throws {RangeError} If `size` is not a positive safe integer.
 * @category Transformation
 */
export function chunks(
  size: number,
): <T>(source: AsyncIterable<T>) => Stream<T[]>;
export function chunks<T>(size: number): Operator<T, T[]>;
export function chunks(
  size: number,
): <T>(source: AsyncIterable<T>) => Stream<T[]> {
  integer(size, "size");
  return <T>(source: AsyncIterable<T>) =>
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
 * Reduces values sequentially and emits the updated accumulator after each item.
 *
 * `initial` is awaited before opening the upstream source and is not emitted on its own (an empty source emits nothing). Objects resolved from `initial` are used by reference.
 * @category Transformation
 */
export function scan<T, A>(
  reducer: (
    accumulator: Awaited<A>,
    value: T,
    context: OperatorContext,
  ) => Awaited<A> | PromiseLike<Awaited<A>>,
  initial: A,
): Operator<T, Awaited<A>> {
  return (source) =>
    create((scope) => {
      let state:
        | { upstream: ReturnType<typeof open<T>>; accumulator: Awaited<A> }
        | undefined;
      let index = 0;
      scope.finalizers.push(() => {
        state = undefined;
      });
      return {
        async next() {
          if (!state) {
            const accumulator = await initial;
            scope.check();
            state = { upstream: open(source, scope), accumulator };
          }
          const result = await state.upstream.next();
          if (result.done) return done();
          scope.check();
          state.accumulator = await reducer(state.accumulator, result.value, {
            index: index++,
            signal: scope.signal,
          });
          return item(state.accumulator);
        },
      };
    });
}
