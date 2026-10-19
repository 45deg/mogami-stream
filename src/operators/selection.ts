import {
  create,
  open,
  type Stream,
  type Read,
  type Operator,
  type OperatorContext,
} from "../core/stream.js";
import { done } from "../internal/deferred.js";
import { cancellationReason, integer } from "../internal/errors.js";
import { from } from "./creation.js";

/**
 * Skips leading values while `predicate` returns `true`, then emits the first non-matching value and all subsequent values without calling `predicate` again.
 *
 * Predicates run sequentially and may be asynchronous.
 * @category Selection
 */
export function skipWhile<T>(
  predicate: (
    value: T,
    context: OperatorContext,
  ) => boolean | PromiseLike<boolean>,
): Operator<T, T> {
  return (source) =>
    create((scope) => {
      const upstream = open(source, scope);
      let index = 0;
      let skipping = true;
      return {
        async next() {
          for (;;) {
            const result = await upstream.next();
            if (result.done || !skipping) return result;
            scope.check();
            skipping = await predicate(result.value, {
              index: index++,
              signal: scope.signal,
            });
            scope.check();
            if (!skipping) return result;
          }
        },
      };
    });
}

/**
 * Emits at most `count` values, then closes upstream.
 *
 * Upstream cleanup begins before the final (`count`-th) value is delivered, while downstream callback signals remain active. `take(0)` completes immediately without opening upstream.
 * @throws {RangeError} If `count` is not a non-negative safe integer.
 * @category Selection
 */
export function take(count: number): <T>(source: AsyncIterable<T>) => Stream<T>;
export function take<T>(count: number): Operator<T, T>;
export function take(
  count: number,
): <T>(source: AsyncIterable<T>) => Stream<T> {
  integer(count, "count", 0);
  return <T>(source: AsyncIterable<T>) =>
    create((scope) => {
      let upstream: ReturnType<typeof open<T>> | undefined;
      let remaining = count;
      const read: Read<T> = (receive, reject) => {
        if (remaining === 0) {
          receive(done());
          return;
        }
        upstream ??= open(source, scope);
        upstream.read((result) => {
          if (!result.done && --remaining === 0) {
            // Stop only the owned upstream, before delivering the last value.
            upstream!.scope.stop(cancellationReason());
            void upstream!.scope.close();
          }
          receive(result);
        }, reject);
      };
      return { read, next: () => new Promise(read) };
    });
}

/**
 * Skips the first `count` values and emits the remaining values.
 * @throws {RangeError} If `count` is not a non-negative safe integer.
 * @category Selection
 */
export function skip(count: number): <T>(source: AsyncIterable<T>) => Stream<T>;
export function skip<T>(count: number): Operator<T, T>;
export function skip(
  count: number,
): <T>(source: AsyncIterable<T>) => Stream<T> {
  integer(count, "count", 0);
  if (count === 0) return from;
  return <T>(source: AsyncIterable<T>) =>
    create((scope) => {
      const upstream = open(source, scope);
      let remaining = count;
      const read: Read<T> = (receive, reject) => {
        if (remaining === 0) {
          upstream.read(receive, reject);
          return;
        }
        const accept = async (result: IteratorResult<T>): Promise<void> => {
          try {
            if (result.done || remaining === 0) {
              receive(result);
              return;
            }
            remaining--;
            // Yield before another read, even for synchronous internal sources.
            await undefined;
            scope.check();
            upstream.read(accept, reject);
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
 * Emits values while `predicate` returns `true`, stopping and closing upstream on the first `false` result.
 *
 * Predicates run sequentially and may be asynchronous. Synchronous type guard predicates narrow the output stream type. The first non-matching value is consumed by the test and is not emitted.
 * @category Selection
 */
export function takeWhile<T, S extends T>(
  predicate: (value: T, context: OperatorContext) => value is S,
): Operator<T, S>;
export function takeWhile<T>(
  predicate: (
    value: T,
    context: OperatorContext,
  ) => boolean | PromiseLike<boolean>,
): Operator<T, T>;
export function takeWhile<T>(
  predicate: (
    value: T,
    context: OperatorContext,
  ) => boolean | PromiseLike<boolean>,
): Operator<T, T> {
  return (source) =>
    create((scope) => {
      const upstream = open(source, scope);
      let index = 0;
      let ended = false;
      return {
        async next() {
          if (ended) return done();
          const result = await upstream.next();
          if (result.done) return result;
          scope.check();
          if (
            await predicate(result.value, {
              index: index++,
              signal: scope.signal,
            })
          )
            return result;
          ended = true;
          upstream.scope.stop(cancellationReason());
          await upstream.scope.close();
          return done();
        },
      };
    });
}
