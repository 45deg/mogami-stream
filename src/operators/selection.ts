import {
  create,
  open,
  type Operator,
  type OperatorContext,
} from "../core/stream.js";
import { done } from "../internal/deferred.js";
import { cancellationReason, integer } from "../internal/errors.js";
import { from } from "./creation.js";

/**
 * Discard a prefix while the sequential, awaited predicate returns true.
 *
 * Emit the first failing value and all remaining values without testing again.
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
 * Emit at most `count` values and start closing upstream before delivering the last one.
 *
 * A count of zero never opens upstream. Downstream callback signals remain active.
 * @throws RangeError if count is not a non-negative safe integer.
 * @category Selection
 */
export function take<T>(count: number): Operator<T, T> {
  integer(count, "count", 0);
  return (source) =>
    create((scope) => {
      let upstream: ReturnType<typeof open<T>> | undefined;
      let remaining = count;
      return {
        async next() {
          if (remaining === 0) return done();
          upstream ??= open(source, scope);
          const result = await upstream.next();
          if (!result.done && --remaining === 0) {
            // Stop only the owned upstream, before delivering the last value.
            upstream.scope.stop(cancellationReason());
            void upstream.scope.close();
          }
          return result;
        },
      };
    });
}

/**
 * Discard the first `count` values, then emit the remainder.
 * @throws RangeError if count is not a non-negative safe integer.
 * @category Selection
 */
export function skip<T>(count: number): Operator<T, T> {
  integer(count, "count", 0);
  if (count === 0) return from;
  return (source) =>
    create((scope) => {
      const upstream = open(source, scope);
      let remaining = count;
      return {
        async next() {
          while (remaining > 0) {
            const result = await upstream.next();
            if (result.done) return result;
            remaining--;
          }
          return upstream.next();
        },
      };
    });
}

/**
 * Emit values until the sequential, awaited predicate first returns false.
 *
 * The first failing value is consumed but not emitted; the remainder is closed.
 * @category Selection
 */
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
