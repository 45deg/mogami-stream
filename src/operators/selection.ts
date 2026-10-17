import {
  create,
  open,
  type Operator,
  type OperatorContext,
} from "../core/stream.js";
import { done } from "../internal/deferred.js";
import { cancellationReason, integer } from "../internal/errors.js";
import { from } from "./creation.js";

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
