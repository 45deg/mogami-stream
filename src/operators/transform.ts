import { boundedMap } from "../internal/bounded-map.js";
import {
  create,
  open,
  type Operator,
  type OperatorContext,
} from "../core/stream.js";
import { done, item } from "../internal/deferred.js";
import { integer } from "../internal/errors.js";

export const map = boundedMap;
export type { MapOptions } from "../internal/bounded-map.js";

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
      return {
        async next() {
          for (;;) {
            const result = await upstream.next();
            if (result.done) return done();
            if (
              await predicate(result.value, {
                index: index++,
                signal: scope.signal,
              })
            )
              return result;
            scope.check();
          }
        },
      };
    });
}

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
