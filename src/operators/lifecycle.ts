import {
  create,
  open,
  type Operator,
  type OperatorContext,
} from "../core/stream.js";

/**
 * Await a sequential effect for each value and emit the original value unchanged.
 * @category Lifecycle
 */
export function tap<T>(
  effect: (value: T, context: OperatorContext) => unknown,
): Operator<T, T> {
  return (source) =>
    create((scope) => {
      const upstream = open(source, scope);
      let index = 0;
      return {
        async next() {
          const result = await upstream.next();
          if (!result.done) {
            scope.check();
            await effect(result.value, {
              index: index++,
              signal: scope.signal,
            });
          }
          return result;
        },
      };
    });
}

/**
 * Run and await an effect once when a started stage closes.
 *
 * Children and outstanding work finish first. Nested finalizers run from the inner stage outward; an unstarted stage does not run its finalizer.
 * @category Lifecycle
 */
export function finalize<T>(effect: () => unknown): Operator<T, T> {
  return (source) =>
    create((scope) => {
      scope.finalizers.push(effect);
      const upstream = open(source, scope);
      return {
        next: () => upstream.next(),
        read: (receive, reject) => upstream.read(receive, reject),
      };
    });
}
