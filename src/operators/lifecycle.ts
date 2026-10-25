import {
  create,
  open,
  type Stream,
  type Read,
  type Operator,
  type OperatorContext,
} from "../core/stream.js";

/**
 * Runs `effect` sequentially for each value and emits the original value unchanged.
 * @category Lifecycle
 */
export function tap<T>(
  effect: (value: T, context: OperatorContext) => unknown,
): Operator<T, T> {
  return (source) =>
    create((scope) => {
      const upstream = open(source, scope);
      let index = 0;
      const read: Read<T> = (receive, reject) => {
        upstream.read(async (result) => {
          try {
            if (!result.done) {
              // Both turns preserve next()'s read-to-effect cancellation window
              // (range-cancellation.test.mjs). One turn invokes the effect too early.
              await undefined;
              await undefined;
              scope.check();
              await effect(result.value, {
                index: index++,
                signal: scope.signal,
              });
            }
            receive(result);
          } catch (error) {
            reject(error);
          }
        }, reject);
      };
      return { read, next: () => new Promise(read) };
    });
}

/**
 * Registers a cleanup callback that runs once when the stage closes, if the stage was started.
 *
 * The callback runs after upstream stages and in-flight operations in the stage have finished. Nested `finalize` callbacks run from inner stages outward.
 * @category Lifecycle
 */
export function finalize(
  effect: () => unknown,
): <T>(source: AsyncIterable<T>) => Stream<T>;
export function finalize<T>(effect: () => unknown): Operator<T, T>;
export function finalize(
  effect: () => unknown,
): <T>(source: AsyncIterable<T>) => Stream<T> {
  return <T>(source: AsyncIterable<T>) =>
    create<T>((scope) => {
      scope.finalizers.push(effect);
      const upstream = open(source, scope);
      return {
        next: () => upstream.next(),
        read: (receive, reject) => upstream.read(receive, reject),
      };
    });
}
