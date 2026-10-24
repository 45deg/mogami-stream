import {
  create,
  open,
  type OperatorContext,
  type Source,
  type Read,
} from "../core/stream.js";
import { iterate, type ExecutionOptions } from "./iterate.js";

/**
 * Reduces values from `source` sequentially using `reducer` and returns the final accumulator after cleanup.
 *
 * `initial` is awaited before opening `source`. If `source` is empty, the resolved initial value is returned. Object accumulators are passed by reference.
 * @category Consumption
 */
export async function reduce<T, A>(
  source: Source<T>,
  reducer: (
    accumulator: Awaited<A>,
    value: T,
    context: OperatorContext,
  ) => Awaited<A> | PromiseLike<Awaited<A>>,
  initial: A,
  options: ExecutionOptions = {},
): Promise<Awaited<A>> {
  let accumulator!: Awaited<A>;
  const steps = create<T>((scope) => {
    let upstream: ReturnType<typeof open<T>> | undefined;
    let index = 0;
    const read: Read<T> = (receive, reject) => {
      const accept = async (result: IteratorResult<T>): Promise<void> => {
        try {
          if (!result.done) {
            // Both turns preserve next()'s read-to-callback cancellation window
            // (range-cancellation.test.mjs). One turn invokes the reducer too early.
            await undefined;
            await undefined;
            scope.check();
            accumulator = await reducer(accumulator, result.value, {
              index: index++,
              signal: scope.signal,
            });
          }
          receive(result);
        } catch (error) {
          reject(error);
        }
      };
      if (upstream) upstream.read(accept, reject);
      else
        void (async () => {
          try {
            accumulator = await initial;
            scope.check();
            upstream = open(source, scope);
            upstream.read(accept, reject);
          } catch (error) {
            reject(error);
          }
        })();
    };
    return { read, next: () => new Promise(read) };
  });
  const iterator = iterate(steps, options);
  try {
    while (!(await iterator.next()).done) {
      // Each read applies exactly one reduction step without retaining values.
    }
    return accumulator;
  } finally {
    await iterator.closed;
  }
}
