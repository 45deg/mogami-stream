import type { OperatorContext, Source } from "../core/stream.js";
import { defer, from } from "../operators/creation.js";
import { tap } from "../operators/lifecycle.js";
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
  // Keep reducer work inside the execution so cancellation reaches its signal
  // and cleanup waits for the returned promise, just like any other callback.
  const steps = defer(async () => {
    accumulator = await initial;
    return from(source).pipe(
      tap(async (value, context) => {
        accumulator = await reducer(accumulator, value, context);
      }),
    );
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
