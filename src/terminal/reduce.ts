import type { OperatorContext, Source } from "../core/stream.js";
import { from } from "../operators/creation.js";
import { tap } from "../operators/lifecycle.js";
import { iterate, type ExecutionOptions } from "./iterate.js";

/**
 * Sequentially await a reducer and return the final accumulator after cleanup.
 *
 * The initial accumulator is required and used by reference. Empty inputs return it unchanged; callback indexes restart for each execution.
 * @category Consumption
 */
export async function reduce<T, A>(
  source: Source<T>,
  reducer: (
    accumulator: A,
    value: T,
    context: OperatorContext,
  ) => A | PromiseLike<A>,
  initial: A,
  options: ExecutionOptions = {},
): Promise<A> {
  let accumulator = initial;
  // Keep reducer work inside the execution so cancellation reaches its signal
  // and cleanup waits for the returned promise, just like any other callback.
  const steps = from(source).pipe(
    tap(async (value, context) => {
      accumulator = await reducer(accumulator, value, context);
    }),
  );
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
