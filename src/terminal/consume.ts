import type { OperatorContext, Source } from "../core/stream.js";
import { from } from "../operators/creation.js";
import { filter } from "../operators/transform.js";
import { take } from "../operators/selection.js";
import type { ExecutionOptions } from "./iterate.js";
import { reduce } from "./reduce.js";

/**
 * Await sequential effects without retaining values, then wait for cleanup.
 *
 * Callback return values are discarded.
 * @category Consumption
 */
export async function forEach<T>(
  source: Source<T>,
  effect: (value: T, context: OperatorContext) => unknown,
  options: ExecutionOptions = {},
): Promise<void> {
  await reduce<T, void>(
    source,
    async (_accumulator, value, context) => {
      await effect(value, context);
    },
    undefined,
    options,
  );
}

export function find<T, S extends T>(
  source: Source<T>,
  predicate: (value: T, context: OperatorContext) => value is S,
  options?: ExecutionOptions,
): Promise<S | undefined>;
export function find<T>(
  source: Source<T>,
  predicate: (
    value: T,
    context: OperatorContext,
  ) => boolean | PromiseLike<boolean>,
  options?: ExecutionOptions,
): Promise<T | undefined>;
/**
 * Return the first matching value, or undefined if no value matches.
 *
 * Await predicates sequentially and close upstream on a match. Synchronous type predicates narrow the result. A matching undefined value is indistinguishable from no match.
 * @category Consumption
 */
export function find<T>(
  source: Source<T>,
  predicate: (
    value: T,
    context: OperatorContext,
  ) => boolean | PromiseLike<boolean>,
  options: ExecutionOptions = {},
): Promise<T | undefined> {
  return reduce<T, T | undefined>(
    from(source).pipe(filter(predicate), take(1)),
    (_accumulator, value) => value,
    undefined,
    options,
  );
}

/**
 * Return whether any value matches, closing upstream on the first match.
 *
 * Predicates are awaited sequentially. Empty inputs return false; cleanup completes before settlement.
 * @category Consumption
 */
export function some<T>(
  source: Source<T>,
  predicate: (
    value: T,
    context: OperatorContext,
  ) => boolean | PromiseLike<boolean>,
  options: ExecutionOptions = {},
): Promise<boolean> {
  return reduce(
    from(source).pipe(filter(predicate), take(1)),
    () => true,
    false,
    options,
  );
}

/**
 * Return whether all values match, closing upstream on the first nonmatch.
 *
 * Predicates are awaited sequentially. Empty inputs return true; cleanup completes before settlement.
 * @category Consumption
 */
export async function every<T>(
  source: Source<T>,
  predicate: (
    value: T,
    context: OperatorContext,
  ) => boolean | PromiseLike<boolean>,
  options: ExecutionOptions = {},
): Promise<boolean> {
  return !(await some(
    source,
    async (value, context) => !(await predicate(value, context)),
    options,
  ));
}
