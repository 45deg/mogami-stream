import type { OperatorContext, Source } from "../core/stream.js";
import { from } from "../operators/creation.js";
import { filter } from "../operators/transform.js";
import { take } from "../operators/selection.js";
import { iterate, type ExecutionOptions } from "./iterate.js";
import { tap } from "../operators/lifecycle.js";
import { reduce } from "./reduce.js";

/**
 * Calls `effect` sequentially for each value in `source` and waits for cleanup before resolving.
 *
 * Return values from `effect` are awaited and then ignored.
 * @category Consumption
 */
export async function forEach<T>(
  source: Source<T>,
  effect: (value: T, context: OperatorContext) => unknown,
  options: ExecutionOptions = {},
): Promise<void> {
  const iterator = iterate(from(source).pipe(tap(effect)), options);
  try {
    while (!(await iterator.next()).done) {
      // The tap stage owns each effect until it settles.
    }
  } finally {
    await iterator.closed;
  }
}

/**
 * Returns the first value in `source` that satisfies `predicate`, or `undefined` if no value matches.
 *
 * Predicates are evaluated sequentially. As soon as a match is found, upstream is closed and the matched value is awaited before returning (while `predicate` itself receives the unawaited item). Synchronous type guard predicates narrow the return type.
 * @category Consumption
 */
export function find<T, S extends T>(
  source: Source<T>,
  predicate: (value: T, context: OperatorContext) => value is S,
  options?: ExecutionOptions,
): Promise<Awaited<S> | undefined>;
export function find<T>(
  source: Source<T>,
  predicate: (
    value: T,
    context: OperatorContext,
  ) => boolean | PromiseLike<boolean>,
  options?: ExecutionOptions,
): Promise<Awaited<T> | undefined>;
export function find<T>(
  source: Source<T>,
  predicate: (
    value: T,
    context: OperatorContext,
  ) => boolean | PromiseLike<boolean>,
  options: ExecutionOptions = {},
): Promise<Awaited<T> | undefined> {
  return reduce<T, T | undefined>(
    from(source).pipe(filter(predicate), take(1)),
    async (_accumulator, value): Promise<Awaited<T>> => await value,
    undefined,
    options,
  );
}

/**
 * Returns `true` as soon as `predicate` returns `true` for any value in `source`, closing upstream early.
 *
 * Predicates are evaluated sequentially. Returns `false` for empty sources, and waits for cleanup before resolving.
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
 * Returns `false` as soon as `predicate` returns `false` for any value in `source`, closing upstream early.
 *
 * Predicates are evaluated sequentially. Returns `true` for empty sources, and waits for cleanup before resolving.
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
