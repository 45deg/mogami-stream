import type { Source, OperatorContext } from "./stream.js";
import type { BranchGroup } from "./branch.js";
import type { ExecutionOptions } from "../terminal/iterate.js";
import { branchLayout } from "../internal/branch-layout.js";
import { createBranchHub } from "../internal/branch-hub.js";

export type RoutePredicate<T> = (
  value: T,
  context: OperatorContext,
) => boolean | PromiseLike<boolean>;
export interface RouteSettings extends ExecutionOptions {
  /** Positive safe integer limiting each branch queue, excluding the coordinator's single item. */
  readonly capacity: number;
  /** Handles items with no active destination after trying `default`. Defaults to `"ignore"`. */
  readonly fallback?: "error" | "ignore";
}
export interface RouteOptions<T> extends RouteSettings {
  /** Positive safe integer. Its product with capacity must also be a safe integer. */
  readonly branches: number;
  readonly select: (
    value: T,
    context: OperatorContext,
  ) => number | PromiseLike<number>;
  /** Existing branch to use if the selected destination departs. */
  readonly default?: number;
}
export interface KeyedRouteOptions<T, K extends string> extends RouteSettings {
  /** Non-empty, unique string keys, captured at construction. */
  readonly branches: readonly K[];
  readonly select: (
    value: T,
    context: OperatorContext,
  ) => NoInfer<K> | PromiseLike<NoInfer<K>>;
  /** Existing branch to use if the selected destination departs. */
  readonly default?: NoInfer<K>;
}
export interface PredicateRouteOptions<
  T,
  R extends Record<string, RoutePredicate<T>>,
  D extends (keyof R & string) | undefined = undefined,
> extends RouteSettings {
  /**
   * Predicates captured in `Object.keys` order at construction; first match wins.
   * Departed endpoints still participate in classification, with no fallthrough.
   * Each predicate receives the same context for an item. Only synchronous type guards
   * narrow values, using each branch's own guard without excluding earlier matches.
   */
  readonly branches: R;
  readonly select?: never;
  /**
   * Existing branch for unmatched items or departed destinations; its predicate is not reevaluated.
   * All possible default destinations keep the original input type, including union-valued defaults.
   */
  readonly default?: D;
}
export type RoutedValues<T, R, D> = {
  readonly [K in keyof R]: K extends D
    ? T
    : R[K] extends (value: any, ...args: any[]) => value is infer S
      ? T & S
      : T;
};

/**
 * Routes each input item to at most one bounded, one-shot branch without copying it.
 * Selectors run once per item in input order; predicates use the first match in `Object.keys` order.
 * An unavailable destination uses `default`, then `fallback`; a full destination waits.
 *
 * Values, including Promise-valued items, are neither cloned nor awaited. Distinct input
 * items can share a reference. Callback `index` counts discarded items too.
 *
 * New source reads require branch demand. The coordinator holds at most one item across
 * reading, classification, and delivery, in addition to the bounded branch queues.
 * A full selected or default destination blocks later items, without redirecting them.
 * Departure while waiting triggers default/fallback; an unavailable default is not retried.
 * Enqueuing or handing an item to a pending read commits delivery. Departure discards
 * queued items without rerouting them. `fallback: "error"` does not guarantee downstream completion.
 *
 * Invalid selector results (including `undefined`) and callback errors fail the group
 * regardless of fallback, with the failure reported in `report.source`.
 * Discarding an item leaves demand pending. Sustained reads yield periodically,
 * including discard-only runs. Configuration is validated before source acquisition.
 * @throws {RangeError} If bounds or their product are invalid, keys are empty or duplicated, or `default` does not name an existing branch.
 * @category Sources
 */
export function route<
  T,
  const R extends Record<string, RoutePredicate<T>>,
  const D extends (keyof R & string) | undefined = undefined,
>(
  source: Source<T>,
  options: PredicateRouteOptions<T, R, D>,
): BranchGroup<RoutedValues<T, R, D>>;
export function route<T, const K extends readonly string[]>(
  source: Source<T>,
  options: KeyedRouteOptions<T, K[number]> & { readonly branches: K },
): BranchGroup<{ readonly [P in K[number]]: T }>;
export function route<T>(
  source: Source<T>,
  options: RouteOptions<T>,
): BranchGroup<readonly T[]>;
export function route<T>(
  source: Source<T>,
  options: RouteSettings & {
    readonly branches:
      number | readonly string[] | Record<string, RoutePredicate<T>>;
    readonly select?: (value: T, context: OperatorContext) => unknown;
    readonly default?: string | number;
  },
): BranchGroup<any> {
  const {
    branches,
    capacity,
    signal,
    select,
    default: defaultKey,
    fallback = "ignore",
  } = options;
  if (fallback !== "ignore" && fallback !== "error")
    throw new TypeError("fallback must be 'ignore' or 'error'.");
  const predicates =
    typeof branches === "object" &&
    branches !== null &&
    !Array.isArray(branches);
  if (predicates ? select !== undefined : typeof select !== "function")
    throw new TypeError(
      "Use either branch predicates or a selector with branch keys/count.",
    );
  const layout = branchLayout(
    predicates
      ? Object.keys(branches)
      : (branches as number | readonly string[]),
    capacity,
  );
  const defaultIndex = defaultKey === undefined ? -1 : layout.index(defaultKey);
  if (defaultKey !== undefined && defaultIndex < 0)
    throw new RangeError("default must name an existing branch.");
  const tests = predicates
    ? layout.keys.map(
        (key) => (branches as Record<string, RoutePredicate<T>>)[key]!,
      )
    : undefined;
  if (tests?.some((test) => typeof test !== "function"))
    throw new TypeError("Each branch predicate must be a function.");

  const checkedIndex = (key: unknown): number => {
    const index = layout.index(key);
    if (index < 0)
      throw new RangeError(
        "select must return an existing branch key or index.",
      );
    return index;
  };
  function choose(
    value: T,
    context: OperatorContext,
  ): number | PromiseLike<number> {
    if (!tests) {
      const key = select!(value, context);
      return typeof key === "number" || typeof key === "string"
        ? checkedIndex(key)
        : Promise.resolve(key).then(checkedIndex);
    }
    function next(start: number): number | PromiseLike<number> {
      for (let index = start; index < tests!.length; index++) {
        if (context.signal.aborted) throw context.signal.reason;
        const matches = tests![index]!(value, context);
        if (typeof matches === "boolean") {
          if (matches) return index;
        } else {
          return Promise.resolve(matches).then((matched) =>
            matched ? index : next(index + 1),
          );
        }
      }
      return -1;
    }
    return next(0);
  }
  return createBranchHub(source, layout, capacity, signal, {
    select: choose,
    defaultIndex,
    fallback,
  });
}
