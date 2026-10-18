import type { Scope } from "../core/scope.js";
import type { Operator } from "../core/stream.js";
import {
  scopedMap,
  type BoundedMapOptions,
  type MapContext,
} from "../internal/bounded-map.js";
import { duration, integer } from "../internal/errors.js";

export type { MapContext } from "../internal/bounded-map.js";

/**
 * Retry configuration for individual mapper failures. Upstream read errors and cleanup errors are not retried.
 */
export interface RetryOptions {
  /** Maximum number of retry attempts after the initial failure. Must be a non-negative safe integer (`0` disables retries). */
  readonly retries: number;
  /** Predicate called after a failed attempt to decide whether to retry. Receives the error and the failed attempt's context. Defaults to retrying all mapper errors. */
  readonly shouldRetry?: (
    error: unknown,
    context: MapContext,
  ) => boolean | PromiseLike<boolean>;
  /** Delay in milliseconds before the next retry, or a synchronous function that computes the delay from the failed attempt. Must be finite and non-negative. Defaults to `0`. */
  readonly delayMs?: number | ((error: unknown, context: MapContext) => number);
}

/**
 * Concurrency, ordering, capacity, and retry options for {@link map} and {@link mapSettled}.
 */
export interface MapOptions extends BoundedMapOptions {
  /** Optional retry policy for failed mapper calls. */
  readonly retry?: RetryOptions;
}

/**
 * Outcome of a single item in {@link mapSettled}, after any configured retries.
 */
export type SettledResult<T> =
  | { readonly status: "fulfilled"; readonly value: T }
  | { readonly status: "rejected"; readonly reason: unknown };

/** @inline */
type Mapper<T, U> = (value: T, context: MapContext) => U;

function validateRetry(retry: RetryOptions | undefined): void {
  if (!retry) return;
  integer(retry.retries, "retry.retries", 0);
  if (typeof retry.delayMs !== "function")
    duration(retry.delayMs ?? 0, "retry.delayMs");
}

function prepareMapper<T, U>(
  mapper: Mapper<T, U>,
  retry: RetryOptions | undefined,
) {
  const invoke = mapper;
  // Snapshot settings, but enter the asynchronous retry loop only after failure.
  const { retries = 0, shouldRetry, delayMs = 0 } = retry ?? {};
  const makeRecovery =
    retries === 0
      ? undefined
      : (scope: Scope) =>
          async (
            error: unknown,
            value: T,
            context: MapContext,
          ): Promise<Awaited<U>> => {
            let failedContext = context;
            for (;;) {
              if (scope.signal.aborted || failedContext.attempt > retries)
                throw error;
              const retry = shouldRetry
                ? await shouldRetry(error, failedContext)
                : true;
              scope.check();
              if (!retry) throw error;
              const wait =
                typeof delayMs === "function"
                  ? delayMs(error, failedContext)
                  : delayMs;
              duration(wait, "retry.delayMs");
              scope.check();
              if (wait > 0) await scope.scheduler.sleep(wait, context.signal);
              scope.check();
              const nextContext = {
                ...context,
                attempt: failedContext.attempt + 1,
              };
              try {
                return await mapper(value, nextContext);
              } catch (nextError) {
                error = nextError;
                failedContext = nextContext;
              }
            }
          };
  return { invoke, makeRecovery };
}

/**
 * Transforms each value with bounded concurrency, awaiting the mapper's result.
 *
 * Defaults to `concurrency: 1`, `capacity: concurrency`, and `ordered: true`. An unhandled mapper error immediately stops the execution, even if earlier items are still in flight. When `retry` is configured, only the failed mapper invocation for that item is retried while holding its concurrency and capacity slot.
 * @throws {RangeError} If `concurrency`, `capacity`, or `retry` options are invalid.
 * @example
 * ```ts
 * const values = await range(0, 3)
 *   .pipe(map(value => value * 2, { concurrency: 2 }))
 *   .toArray(); // [0, 2, 4]
 * ```
 * @category Mapping
 */
export function map<T, U>(
  mapper: Mapper<T, U>,
  options: MapOptions = {},
): Operator<T, Awaited<U>> {
  validateRetry(options.retry);
  const { invoke, makeRecovery } = prepareMapper(mapper, options.retry);
  return scopedMap<T, U | Promise<Awaited<U>>>(
    () => invoke,
    options,
    makeRecovery,
  );
}

/**
 * Maps each input to a `{ status: "fulfilled", value }` or `{ status: "rejected", reason }` outcome after applying any retry policy.
 *
 * Mapper errors are captured as `"rejected"` outcomes without stopping other items. Upstream errors, cleanup failures, and execution cancellation still terminate the stream as usual. Concurrency bounds and ordering match {@link map}.
 * @throws {RangeError} If `concurrency`, `capacity`, or `retry` options are invalid.
 * @category Mapping
 */
export function mapSettled<T, U>(
  mapper: Mapper<T, U>,
  options: MapOptions = {},
): Operator<T, SettledResult<Awaited<U>>> {
  validateRetry(options.retry);
  const { invoke, makeRecovery } = prepareMapper(mapper, options.retry);
  return scopedMap((scope) => {
    const recover = makeRecovery?.(scope);
    return async (value: T, context): Promise<SettledResult<Awaited<U>>> => {
      try {
        let result: Awaited<U>;
        try {
          result = await invoke(value, context);
        } catch (error) {
          if (!recover) throw error;
          result = await recover(error, value, context);
        }
        return { status: "fulfilled", value: result };
      } catch (reason) {
        // Preserve late failures in the execution's close report on cancellation.
        if (context.signal.aborted) throw reason;
        return { status: "rejected", reason };
      }
    };
  }, options);
}
