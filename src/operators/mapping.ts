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
 * Finite retry policy for one mapper input; upstream and cleanup failures are not retried.
 */
export interface RetryOptions {
  /** Retries after the initial attempt. Zero disables retries. */
  readonly retries: number;
  /** Decide whether to retry; receives the failed attempt. Defaults to true. */
  readonly shouldRetry?: (
    error: unknown,
    context: MapContext,
  ) => boolean | PromiseLike<boolean>;
  /** Finite non-negative wait, or synchronous policy for the failed attempt. Defaults to zero. */
  readonly delayMs?: number | ((error: unknown, context: MapContext) => number);
}

/**
 * Concurrency, output ordering, capacity, and retry policy for map and mapSettled.
 */
export interface MapOptions extends BoundedMapOptions {
  /** Optional finite retry policy; omitted by default. */
  readonly retry?: RetryOptions;
}

/**
 * The final outcome of one mapper input after any retries.
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
 * Transform values with bounded concurrency, awaiting each callback result.
 *
 * Defaults to ordered output and one concurrent item. An unrecovered mapper failure stops execution, including when an earlier result is unfinished. Retries apply only to the failed invocation for the same input, retaining its slot.
 * @throws RangeError if concurrency, capacity, or retry settings are invalid.
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
 * Transform each input into a fulfilled or rejected outcome after its retry policy finishes.
 *
 * Mapper failures do not stop other inputs. Upstream failures, cleanup failures, and execution cancellation still stop execution. Bounds and ordering match map.
 * @throws RangeError if concurrency, capacity, or retry settings are invalid.
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
