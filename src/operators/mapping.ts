import type { Scope } from "../core/scope.js";
import type { Operator } from "../core/stream.js";
import {
  scopedMap,
  type BoundedMapOptions,
  type MapContext,
} from "../internal/bounded-map.js";
import { duration, integer } from "../internal/errors.js";

export type { MapContext } from "../internal/bounded-map.js";

export interface RetryOptions {
  /** Retries after the initial attempt. Zero disables retries. */
  readonly retries: number;
  readonly shouldRetry?: (
    error: unknown,
    context: MapContext,
  ) => boolean | PromiseLike<boolean>;
  readonly delayMs?: number | ((error: unknown, context: MapContext) => number);
}

export interface MapOptions extends BoundedMapOptions {
  readonly retry?: RetryOptions;
}

export type SettledResult<T> =
  | { readonly status: "fulfilled"; readonly value: T }
  | { readonly status: "rejected"; readonly reason: unknown };

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
