import type { Scope } from "../core/scope.js";

/** A disposable timer whose cancellation is an expected outcome, not a failure. */
export function alarm(scope: Scope, ms: number) {
  const controller = new AbortController();
  const abort = () => controller.abort(scope.signal.reason);
  scope.signal.addEventListener("abort", abort, { once: true });
  if (scope.signal.aborted) abort();
  const promise = scope.observe(
    scope.scheduler
      .sleep(ms, controller.signal)
      .then(
        () => !controller.signal.aborted,
        (error: unknown) => {
          if (controller.signal.aborted && error === controller.signal.reason)
            return false;
          throw error;
        },
      )
      .finally(() => scope.signal.removeEventListener("abort", abort)),
  );
  return {
    promise,
    cancel() {
      controller.abort();
    },
  };
}
