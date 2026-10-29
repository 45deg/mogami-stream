import type { Source } from "../core/stream.js";
import {
  iterateWithScheduler,
  readExecution,
  type ExecutionOptions,
} from "./iterate.js";
import { defaultScheduler } from "../core/scheduler.js";

/**
 * Exposes one execution as a Web Stream with no output prefetch queue.
 * At most one upstream read is active. Cancellation propagates its reason upstream and
 * waits for cleanup. External aborts error the Web Stream, including while idle.
 * Cleanup failures during cancellation reject cancel(); additional errors remain
 * subject to the execution's close-report semantics.
 * @category Consumption
 */
export function toReadableStream<T>(
  source: Source<T>,
  options: ExecutionOptions = {},
): ReadableStream<T> {
  const iterator = iterateWithScheduler(source, options, defaultScheduler);
  let cancelled = false;
  let reading = false;
  return new ReadableStream<T>(
    {
      start(controller) {
        void iterator.closed.then((report) => {
          if (!cancelled && report.kind !== "completed")
            controller.error(report.reason);
        });
      },
      pull(controller) {
        // Multiple pending Web reads can request a pull before delivery. The
        // enqueue below wakes further demand after the active read completes.
        if (reading || cancelled) return;
        reading = true;
        const fail = (error: unknown) => {
          reading = false;
          if (!cancelled) controller.error(error);
        };
        iterator[readExecution]((result) => {
          reading = false;
          try {
            if (!cancelled) {
              if (result.done) controller.close();
              else controller.enqueue(result.value);
            }
          } catch (error) {
            fail(error);
          }
        }, fail);
      },
      async cancel(reason) {
        cancelled = true;
        iterator.cancel(reason);
        const report = await iterator.closed;
        if (report.kind === "failed") throw report.reason;
        if (report.suppressedErrors.length)
          throw new AggregateError(
            report.suppressedErrors,
            "Stream cleanup failed.",
          );
      },
    },
    { highWaterMark: 0 },
  );
}
