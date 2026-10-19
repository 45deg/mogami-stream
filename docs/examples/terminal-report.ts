// #region collect
import {
  iterate,
  toArray,
  type ExecutionOptions,
  type Source,
} from "mogami-stream";

// Application helper: not an export of mogami-stream.
export async function collectWithReport<T>(
  source: Source<T>,
  options: ExecutionOptions = {},
) {
  const iterator = iterate(source, options);
  const outcome = await toArray(iterator).then(
    (value) => ({ status: "fulfilled" as const, value }),
    (reason: unknown) => ({ status: "rejected" as const, reason }),
  );
  return { ...outcome, report: await iterator.closed };
}
// #endregion collect

// #region timeout
import { finalize, from, map, timeoutEach } from "mogami-stream";

export async function nonCooperativeTimeout() {
  const lateError = new Error("work failed after timeout");
  const cleanupError = new Error("cleanup failed");
  let rejectWork!: (reason: unknown) => void;
  const work = new Promise<number>((_, reject) => {
    rejectWork = reject;
  });
  let cleanedUp = false;
  const iterator = from([1])
    .pipe(
      map(() => work), // Deliberately ignores the callback's signal.
      timeoutEach(5),
      finalize(() => {
        cleanedUp = true;
        throw cleanupError;
      }),
    )
    .iterate();

  // Attach both handlers immediately; notification itself never rejects.
  const notification = await iterator.next().then(
    (value) => ({ status: "fulfilled" as const, value }),
    (reason: unknown) => ({ status: "rejected" as const, reason }),
  );
  // TimeoutError is observable, but work is still pending and cleanup has not run.
  const cleanupPending = !cleanedUp;
  rejectWork(lateError); // Only the work's owner can settle this promise.
  const report = await iterator.closed;
  // report.reason is the TimeoutError; both later errors are suppressedErrors.
  return { notification, cleanupPending, report };
}
// #endregion timeout
