// #region example
import { broadcast } from "mogami-stream";

const stop = new AbortController();
const group = broadcast([1, 2, 3], {
  branches: 3,
  capacity: 2,
  signal: stop.signal,
});
group.branches[2]!.detach();
const jobs = group.branches.slice(0, 2).map((branch) =>
  branch.toArray({ signal: stop.signal }).catch((error: unknown) => {
    stop.abort(error);
    throw error;
  }),
);
let result: number[][];
try {
  result = await Promise.all(jobs);
} finally {
  const closing = group.close();
  await Promise.allSettled(jobs);
  const report = await closing;
  if (report.source.kind !== "completed") {
    console.error(report.source.reason, report.source.suppressedErrors);
  }
}

// #endregion example

export { result };
