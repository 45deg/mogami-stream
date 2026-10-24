// #region example
import { from } from "mogami-stream";

const stop = new AbortController();
const group = from([1, 2, 3, 4]).route({
  branches: ["odd", "even"],
  capacity: 2,
  signal: stop.signal,
  select: (value) => (value % 2 ? "odd" : "even"),
});
const jobs = [
  group.branches.odd.toArray({ signal: stop.signal }),
  group.branches.even.toArray({ signal: stop.signal }),
].map((job) =>
  job.catch((error: unknown) => {
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
