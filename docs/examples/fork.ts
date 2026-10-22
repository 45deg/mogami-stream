import { fork, from } from "mogami-stream";

const stop = new AbortController();
const group = fork([1, 2, 3], {
  branches: 2,
  capacity: 8,
  signal: stop.signal,
});
const jobs = group.branches.map(async (branch) => {
  try {
    return await from(branch).toArray({ signal: stop.signal });
  } catch (error) {
    stop.abort(error);
    throw error;
  } finally {
    branch.detach();
  }
});

try {
  const results = await Promise.all(jobs);
  console.log(results); // [[1, 2, 3], [1, 2, 3]]
} finally {
  const closing = group.close();
  await Promise.allSettled(jobs);
  const report = await closing;
  if (report.source.kind !== "completed") {
    console.error(report.source.reason, report.source.suppressedErrors);
  }
}
