import { defer, finalize, iterate } from "mogami-stream";

export async function readOne(signal: AbortSignal) {
  const source = defer(({ signal }) =>
    (async function* () {
      const response = await fetch("https://example.com/data", { signal });
      if (!response.ok) {
        await response.body?.cancel();
        throw new Error(`HTTP ${response.status}`);
      }
      yield await response.text();
    })(),
  ).pipe(finalize(() => console.log("cleanup complete")));

  const iterator = iterate(source, { signal });
  try {
    return await iterator.next();
  } finally {
    try {
      await iterator.return();
    } finally {
      const report = await iterator.closed;
      if (report.kind !== "completed") console.error(report.reason);
      for (const error of report.suppressedErrors) console.error(error);
    }
  }
}
