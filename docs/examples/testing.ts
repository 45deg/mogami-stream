// #region sources
import { catchError, fromTimeline, of, timeoutEach } from "mogami-stream";

export function delayedResponse() {
  return fromTimeline([
    { at: 0, kind: "value", value: "A" },
    { at: 100, kind: "value", value: "B" },
    { at: 150, kind: "complete" },
  ]).pipe(timeoutEach(250));
}

export function fallbackOnFailure() {
  return fromTimeline<string>([
    { at: 0, kind: "value", value: "cached" },
    { at: 50, kind: "error", error: new Error("upstream offline") },
  ]).pipe(catchError(() => of("fallback")));
}
// #endregion sources

// #region assertions
import assert from "node:assert/strict";

assert.deepEqual(await delayedResponse().toArray(), ["A", "B"]);
assert.deepEqual(await fallbackOnFailure().toArray(), ["cached", "fallback"]);
await assert.rejects(
  fromTimeline([
    { at: 50, kind: "error", error: new Error("offline") },
  ]).toArray(),
  /offline/,
);
// #endregion assertions
