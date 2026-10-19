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
