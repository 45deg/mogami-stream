import { catchError, fromTimeline, of } from "mogami-stream";
const result = await fromTimeline([
  { at: 0, kind: "value", value: "cached" },
  { at: 10, kind: "error", error: new Error("offline") },
])
  .pipe(catchError(() => of("fallback")))
  .toArray();

export { result };
