import { catchError, fromTimeline, of, timeoutEach } from "mogami-stream";
const result = await fromTimeline([{ at: 100, kind: "value", value: "late" }])
  .pipe(
    timeoutEach(10),
    catchError(() => of("timeout")),
  )
  .toArray();

export { result };
