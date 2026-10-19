import { catchError, deadline, fromTimeline, of } from "mogami-stream";
const result = await fromTimeline([{ at: 100, kind: "value", value: "late" }])
  .pipe(
    deadline(10),
    catchError(() => of("deadline")),
  )
  .toArray();

export { result };
