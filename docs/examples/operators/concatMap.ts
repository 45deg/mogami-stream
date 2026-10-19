import { concatMap, from, fromTimeline } from "mogami-stream";
const result = await from([1, 2])
  .pipe(
    concatMap(
      (value) =>
        fromTimeline([{ at: value === 1 ? 20 : 0, kind: "value", value }]),
      { concurrency: 2 },
    ),
  )
  .toArray();

export { result };
