import { concat, from, fromTimeline, of } from "mogami-stream";
const children = [
  fromTimeline([{ at: 20, kind: "value", value: "slow" }]),
  of("fast"),
];
const result = await from(children)
  .pipe(concat({ concurrency: 2 }))
  .toArray();

export { result };
