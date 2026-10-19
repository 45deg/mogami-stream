import { fromTimeline, of, zipLatest } from "mogami-stream";
const numbers = fromTimeline([
  { at: 0, kind: "value", value: 1 },
  { at: 20, kind: "value", value: 2 },
]);
const result = await zipLatest([numbers, of("A")]).toArray();

export { result };
