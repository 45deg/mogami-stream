import { fromTimeline } from "mogami-stream";
const result = await fromTimeline([
  { at: 0, kind: "value", value: "A" },
  { at: 10, kind: "value", value: "B" },
  { at: 15, kind: "complete" },
]).toArray();

export { result };
