import { fromTimeline, merge, of } from "mogami-stream";
const slow = fromTimeline([{ at: 20, kind: "value", value: "slow" }]);
const result = await merge(slow, of("fast")).toArray();

export { result };
