import {
  DeadlineExceededError,
  deadline,
  fromTimeline,
  timeoutEach,
} from "mogami-stream";

const source = fromTimeline([
  { at: 0, kind: "value", value: "A" },
  { at: 800, kind: "value", value: "B" },
  { at: 1_600, kind: "value", value: "C" },
]);

console.log(await source.pipe(timeoutEach(1_000)).toArray()); // ["A", "B", "C"]

try {
  await source.pipe(deadline(1_500)).toArray();
} catch (error) {
  if (!(error instanceof DeadlineExceededError)) throw error;
  console.log(error.name); // "DeadlineExceededError"
}
