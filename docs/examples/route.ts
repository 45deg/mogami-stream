import { from } from "mogami-stream";

type ImageEvent = { kind: "image"; width: number };
type TextEvent = { kind: "text"; text: string };
type Event = ImageEvent | TextEvent;

const events: Event[] = [
  { kind: "image", width: 640 },
  { kind: "text", text: "hello" },
];
const stop = new AbortController();
const group = from(events).route({
  branches: {
    image: (event): event is ImageEvent => event.kind === "image",
    text: (event): event is TextEvent => event.kind === "text",
    other: () => true,
  },
  capacity: 8,
  default: "other",
  fallback: "error",
  signal: stop.signal,
});
const jobs = [
  group.branches.image
    .map((event) => event.width)
    .toArray({ signal: stop.signal }),
  group.branches.text
    .map((event) => event.text)
    .toArray({ signal: stop.signal }),
  group.branches.other.toArray({ signal: stop.signal }),
].map((job) =>
  job.catch((error: unknown) => {
    stop.abort(error);
    throw error;
  }),
);

try {
  console.log(await Promise.all(jobs)); // [[640], ["hello"], []]
} finally {
  const closing = group.close();
  await Promise.allSettled(jobs);
  const report = await closing;
  if (report.source.kind !== "completed") {
    console.error(report.source.reason, report.source.suppressedErrors);
  }
}
