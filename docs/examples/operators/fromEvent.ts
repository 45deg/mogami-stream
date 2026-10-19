import { fromEvent, map, take } from "mogami-stream";
const target = new EventTarget();
const pending = fromEvent(target, "ready", { capacity: 1 })
  .pipe(
    map((event) => event.type),
    take(1),
  )
  .toArray();
target.dispatchEvent(new Event("ready"));
const result = await pending;

export { result };
