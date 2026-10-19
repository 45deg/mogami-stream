import { filter, map, pipe } from "mogami-stream";
const result = await pipe(
  [1, 2, 3],
  filter((value) => value > 1),
  map((value) => value * 2),
).toArray();

export { result };
