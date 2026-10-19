import { from, takeWhile } from "mogami-stream";
const result = await from([1, 2, 3, 1])
  .pipe(takeWhile((value) => value < 3))
  .toArray();

export { result };
