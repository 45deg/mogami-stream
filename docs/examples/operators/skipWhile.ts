import { from, skipWhile } from "mogami-stream";
const result = await from([1, 2, 3, 1])
  .pipe(skipWhile((value) => value < 3))
  .toArray();

export { result };
