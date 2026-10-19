import { from, map } from "mogami-stream";
const result = await from([1, 2, 3])
  .pipe(map(async (value) => value * 2, { concurrency: 2 }))
  .toArray();

export { result };
