import { from, scan } from "mogami-stream";
const result = await from([1, 2, 3])
  .pipe(scan((sum, value) => sum + value, 0))
  .toArray();

export { result };
