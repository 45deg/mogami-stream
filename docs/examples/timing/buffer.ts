import { bufferTime, from } from "mogami-stream";

const batches = await from([1, 2, 3, 4, 5])
  .pipe(bufferTime(1_000, { maxItems: 2 }))
  .toArray();

console.log(batches); // [[1, 2], [3, 4], [5]]
