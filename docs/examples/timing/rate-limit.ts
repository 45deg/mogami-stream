import { from, rateLimit } from "mogami-stream";

const values = await from([1, 2, 3, 4, 5])
  .pipe(rateLimit({ count: 2, perMs: 1_000 }))
  .toArray();

console.log(values); // [1, 2, 3, 4, 5]
