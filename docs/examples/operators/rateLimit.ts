import { from, rateLimit } from "mogami-stream";
const result = await from([1, 2, 3])
  .pipe(rateLimit({ count: 2, perMs: 100 }))
  .toArray();

export { result };
