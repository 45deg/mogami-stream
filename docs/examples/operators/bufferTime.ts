import { bufferTime, from } from "mogami-stream";
const result = await from([1, 2, 3])
  .pipe(bufferTime(1_000, { maxItems: 2 }))
  .toArray();

export { result };
