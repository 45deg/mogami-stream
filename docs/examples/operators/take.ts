import { from, take } from "mogami-stream";
const result = await from([1, 2, 3]).pipe(take(2)).toArray();

export { result };
