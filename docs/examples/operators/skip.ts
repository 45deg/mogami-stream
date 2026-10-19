import { from, skip } from "mogami-stream";
const result = await from([1, 2, 3]).pipe(skip(1)).toArray();

export { result };
