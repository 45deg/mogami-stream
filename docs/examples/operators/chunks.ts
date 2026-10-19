import { chunks, from } from "mogami-stream";
const result = await from([1, 2, 3, 4, 5]).pipe(chunks(2)).toArray();

export { result };
