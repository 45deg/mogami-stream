import { from } from "mogami-stream";
const result = await from([1, 2, 3, 4, 5]).slice(1, -1).toArray();

export { result };
