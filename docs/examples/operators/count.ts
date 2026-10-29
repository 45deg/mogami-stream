import { from } from "mogami-stream";
const result = await from([1, 2, 3]).count();

export { result };
