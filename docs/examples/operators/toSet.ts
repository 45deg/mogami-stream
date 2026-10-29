import { from } from "mogami-stream";
const result = [...(await from([1, 2, 1]).toSet())];

export { result };
