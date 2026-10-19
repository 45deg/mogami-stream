import { chain } from "mogami-stream";
const result = await chain([1, 2], [3, 4]).toArray();

export { result };
