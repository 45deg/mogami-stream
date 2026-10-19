import { range } from "mogami-stream";
const result = await range(1, 6, 2).toArray();

export { result };
