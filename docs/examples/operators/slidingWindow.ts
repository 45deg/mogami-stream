import { from } from "mogami-stream";
const result = await from([1, 2, 3, 4]).slidingWindow(3).toArray();

export { result };
