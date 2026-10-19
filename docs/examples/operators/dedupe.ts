import { dedupe, from } from "mogami-stream";
const result = await from([1, 1, 2, 2, 1]).pipe(dedupe()).toArray();

export { result };
