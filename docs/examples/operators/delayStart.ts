import { delayStart, of } from "mogami-stream";
const result = await of("ready").pipe(delayStart(20)).toArray();

export { result };
