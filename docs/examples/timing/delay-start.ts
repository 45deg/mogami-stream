import { delayStart, of } from "mogami-stream";

const stream = of("ready").pipe(delayStart(500));
console.log(await stream.toArray()); // ["ready"]
