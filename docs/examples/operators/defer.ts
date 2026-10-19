import { defer } from "mogami-stream";
let run = 0;
const source = defer(() => [++run]);
const result = [await source.toArray(), await source.toArray()];

export { result };
