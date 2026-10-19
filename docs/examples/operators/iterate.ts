import { from } from "mogami-stream";
const iterator = from([1, 2, 3]).iterate();
const first = await iterator.next();
await iterator.return();
const result = { value: first.value, kind: (await iterator.closed).kind };

export { result };
