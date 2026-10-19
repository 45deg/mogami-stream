import { from, spaceout } from "mogami-stream";
const result = await from([1, 2, 3]).pipe(spaceout(20)).toArray();

export { result };
