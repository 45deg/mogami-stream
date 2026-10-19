import { from, spaceout } from "mogami-stream";

const values = await from([1, 2, 3, 4, 5]).pipe(spaceout(500)).toArray();

console.log(values); // [1, 2, 3, 4, 5]
