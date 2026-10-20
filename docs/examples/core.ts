import { from, map, take } from "mogami-stream/core";

const values = await from([1, 2, 3])
  .pipe(
    map((value) => value * 2),
    take(2),
  )
  .toArray();

console.log(values); // [2, 4]
