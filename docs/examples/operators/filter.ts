import { filter, from } from "mogami-stream";
const result = await from([1, 2, 3, 4])
  .pipe(filter((value) => value % 2 === 0))
  .toArray();

export { result };
