import { from } from "mogami-stream";
let result = 0;
await from([1, 2, 3])
  .tap((value) => {
    result += value;
  })
  .drain();

export { result };
