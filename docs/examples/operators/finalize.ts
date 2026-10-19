import { finalize, of } from "mogami-stream";
const result: string[] = [];
await of("A")
  .pipe(
    finalize(() => {
      result.push("closed");
    }),
  )
  .forEach((value) => {
    result.push(value);
  });

export { result };
