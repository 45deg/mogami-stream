import { from, tap } from "mogami-stream";
const seen: number[] = [];
const values = await from([1, 2])
  .pipe(
    tap((value) => {
      seen.push(value);
    }),
  )
  .toArray();
const result = { values, seen };

export { result };
