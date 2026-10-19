import { from, mapSettled } from "mogami-stream";
const result = await from([1, 2, 3])
  .pipe(
    mapSettled((value) =>
      value === 2 ? Promise.reject("unavailable") : Promise.resolve(value * 10),
    ),
  )
  .toArray();

export { result };
