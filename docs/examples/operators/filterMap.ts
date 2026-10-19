import { filterMap, from } from "mogami-stream";
const result = await from(["12", "?", "0"])
  .pipe(
    filterMap((text) => {
      const value = Number(text);
      return Number.isNaN(value) ? undefined : value;
    }),
  )
  .toArray();

export { result };
