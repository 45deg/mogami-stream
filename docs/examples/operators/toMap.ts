import { from } from "mogami-stream";
const result = [...(await from([1, 2, 3]).toMap((value) => value % 2))];

export { result };
