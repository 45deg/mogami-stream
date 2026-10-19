import { zip } from "mogami-stream";
const result = await zip([1, 2, 3], ["A", "B"]).toArray();

export { result };
