import { some } from "mogami-stream";
const result = await some([1, 2, 3], (value) => value > 2);

export { result };
