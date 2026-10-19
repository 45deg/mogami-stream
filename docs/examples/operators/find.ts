import { find } from "mogami-stream";
const result = await find([1, 2, 3], (value) => value > 1);

export { result };
