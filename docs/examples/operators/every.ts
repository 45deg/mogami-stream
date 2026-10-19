import { every } from "mogami-stream";
const result = await every([1, 2, 3], (value) => value > 0);

export { result };
