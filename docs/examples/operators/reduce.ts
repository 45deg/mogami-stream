import { reduce } from "mogami-stream";
const result = await reduce([1, 2, 3], (sum, value) => sum + value, 0);

export { result };
