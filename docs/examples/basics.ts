import { from } from "mogami-stream";

const doubled = from([1, 2, 3]).map((value) => value * 2);

const result = await doubled.toArray();
console.log(result); // [2, 4, 6]
