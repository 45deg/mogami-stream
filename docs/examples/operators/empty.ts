import { empty } from "mogami-stream";
const result = await empty<number>().toArray();

export { result };
