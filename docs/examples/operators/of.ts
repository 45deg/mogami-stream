import { of } from "mogami-stream";
const result = await of("A", "B").toArray();

export { result };
