import { forEach } from "mogami-stream";
const result: number[] = [];
await forEach([1, 2, 3], async (value) => {
  result.push(value * 2);
});

export { result };
