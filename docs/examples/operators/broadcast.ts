// #region example
import { broadcast } from "mogami-stream";

const group = broadcast([1, 2, 3], { branches: 2, capacity: 2 });
const result = await Promise.all(
  group.branches.map((branch) => branch.toArray()),
);
await group.closed;

// #endregion example

export { result };
