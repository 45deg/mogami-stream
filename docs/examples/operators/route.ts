// #region example
import { from } from "mogami-stream";

const group = from([1, 2, 3, 4]).route({
  branches: ["odd", "even"],
  capacity: 2,
  select: (value) => (value % 2 ? "odd" : "even"),
});
const result = await Promise.all([
  group.branches.odd.toArray(),
  group.branches.even.toArray(),
]);
await group.closed;

// #endregion example

export { result };
