import {
  from,
  type StreamIterator,
  type CloseReport,
  type BranchGroupCloseReport,
} from "../../dist/index.js";
import { broadcast, route } from "../../dist/core/index.js";

const groups = [
  broadcast([1], { branches: 1, capacity: 1 }),
  route([1], { branches: 1, capacity: 1, select: () => 0 }),
  from([1]).broadcast({ branches: 1, capacity: 1 }),
  from([1]).route({ branches: 1, capacity: 1, select: () => 0 }),
];
for (const group of groups) {
  const branch = group.branches[0]!;
  const iterator: StreamIterator<number> = branch.iterate();
  const endpoint: Promise<CloseReport> = iterator.closed;
  const branchReport: Promise<CloseReport> = branch.closed;
  const shared: Promise<BranchGroupCloseReport<readonly number[]>> =
    group.closed;
  const closing: typeof shared = group.close();
  const returning: Promise<IteratorResult<number, unknown>> = iterator.return();
  void [endpoint, branchReport, shared, closing, returning];
}
