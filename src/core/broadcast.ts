import type { Source } from "./stream.js";
import type { Branch, BranchGroup, BranchGroupCloseReport } from "./branch.js";
import type { ExecutionOptions } from "../terminal/iterate.js";
import { branchLayout } from "../internal/branch-layout.js";
import { createBranchHub } from "../internal/branch-hub.js";

export interface BroadcastOptions extends ExecutionOptions {
  /** Fixed number of one-shot branches; a positive safe integer. */
  readonly branches: number;
  /** Maximum queued values and reserved reads per branch; a positive safe integer. */
  readonly capacity: number;
}
export interface KeyedBroadcastOptions<
  K extends string,
> extends ExecutionOptions {
  /** Non-empty, unique string keys, captured at construction. */
  readonly branches: readonly K[];
  /** Maximum queued values and reserved reads per branch; a positive safe integer. */
  readonly capacity: number;
}
export type Broadcast<T> = BranchGroup<readonly T[]>;
export type BroadcastBranch<T> = Branch<T>;
export type BroadcastCloseReport = BranchGroupCloseReport<readonly unknown[]>;

/**
 * Shares each source value by reference across fixed, bounded, one-shot branches.
 * A full branch stops source reads; detach unused branches to release capacity.
 * Consume branches concurrently and inspect `closed` for shared cleanup failures.
 * Promise-valued items are not awaited. New reads require branch demand; existing
 * queues remain readable when a sibling is full. At most one source read is in flight.
 * Sustained reads periodically yield so timers and abort handlers can run.
 * @throws {RangeError} If the branch count, capacity, or their product is not a positive safe integer, or named keys are empty or duplicated.
 * @category Sources
 */
export function broadcast<T, const K extends readonly string[]>(
  source: Source<T>,
  options: KeyedBroadcastOptions<K[number]> & { readonly branches: K },
): BranchGroup<{ readonly [P in K[number]]: T }>;
export function broadcast<T>(
  source: Source<T>,
  options: BroadcastOptions,
): Broadcast<T>;
export function broadcast<T>(
  source: Source<T>,
  options: BroadcastOptions | KeyedBroadcastOptions<string>,
): BranchGroup<any> {
  const { branches, capacity, signal } = options;
  return createBranchHub(
    source,
    branchLayout(branches, capacity),
    capacity,
    signal,
  );
}
