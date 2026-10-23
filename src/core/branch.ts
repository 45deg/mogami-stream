import type {
  CloseReport,
  ExecutionOptions,
  StreamIterator,
} from "../terminal/iterate.js";

/**
 * A bounded endpoint that permits one iterator acquisition and one pending `next()`.
 * Derived streams consume the same endpoint; wrapping a branch does not make it reusable.
 */
export interface Branch<T> extends AsyncIterable<T> {
  [Symbol.asyncIterator](): StreamIterator<T>;
  /**
   * Acquires this branch once; a second acquisition throws `TypeError`.
   * Overlapping `next()` calls reject with `TypeError`. The signal cancels only this branch.
   * Iterator `return()` or a `for await` break detaches immediately; iterator `throw(error)`
   * fails only this endpoint without forwarding `throw` to the source.
   */
  iterate(options?: ExecutionOptions): StreamIterator<T>;
  /** Discards unread values immediately, without waiting for siblings or shared cleanup. */
  detach(): void;
  /** Cancels this branch. Its pending or next read rejects once; later reads return done. */
  cancel(reason?: unknown): void;
  /**
   * Endpoint completion, independent of siblings, downstream callbacks, and shared cleanup.
   * At EOF, the queue drains before completion. Shared failures discard active queues
   * and fail remaining endpoints without changing already completed reports.
   */
  readonly closed: Promise<CloseReport>;
}

export type Branches<V> = { readonly [K in keyof V]: Branch<V[K]> };
export type BranchReports<V> = { readonly [K in keyof V]: CloseReport };

export interface BranchGroupCloseReport<V> {
  /** Shared source, routing, and cleanup result; it can fail after endpoints complete. */
  readonly source: CloseReport;
  /** Endpoint results in the same array or key structure as the branches. Excludes downstream effects. */
  readonly branches: BranchReports<V>;
}

/**
 * Fixed endpoints sharing one lazy source execution and its cleanup.
 * The group owns the source iterator exclusively. All endpoints participate from
 * construction, including unread ones. Stopping before demand never acquires the source.
 * When the last endpoint leaves, the source stops; a failed or cancelled last endpoint
 * cancels it with that reason. Termination is idempotent.
 */
export interface BranchGroup<V> {
  readonly branches: Branches<V>;
  /** Cancels all endpoints before shared cleanup, aborting source work and routing callbacks. */
  cancel(reason?: unknown): void;
  /** Detaches every endpoint and returns `closed`, waiting for source work and cleanup. Never rejects. */
  close(): Promise<BranchGroupCloseReport<V>>;
  /**
   * Waits for every endpoint, outstanding source work and routing callbacks, and shared cleanup.
   * Never rejects and does not initiate a stop. Unread queues or work that ignores
   * cancellation can keep it pending while detached endpoints finish earlier.
   * Never await it from a source finalizer, since the group waits for that finalizer.
   */
  readonly closed: Promise<BranchGroupCloseReport<V>>;
}
