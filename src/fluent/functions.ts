import * as core from "../core/index.js";
import {
  wrap,
  wrapBranchGroup,
  type FluentBranchGroup,
  type Stream,
} from "./stream.js";
import type {
  Source,
  OperatorContext,
  ResourceContext,
  MapOptions,
  MapContext,
  SettledResult,
  HigherOrderOptions,
  RateLimitOptions,
  BufferTimeOptions,
  TimelineEvent,
  PushOptions,
  PushObserver,
  SourceValues,
  PartialSourceValues,
  ZipLatestOptions,
} from "../core/index.js";

/** A transformation whose result supports fluent methods. */
export type Operator<T, U> = (source: AsyncIterable<T>) => Stream<U>;

/**
 * Wraps a synchronous or asynchronous iterable into a {@link Stream} without altering its replayability or unwrapping Promise values.
 *
 * The underlying iterator is not acquired until the first read. Single-use iterators remain single-use.
 * @example
 * ```ts
 * const values = await from([1, 2, 3]).toArray();
 * ```
 * @category Sources
 */
export function from<T>(source: Source<T>): Stream<T> {
  return wrap(core.from<T>(source));
}

/**
 * Creates a fresh source on the first read of each execution.
 *
 * The factory may return a source directly or a `PromiseLike` that resolves to one. Pass `context.signal` to any external I/O so cancellation can abort in-flight setup. Use `context.acquire(acquire, release)` for resources opened during setup; plain sources returned after cancellation are never opened or closed by the library.
 * @category Sources
 */
export function defer<T>(
  factory: (context: ResourceContext) => Source<T> | PromiseLike<Source<T>>,
): Stream<T> {
  return wrap(core.defer<T>(factory));
}

/**
 * Creates a replayable stream that emits the given values in order.
 * @category Sources
 */
export function of<T>(...values: T[]): Stream<T> {
  return wrap(core.of<T>(...values));
}

/**
 * Creates a stream that completes immediately without emitting any values.
 * @category Sources
 */
export function empty<T = never>(): Stream<T> {
  return wrap(core.empty<T>());
}

/**
 * Emits safe integers starting at `start` and stepping by `step` toward (but excluding) `end`.
 *
 * Use a negative `step` for a descending range. If `step` moves away from `end`, the stream completes immediately without emitting values.
 * @throws {RangeError} If `start`, `end`, or `step` is not a safe integer, or if `step` is `0`.
 * @category Sources
 */
export function range(start: number, end: number, step = 1): Stream<number> {
  return wrap(core.range(start, end, step));
}

/**
 * Maps each value sequentially and emits only results that are neither `null` nor `undefined`.
 *
 * Other falsy values (`0`, `false`, `""`, `NaN`) are kept.
 * @category Transformation
 */
export function filterMap<T, U>(
  mapper: (value: T, context: OperatorContext) => U,
): Operator<T, NonNullable<Awaited<U>>> {
  const operator = core.filterMap<T, U>(mapper);
  return (source) => wrap(operator(source));
}

/**
 * Drops consecutive values that produce the same key according to `Object.is`.
 *
 * When `keySelector` is omitted, values are compared directly (preserving Promise identity). When provided, `keySelector` is called and awaited sequentially for each item, and only the last emitted key is retained in memory.
 * @category Transformation
 */
export function dedupe<T, K = T>(
  keySelector?: (value: T, context: OperatorContext) => K,
): Operator<T, T> {
  const operator = core.dedupe<T, K>(keySelector);
  return (source) => wrap(operator(source));
}

/**
 * Emits only values for which `predicate` returns `true`.
 *
 * Predicates run sequentially and may be asynchronous. Synchronous type guard predicates narrow the output stream type.
 * @category Transformation
 */
export function filter<T, S extends T>(
  predicate: (value: T, context: OperatorContext) => value is S,
): Operator<T, S>;
export function filter<T>(
  predicate: (
    value: T,
    context: OperatorContext,
  ) => boolean | PromiseLike<boolean>,
): Operator<T, T>;
export function filter<T>(
  predicate: (
    value: T,
    context: OperatorContext,
  ) => boolean | PromiseLike<boolean>,
): Operator<T, T> {
  const operator = core.filter<T>(predicate);
  return (source) => wrap(operator(source));
}

/**
 * Groups consecutive values into arrays of up to `size` items.
 *
 * If the stream completes normally with fewer than `size` buffered items, the remaining items are emitted as a final smaller array.
 * @throws {RangeError} If `size` is not a positive safe integer.
 * @category Transformation
 */
export function chunks(
  size: number,
): <T>(source: AsyncIterable<T>) => Stream<T[]>;
export function chunks<T>(size: number): Operator<T, T[]>;
export function chunks(
  size: number,
): <T>(source: AsyncIterable<T>) => Stream<T[]> {
  const operator = core.chunks(size);
  return (source) => wrap(operator(source));
}

/**
 * Reduces values sequentially and emits the updated accumulator after each item.
 *
 * `initial` is awaited before opening the upstream source and is not emitted on its own (an empty source emits nothing). Objects resolved from `initial` are used by reference.
 * @category Transformation
 */
export function scan<T, A>(
  reducer: (
    accumulator: Awaited<A>,
    value: T,
    context: OperatorContext,
  ) => Awaited<A> | PromiseLike<Awaited<A>>,
  initial: A,
): Operator<T, Awaited<A>> {
  const operator = core.scan<T, A>(reducer, initial);
  return (source) => wrap(operator(source));
}

/**
 * Skips leading values while `predicate` returns `true`, then emits the first non-matching value and all subsequent values without calling `predicate` again.
 *
 * Predicates run sequentially and may be asynchronous.
 * @category Selection
 */
export function skipWhile<T>(
  predicate: (
    value: T,
    context: OperatorContext,
  ) => boolean | PromiseLike<boolean>,
): Operator<T, T> {
  const operator = core.skipWhile<T>(predicate);
  return (source) => wrap(operator(source));
}

/**
 * Emits at most `count` values, then closes upstream.
 *
 * Upstream cleanup begins before the final (`count`-th) value is delivered, while downstream callback signals remain active. `take(0)` completes immediately without opening upstream.
 * @throws {RangeError} If `count` is not a non-negative safe integer.
 * @category Selection
 */
export function take(count: number): <T>(source: AsyncIterable<T>) => Stream<T>;
export function take<T>(count: number): Operator<T, T>;
export function take(
  count: number,
): <T>(source: AsyncIterable<T>) => Stream<T> {
  const operator = core.take(count);
  return (source) => wrap(operator(source));
}

/**
 * Skips the first `count` values and emits the remaining values.
 * @throws {RangeError} If `count` is not a non-negative safe integer.
 * @category Selection
 */
export function skip(count: number): <T>(source: AsyncIterable<T>) => Stream<T>;
export function skip<T>(count: number): Operator<T, T>;
export function skip(
  count: number,
): <T>(source: AsyncIterable<T>) => Stream<T> {
  const operator = core.skip(count);
  return (source) => wrap(operator(source));
}

/**
 * Emits values while `predicate` returns `true`, stopping and closing upstream on the first `false` result.
 *
 * Predicates run sequentially and may be asynchronous. Synchronous type guard predicates narrow the output stream type. The first non-matching value is consumed by the test and is not emitted.
 * @category Selection
 */
export function takeWhile<T, S extends T>(
  predicate: (value: T, context: OperatorContext) => value is S,
): Operator<T, S>;
export function takeWhile<T>(
  predicate: (
    value: T,
    context: OperatorContext,
  ) => boolean | PromiseLike<boolean>,
): Operator<T, T>;
export function takeWhile<T>(
  predicate: (
    value: T,
    context: OperatorContext,
  ) => boolean | PromiseLike<boolean>,
): Operator<T, T> {
  const operator = core.takeWhile<T>(predicate);
  return (source) => wrap(operator(source));
}

/**
 * Recovers from an upstream failure by switching once to a fallback source returned by `handler`.
 *
 * Waits for the failed upstream stage to finish cleaning up before calling `handler`. Execution cancellation, cleanup errors, downstream errors, and errors thrown by `handler` or the replacement source are not caught. `handler` may return a source or a promise of one, and receives the active stage's `AbortSignal`. Use `context.acquire(acquire, release)` for resources opened during recovery; plain fallback sources returned after cancellation are never opened or closed by the library.
 * @category Lifecycle
 */
export function catchError<U>(
  handler: (
    error: unknown,
    context: ResourceContext,
  ) => Source<U> | PromiseLike<Source<U>>,
): <T>(source: AsyncIterable<T>) => Stream<T | U> {
  const operator = core.catchError<U>(handler);
  return (source) => wrap(operator(source));
}

/**
 * Concatenates multiple sources sequentially, waiting for each source to close before opening the next.
 * @category Combination
 */
export function chain<const S extends readonly Source<unknown>[]>(
  ...sources: S
): Stream<SourceValues<S>[number]>;
export function chain<T>(...sources: Source<T>[]): Stream<T>;
export function chain(...sources: Source<unknown>[]): Stream<unknown> {
  return wrap(core.chain(...sources));
}

/**
 * Merges multiple sources concurrently, holding at most one pending read or buffered result per source.
 *
 * Ready sources are polled in round-robin order, and no new reads start while downstream is idle.
 * @category Combination
 */
export function merge<const S extends readonly Source<unknown>[]>(
  ...sources: S
): Stream<SourceValues<S>[number]>;
export function merge<T>(...sources: Source<T>[]): Stream<T>;
export function merge(...sources: Source<unknown>[]): Stream<unknown> {
  return wrap(core.merge(...sources));
}

/**
 * Combines corresponding values from each source into typed tuples, stopping as soon as any source completes.
 *
 * Remaining sources are closed when the shortest source finishes. Detecting that a source has ended may require reading one extra value from sibling sources.
 * @category Combination
 */
export function zip<const S extends readonly Source<unknown>[]>(
  ...sources: S
): Stream<SourceValues<S>> {
  return wrap(core.zip<S>(...sources));
}

/**
 * Emits tuples containing the latest value observed from each source.
 *
 * By default (`partial: false`), waits until every source has emitted at least once before producing a tuple. With `partial: true`, emits immediately and fills unseen positions with `undefined`. Updates arriving before the first tuple is ready may coalesce; afterward, reads are driven by downstream demand.
 * @category Combination
 */
export function zipLatest<const S extends readonly Source<unknown>[]>(
  sources: S,
  options: { readonly partial: true },
): Stream<PartialSourceValues<S>>;
export function zipLatest<const S extends readonly Source<unknown>[]>(
  sources: S,
  options?: { readonly partial?: false },
): Stream<SourceValues<S>>;
export function zipLatest<const S extends readonly Source<unknown>[]>(
  sources: S,
  options: ZipLatestOptions,
): Stream<PartialSourceValues<S>>;
export function zipLatest(
  sources: readonly Source<unknown>[],
  options: ZipLatestOptions = {},
): Stream<unknown[]> {
  return wrap(core.zipLatest(sources, options));
}

/**
 * Flattens a stream of child sources, interleaving values when `concurrency > 1`.
 *
 * Each active child holds a single read/result slot and occupies a concurrency slot until its cleanup finishes.
 * A child's cleanup does not block values from other active children.
 * @throws {RangeError} If `concurrency` is not a positive safe integer.
 * @category Higher-order
 */
export function flatten<T>(
  options: HigherOrderOptions = {},
): Operator<Source<T>, T> {
  const operator = core.flatten<T>(options);
  return (source) => wrap(operator(source));
}

/**
 * Flattens a stream of child sources strictly in child order, while optionally opening up to `concurrency` children ahead.
 *
 * Later children that complete early retain their concurrency slots until earlier children finish, keeping buffered work bounded behind a slow leading child.
 * @throws {RangeError} If `concurrency` is not a positive safe integer.
 * @category Higher-order
 */
export function concat<T>(
  options: HigherOrderOptions = {},
): Operator<Source<T>, T> {
  const operator = core.concat<T>(options);
  return (source) => wrap(operator(source));
}

/**
 * Maps each value synchronously to a child source and flattens the children concurrently.
 *
 * Wrap asynchronous setup inside {@link defer} within the returned child source. Defaults to `concurrency: 1`.
 * @throws {RangeError} If `concurrency` is not a positive safe integer.
 * @category Higher-order
 */
export function flatMap<T, U>(
  mapper: (value: T, context: OperatorContext) => Source<U>,
  options: HigherOrderOptions = {},
): Operator<T, U> {
  const operator = core.flatMap<T, U>(mapper, options);
  return (source) => wrap(operator(source));
}

/**
 * Maps each value synchronously to a child source and emits values in child order.
 *
 * Wrap asynchronous setup inside {@link defer} within the returned child source. Defaults to `concurrency: 1`.
 * @throws {RangeError} If `concurrency` is not a positive safe integer.
 * @category Higher-order
 */
export function concatMap<T, U>(
  mapper: (value: T, context: OperatorContext) => Source<U>,
  options: HigherOrderOptions = {},
): Operator<T, U> {
  const operator = core.concatMap<T, U>(mapper, options);
  return (source) => wrap(operator(source));
}

/**
 * Waits `ms` milliseconds on the first read before opening upstream.
 * @throws {RangeError} If `ms` is not finite and non-negative.
 * @category Timing
 */
export function delayStart(
  ms: number,
): <T>(source: AsyncIterable<T>) => Stream<T>;
export function delayStart<T>(ms: number): Operator<T, T>;
export function delayStart(
  ms: number,
): <T>(source: AsyncIterable<T>) => Stream<T> {
  const operator = core.delayStart(ms);
  return (source) => wrap(operator(source));
}

/**
 * Emits the first value without added delay, then ensures at least `intervalMs` milliseconds between subsequent emissions.
 *
 * Time spent waiting for upstream reads or downstream demand counts toward the interval.
 * @throws {RangeError} If `intervalMs` is not finite and non-negative.
 * @category Timing
 */
export function spaceout(
  intervalMs: number,
): <T>(source: AsyncIterable<T>) => Stream<T>;
export function spaceout<T>(intervalMs: number): Operator<T, T>;
export function spaceout(
  intervalMs: number,
): <T>(source: AsyncIterable<T>) => Stream<T> {
  const operator = core.spaceout(intervalMs);
  return (source) => wrap(operator(source));
}

/**
 * Limits how many upstream reads can start within a sliding window of `perMs` milliseconds.
 *
 * Every upstream `next()` call counts toward the limit, including reads that fail or complete the stream. Reads separated by `perMs` or more do not share a window. Retries inside {@link map} do not trigger new upstream reads and are not counted here.
 * @throws {RangeError} If `count` is not a positive safe integer or `perMs` is not finite and positive.
 * @category Timing
 */
export function rateLimit(
  options: RateLimitOptions,
): <T>(source: AsyncIterable<T>) => Stream<T>;
export function rateLimit<T>(options: RateLimitOptions): Operator<T, T>;
export function rateLimit(
  options: RateLimitOptions,
): <T>(source: AsyncIterable<T>) => Stream<T> {
  const operator = core.rateLimit(options);
  return (source) => wrap(operator(source));
}

/**
 * Fails with {@link TimeoutError} if any individual upstream read reaches its `ms` millisecond deadline.
 *
 * Elapsed time is checked when a read returns a value or completes the stream, even if the timer callback is delayed. A result at exactly the deadline is rejected. Idle time between downstream reads does not count. `timeoutEach(0)` fails on the first read without opening upstream.
 * Terminal consumers still wait for in-flight work and cleanup before settling.
 * @throws {RangeError} If `ms` is not finite and non-negative.
 * @category Timing
 */
export function timeoutEach(
  ms: number,
): <T>(source: AsyncIterable<T>) => Stream<T>;
export function timeoutEach<T>(ms: number): Operator<T, T>;
export function timeoutEach(
  ms: number,
): <T>(source: AsyncIterable<T>) => Stream<T> {
  const operator = core.timeoutEach(ms);
  return (source) => wrap(operator(source));
}

/**
 * Fails with {@link DeadlineExceededError} if the stage remains open longer than `durationMs` milliseconds after its first read.
 *
 * Elapsed time includes both upstream reads and downstream idle time. `deadline(0)` fails immediately without opening upstream.
 * @throws {RangeError} If `durationMs` is not finite and non-negative.
 * @category Timing
 */
export function deadline(
  durationMs: number,
): <T>(source: AsyncIterable<T>) => Stream<T>;
export function deadline<T>(durationMs: number): Operator<T, T>;
export function deadline(
  durationMs: number,
): <T>(source: AsyncIterable<T>) => Stream<T> {
  const operator = core.deadline(durationMs);
  return (source) => wrap(operator(source));
}

/**
 * Collects values into non-empty arrays, flushing when `maxItems` is reached or `windowMs` milliseconds have elapsed since the batch's first item arrived.
 *
 * On normal completion, any remaining buffered items are emitted as a final batch. At most one upstream read remains pending when a window flushes, so batches do not accumulate in the background while downstream is idle.
 * @throws {RangeError} If `windowMs` is not finite and positive, or `maxItems` is not a positive safe integer.
 * @category Timing
 */
export function bufferTime(
  windowMs: number,
  options: BufferTimeOptions,
): <T>(source: AsyncIterable<T>) => Stream<T[]>;
export function bufferTime<T>(
  windowMs: number,
  options: BufferTimeOptions,
): Operator<T, T[]>;
export function bufferTime(
  windowMs: number,
  options: BufferTimeOptions,
): <T>(source: AsyncIterable<T>) => Stream<T[]> {
  const operator = core.bufferTime(windowMs, options);
  return (source) => wrap(operator(source));
}

/**
 * Runs `effect` sequentially for each value and emits the original value unchanged.
 * @category Lifecycle
 */
export function tap<T>(
  effect: (value: T, context: OperatorContext) => unknown,
): Operator<T, T> {
  const operator = core.tap<T>(effect);
  return (source) => wrap(operator(source));
}

/**
 * Registers a cleanup callback that runs once when the stage closes, if the stage was started.
 *
 * The callback runs after upstream stages and in-flight operations in the stage have finished. Nested `finalize` callbacks run from inner stages outward.
 * @category Lifecycle
 */
export function finalize(
  effect: () => unknown,
): <T>(source: AsyncIterable<T>) => Stream<T>;
export function finalize<T>(effect: () => unknown): Operator<T, T>;
export function finalize(
  effect: () => unknown,
): <T>(source: AsyncIterable<T>) => Stream<T> {
  const operator = core.finalize(effect);
  return (source) => wrap(operator(source));
}

/**
 * Creates a replayable, pull-driven stream from a timed sequence of events, intended for testing and mocking.
 *
 * Event timestamps (`at`) are finite, non-negative millisecond offsets relative to the first read of each execution, and must be in non-decreasing order. Events with identical `at` times preserve their array order, and a `"complete"` or `"error"` event may only appear last. Event records are shallow-copied at construction time while preserving value and error references. No timers run while downstream is idle.
 * @throws {RangeError} If timestamps are invalid, out of order, or a terminal event appears before the end.
 * @throws {TypeError} If an event has an unknown `kind`.
 * @category Sources
 */
export function fromTimeline<T>(
  events: readonly TimelineEvent<T>[],
): Stream<T> {
  return wrap(core.fromTimeline<T>(events));
}

/**
 * Transforms each value with bounded concurrency, awaiting the mapper's result.
 *
 * Defaults to `concurrency: 1`, `capacity: concurrency`, and `ordered: true`. An unhandled mapper error immediately stops the execution, even if earlier items are still in flight. When `retry` is configured, only the failed mapper invocation for that item is retried while holding its concurrency and capacity slot.
 * @throws {RangeError} If `concurrency`, `capacity`, or `retry` options are invalid.
 * @example
 * ```ts
 * const values = await range(0, 3)
 *   .pipe(map(value => value * 2, { concurrency: 2 }))
 *   .toArray(); // [0, 2, 4]
 * ```
 * @category Mapping
 */
export function map<T, U>(
  mapper: (value: T, context: MapContext) => U,
  options: MapOptions = {},
): Operator<T, Awaited<U>> {
  const operator = core.map<T, U>(mapper, options);
  return (source) => wrap(operator(source));
}

/**
 * Maps each input to a `{ status: "fulfilled", value }` or `{ status: "rejected", reason }` outcome after applying any retry policy.
 *
 * Mapper errors are captured as `"rejected"` outcomes without stopping other items. Upstream errors, cleanup failures, and execution cancellation still terminate the stream as usual. Concurrency bounds and ordering match {@link map}.
 * @throws {RangeError} If `concurrency`, `capacity`, or `retry` options are invalid.
 * @category Mapping
 */
export function mapSettled<T, U>(
  mapper: (value: T, context: MapContext) => U,
  options: MapOptions = {},
): Operator<T, SettledResult<Awaited<U>>> {
  const operator = core.mapSettled<T, U>(mapper, options);
  return (source) => wrap(operator(source));
}

/**
 * Converts a callback producer to a bounded stream. Subscribes on the first read
 * of each execution. The returned cleanup runs once on completion, failure, or
 * cancellation. Synchronous emissions during subscribe count toward capacity.
 * No producer calls are awaited: use an explicit drop policy for lossy events.
 * @category Sources
 */
export function fromCallback<T>(
  subscribe: (
    observer: PushObserver<T>,
  ) => void | (() => void | PromiseLike<void>),
  options: PushOptions,
): Stream<T> {
  return wrap(core.fromCallback<T>(subscribe, options));
}

/**
 * Converts EventTarget events (including WebSocket events) to a bounded stream.
 * Each execution installs one listener on first demand and removes it on close.
 * Events do not complete automatically; use take(), cancellation, or fromCallback
 * when the source has a separate completion event.
 * @category Sources
 */
export function fromEvent<T extends Event = Event>(
  target: EventTarget,
  type: string,
  options: PushOptions,
): Stream<T> {
  return wrap(core.fromEvent<T>(target, type, options));
}

export function pipe<T>(source: Source<T>): Stream<T>;
export function pipe<T, A>(
  source: Source<T>,
  op1: core.Operator<T, A>,
): Stream<A>;
export function pipe<T, A, B>(
  source: Source<T>,
  op1: core.Operator<T, A>,
  op2: core.Operator<A, B>,
): Stream<B>;
export function pipe<T, A, B, C>(
  source: Source<T>,
  op1: core.Operator<T, A>,
  op2: core.Operator<A, B>,
  op3: core.Operator<B, C>,
): Stream<C>;
export function pipe<T, A, B, C, D>(
  source: Source<T>,
  op1: core.Operator<T, A>,
  op2: core.Operator<A, B>,
  op3: core.Operator<B, C>,
  op4: core.Operator<C, D>,
): Stream<D>;
export function pipe<T, A, B, C, D, E>(
  source: Source<T>,
  op1: core.Operator<T, A>,
  op2: core.Operator<A, B>,
  op3: core.Operator<B, C>,
  op4: core.Operator<C, D>,
  op5: core.Operator<D, E>,
): Stream<E>;
export function pipe<T, A, B, C, D, E, F>(
  source: Source<T>,
  op1: core.Operator<T, A>,
  op2: core.Operator<A, B>,
  op3: core.Operator<B, C>,
  op4: core.Operator<C, D>,
  op5: core.Operator<D, E>,
  op6: core.Operator<E, F>,
): Stream<F>;
export function pipe<T, A, B, C, D, E, F, G>(
  source: Source<T>,
  op1: core.Operator<T, A>,
  op2: core.Operator<A, B>,
  op3: core.Operator<B, C>,
  op4: core.Operator<C, D>,
  op5: core.Operator<D, E>,
  op6: core.Operator<E, F>,
  op7: core.Operator<F, G>,
): Stream<G>;
export function pipe<T, A, B, C, D, E, F, G, H>(
  source: Source<T>,
  op1: core.Operator<T, A>,
  op2: core.Operator<A, B>,
  op3: core.Operator<B, C>,
  op4: core.Operator<C, D>,
  op5: core.Operator<D, E>,
  op6: core.Operator<E, F>,
  op7: core.Operator<F, G>,
  op8: core.Operator<G, H>,
): Stream<H>;
export function pipe<T, A, B, C, D, E, F, G, H, I>(
  source: Source<T>,
  op1: core.Operator<T, A>,
  op2: core.Operator<A, B>,
  op3: core.Operator<B, C>,
  op4: core.Operator<C, D>,
  op5: core.Operator<D, E>,
  op6: core.Operator<E, F>,
  op7: core.Operator<F, G>,
  op8: core.Operator<G, H>,
  op9: core.Operator<H, I>,
): Stream<I>;
export function pipe<T, A, B, C, D, E, F, G, H, I, J>(
  source: Source<T>,
  op1: core.Operator<T, A>,
  op2: core.Operator<A, B>,
  op3: core.Operator<B, C>,
  op4: core.Operator<C, D>,
  op5: core.Operator<D, E>,
  op6: core.Operator<E, F>,
  op7: core.Operator<F, G>,
  op8: core.Operator<G, H>,
  op9: core.Operator<H, I>,
  op10: core.Operator<I, J>,
): Stream<J>;
export function pipe<T, A, B, C, D, E, F, G, H, I, J, K>(
  source: Source<T>,
  op1: core.Operator<T, A>,
  op2: core.Operator<A, B>,
  op3: core.Operator<B, C>,
  op4: core.Operator<C, D>,
  op5: core.Operator<D, E>,
  op6: core.Operator<E, F>,
  op7: core.Operator<F, G>,
  op8: core.Operator<G, H>,
  op9: core.Operator<H, I>,
  op10: core.Operator<I, J>,
  op11: core.Operator<J, K>,
): Stream<K>;
export function pipe<T, A, B, C, D, E, F, G, H, I, J, K, L>(
  source: Source<T>,
  op1: core.Operator<T, A>,
  op2: core.Operator<A, B>,
  op3: core.Operator<B, C>,
  op4: core.Operator<C, D>,
  op5: core.Operator<D, E>,
  op6: core.Operator<E, F>,
  op7: core.Operator<F, G>,
  op8: core.Operator<G, H>,
  op9: core.Operator<H, I>,
  op10: core.Operator<I, J>,
  op11: core.Operator<J, K>,
  op12: core.Operator<K, L>,
): Stream<L>;
export function pipe<T, A, B, C, D, E, F, G, H, I, J, K, L, M>(
  source: Source<T>,
  op1: core.Operator<T, A>,
  op2: core.Operator<A, B>,
  op3: core.Operator<B, C>,
  op4: core.Operator<C, D>,
  op5: core.Operator<D, E>,
  op6: core.Operator<E, F>,
  op7: core.Operator<F, G>,
  op8: core.Operator<G, H>,
  op9: core.Operator<H, I>,
  op10: core.Operator<I, J>,
  op11: core.Operator<J, K>,
  op12: core.Operator<K, L>,
  op13: core.Operator<L, M>,
): Stream<M>;
export function pipe<T, A, B, C, D, E, F, G, H, I, J, K, L, M, N>(
  source: Source<T>,
  op1: core.Operator<T, A>,
  op2: core.Operator<A, B>,
  op3: core.Operator<B, C>,
  op4: core.Operator<C, D>,
  op5: core.Operator<D, E>,
  op6: core.Operator<E, F>,
  op7: core.Operator<F, G>,
  op8: core.Operator<G, H>,
  op9: core.Operator<H, I>,
  op10: core.Operator<I, J>,
  op11: core.Operator<J, K>,
  op12: core.Operator<K, L>,
  op13: core.Operator<L, M>,
  op14: core.Operator<M, N>,
): Stream<N>;
export function pipe<T, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O>(
  source: Source<T>,
  op1: core.Operator<T, A>,
  op2: core.Operator<A, B>,
  op3: core.Operator<B, C>,
  op4: core.Operator<C, D>,
  op5: core.Operator<D, E>,
  op6: core.Operator<E, F>,
  op7: core.Operator<F, G>,
  op8: core.Operator<G, H>,
  op9: core.Operator<H, I>,
  op10: core.Operator<I, J>,
  op11: core.Operator<J, K>,
  op12: core.Operator<K, L>,
  op13: core.Operator<L, M>,
  op14: core.Operator<M, N>,
  op15: core.Operator<N, O>,
): Stream<O>;
export function pipe<T, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P>(
  source: Source<T>,
  op1: core.Operator<T, A>,
  op2: core.Operator<A, B>,
  op3: core.Operator<B, C>,
  op4: core.Operator<C, D>,
  op5: core.Operator<D, E>,
  op6: core.Operator<E, F>,
  op7: core.Operator<F, G>,
  op8: core.Operator<G, H>,
  op9: core.Operator<H, I>,
  op10: core.Operator<I, J>,
  op11: core.Operator<J, K>,
  op12: core.Operator<K, L>,
  op13: core.Operator<L, M>,
  op14: core.Operator<M, N>,
  op15: core.Operator<N, O>,
  op16: core.Operator<O, P>,
): Stream<P>;
export function pipe(
  source: Source<any>,
  ...operators: Array<(source: any) => any>
): Stream<any> {
  return wrap(
    operators.reduce((stream, operator) => operator(stream), core.from(source)),
  );
}

/** Shares each source value by reference with every active branch. */
export function broadcast<T, const K extends readonly string[]>(
  source: Source<T>,
  options: core.KeyedBroadcastOptions<K[number]> & { readonly branches: K },
): FluentBranchGroup<{ readonly [P in K[number]]: T }>;
export function broadcast<T>(
  source: Source<T>,
  options: core.BroadcastOptions,
): FluentBranchGroup<readonly T[]>;
export function broadcast<T>(source: Source<T>, options: any): any {
  return wrapBranchGroup(core.broadcast(source, options));
}

/** Routes each source item to at most one branch; predicates can narrow branch types. */
export function route<
  T,
  const R extends Record<string, core.RoutePredicate<T>>,
  const D extends (keyof R & string) | undefined = undefined,
>(
  source: Source<T>,
  options: core.PredicateRouteOptions<T, R, D>,
): FluentBranchGroup<core.RoutedValues<T, R, D>>;
export function route<T, const K extends readonly string[]>(
  source: Source<T>,
  options: core.KeyedRouteOptions<T, K[number]> & { readonly branches: K },
): FluentBranchGroup<{ readonly [P in K[number]]: T }>;
export function route<T>(
  source: Source<T>,
  options: core.RouteOptions<T>,
): FluentBranchGroup<readonly T[]>;
export function route<T>(source: Source<T>, options: any): any {
  return wrapBranchGroup(core.route(source, options));
}

/** Reads a Web Stream lazily and cancels it on early termination. @category Sources */
export function fromReadableStream<T>(source: ReadableStream<T>): Stream<T> {
  return wrap(core.fromReadableStream(source));
}
