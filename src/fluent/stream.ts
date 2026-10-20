import {
  map,
  mapSettled,
  type MapOptions,
  type MapContext,
  type SettledResult,
} from "../operators/mapping.js";
import {
  filter,
  filterMap,
  dedupe,
  chunks,
  scan,
} from "../operators/transform.js";
import { take, skip, takeWhile, skipWhile } from "../operators/selection.js";
import {
  flatten,
  concat,
  flatMap,
  concatMap,
  type HigherOrderOptions,
} from "../operators/higher-order.js";
import {
  delayStart,
  spaceout,
  rateLimit,
  timeoutEach,
  deadline,
  bufferTime,
  type RateLimitOptions,
  type BufferTimeOptions,
} from "../operators/timing.js";
import { tap, finalize } from "../operators/lifecycle.js";
import { catchError } from "../operators/recovery.js";
import type { ResourceContext } from "../core/resource.js";
import { Stream as CoreStream } from "../core/stream.js";
import type { Source, Operator, OperatorContext } from "../core/stream.js";

/** A lazy stream with fluent operators, composition, and terminal methods. */
export class Stream<T> extends CoreStream<T> {
  /** Reuses a core stream's builder without opening or wrapping its reader. @internal */
  static fromCore<T>(source: CoreStream<T>): Stream<T> {
    return source instanceof Stream
      ? source
      : new Stream(Stream.builderFor(source));
  }
  /** Maps values with the same concurrency, ordering, and retry options as {@link map}. */
  map<U>(
    mapper: (value: T, context: MapContext) => U,
    options?: MapOptions,
  ): Stream<Awaited<U>> {
    return wrap(map(mapper, options)(this));
  }
  /** Maps values to fulfilled or rejected outcomes. */
  mapSettled<U>(
    mapper: (value: T, context: MapContext) => U,
    options?: MapOptions,
  ): Stream<SettledResult<Awaited<U>>> {
    return wrap(mapSettled(mapper, options)(this));
  }
  /** Keeps matching values; synchronous type guards narrow the output type. */
  filter<S extends T>(
    predicate: (value: T, context: OperatorContext) => value is S,
  ): Stream<S>;
  filter(
    predicate: (
      value: T,
      context: OperatorContext,
    ) => boolean | PromiseLike<boolean>,
  ): Stream<T>;
  filter(
    predicate: (
      value: T,
      context: OperatorContext,
    ) => boolean | PromiseLike<boolean>,
  ): Stream<T> {
    return wrap(filter(predicate)(this));
  }
  /** Maps values sequentially, omitting null and undefined results. */
  filterMap<U>(
    mapper: (value: T, context: OperatorContext) => U,
  ): Stream<NonNullable<Awaited<U>>> {
    return wrap(filterMap(mapper)(this));
  }
  /** Drops consecutive equal values or keys. */
  dedupe<K = T>(
    keySelector?: (value: T, context: OperatorContext) => K,
  ): Stream<T> {
    return wrap(dedupe(keySelector)(this));
  }
  /** Groups values into arrays of at most size items. */
  chunks(size: number): Stream<T[]> {
    return wrap(chunks<T>(size)(this));
  }
  /** Emits each updated accumulator after reducing a value. */
  scan<A>(
    reducer: (
      accumulator: Awaited<A>,
      value: T,
      context: OperatorContext,
    ) => Awaited<A> | PromiseLike<Awaited<A>>,
    initial: A,
  ): Stream<Awaited<A>> {
    return wrap(scan(reducer, initial)(this));
  }
  /** Emits at most count values, then closes upstream. */
  take(count: number): Stream<T> {
    return wrap(take<T>(count)(this));
  }
  /** Skips the first count values. */
  skip(count: number): Stream<T> {
    return wrap(skip<T>(count)(this));
  }
  /** Emits a matching prefix; synchronous type guards narrow the output type. */
  takeWhile<S extends T>(
    predicate: (value: T, context: OperatorContext) => value is S,
  ): Stream<S>;
  takeWhile(
    predicate: (
      value: T,
      context: OperatorContext,
    ) => boolean | PromiseLike<boolean>,
  ): Stream<T>;
  takeWhile(
    predicate: (
      value: T,
      context: OperatorContext,
    ) => boolean | PromiseLike<boolean>,
  ): Stream<T> {
    return wrap(takeWhile(predicate)(this));
  }
  /** Skips a matching prefix. */
  skipWhile(
    predicate: (
      value: T,
      context: OperatorContext,
    ) => boolean | PromiseLike<boolean>,
  ): Stream<T> {
    return wrap(skipWhile(predicate)(this));
  }
  /** Flattens child sources concurrently. Only available on streams of sources. */
  flatten<U>(this: Stream<Source<U>>, options?: HigherOrderOptions): Stream<U> {
    return wrap(flatten<U>(options)(this));
  }
  /** Flattens child sources in child order. Only available on streams of sources. */
  concat<U>(this: Stream<Source<U>>, options?: HigherOrderOptions): Stream<U> {
    return wrap(concat<U>(options)(this));
  }
  /** Maps values to child sources and flattens them concurrently. */
  flatMap<U>(
    mapper: (value: T, context: OperatorContext) => Source<U>,
    options?: HigherOrderOptions,
  ): Stream<U> {
    return wrap(flatMap(mapper, options)(this));
  }
  /** Maps values to child sources and emits in child order. */
  concatMap<U>(
    mapper: (value: T, context: OperatorContext) => Source<U>,
    options?: HigherOrderOptions,
  ): Stream<U> {
    return wrap(concatMap(mapper, options)(this));
  }
  /** Runs an effect for each value without changing the value. */
  tap(effect: (value: T, context: OperatorContext) => unknown): Stream<T> {
    return wrap(tap(effect)(this));
  }
  /** Runs an effect during cleanup and waits for it to finish. */
  finalize(effect: () => unknown): Stream<T> {
    return wrap(finalize<T>(effect)(this));
  }
  /** Recovers once from an upstream failure using a fallback source. */
  catchError<U>(
    handler: (
      error: unknown,
      context: ResourceContext,
    ) => Source<U> | PromiseLike<Source<U>>,
  ): Stream<T | U> {
    return wrap(catchError(handler)(this));
  }
  /** Delays opening upstream. */
  delayStart(ms: number): Stream<T> {
    return wrap(delayStart<T>(ms)(this));
  }
  /** Spaces delivery of values. */
  spaceout(intervalMs: number): Stream<T> {
    return wrap(spaceout<T>(intervalMs)(this));
  }
  /** Limits the rate of upstream reads. */
  rateLimit(options: RateLimitOptions): Stream<T> {
    return wrap(rateLimit<T>(options)(this));
  }
  /** Limits how long each upstream read may take. */
  timeoutEach(ms: number): Stream<T> {
    return wrap(timeoutEach<T>(ms)(this));
  }
  /** Limits the duration of this stage. */
  deadline(durationMs: number): Stream<T> {
    return wrap(deadline<T>(durationMs)(this));
  }
  /** Collects batches bounded by time and maxItems. */
  bufferTime(windowMs: number, options: BufferTimeOptions): Stream<T[]> {
    return wrap(bufferTime<T>(windowMs, options)(this));
  }
  /** Composes up to eight operators from left to right without starting execution. */
  pipe(): Stream<T>;
  pipe<A>(op1: Operator<T, A>): Stream<A>;
  pipe<A, B>(op1: Operator<T, A>, op2: Operator<A, B>): Stream<B>;
  pipe<A, B, C>(
    op1: Operator<T, A>,
    op2: Operator<A, B>,
    op3: Operator<B, C>,
  ): Stream<C>;
  pipe<A, B, C, D>(
    op1: Operator<T, A>,
    op2: Operator<A, B>,
    op3: Operator<B, C>,
    op4: Operator<C, D>,
  ): Stream<D>;
  pipe<A, B, C, D, E>(
    op1: Operator<T, A>,
    op2: Operator<A, B>,
    op3: Operator<B, C>,
    op4: Operator<C, D>,
    op5: Operator<D, E>,
  ): Stream<E>;
  pipe<A, B, C, D, E, F>(
    op1: Operator<T, A>,
    op2: Operator<A, B>,
    op3: Operator<B, C>,
    op4: Operator<C, D>,
    op5: Operator<D, E>,
    op6: Operator<E, F>,
  ): Stream<F>;
  pipe<A, B, C, D, E, F, G>(
    op1: Operator<T, A>,
    op2: Operator<A, B>,
    op3: Operator<B, C>,
    op4: Operator<C, D>,
    op5: Operator<D, E>,
    op6: Operator<E, F>,
    op7: Operator<F, G>,
  ): Stream<G>;
  pipe<A, B, C, D, E, F, G, H>(
    op1: Operator<T, A>,
    op2: Operator<A, B>,
    op3: Operator<B, C>,
    op4: Operator<C, D>,
    op5: Operator<D, E>,
    op6: Operator<E, F>,
    op7: Operator<F, G>,
    op8: Operator<G, H>,
  ): Stream<H>;
  pipe(...operators: Array<(source: any) => any>): Stream<any> {
    return wrap(operators.reduce((source, operator) => operator(source), this));
  }
}

export function wrap<T>(source: CoreStream<T>): Stream<T> {
  return Stream.fromCore(source);
}
