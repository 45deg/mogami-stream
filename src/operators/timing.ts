import { create, open, type Operator, type Stream } from "../core/stream.js";
import { alarm } from "../internal/alarm.js";
import { done, item } from "../internal/deferred.js";
import {
  DeadlineExceededError,
  TimeoutError,
  duration,
  integer,
} from "../internal/errors.js";

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
  duration(ms, "ms");
  return <T>(source: AsyncIterable<T>) =>
    create((scope) => {
      let upstream: ReturnType<typeof open<T>> | undefined;
      return {
        async next() {
          if (!upstream) {
            await scope.scheduler.sleep(ms, scope.signal);
            scope.check();
            upstream = open(source, scope);
          }
          return upstream.next();
        },
      };
    });
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
  duration(intervalMs, "intervalMs");
  return <T>(source: AsyncIterable<T>) =>
    create((scope) => {
      const upstream = open(source, scope);
      let previous: number | undefined;
      return {
        async next() {
          const result = await upstream.next();
          if (result.done) return result;
          if (previous !== undefined) {
            const remaining = previous + intervalMs - scope.scheduler.now();
            if (remaining > 0)
              await scope.scheduler.sleep(remaining, scope.signal);
          }
          scope.check();
          previous = scope.scheduler.now();
          return result;
        },
      };
    });
}

/**
 * Sliding-window parameters for {@link rateLimit}.
 */
export interface RateLimitOptions {
  /** Maximum number of upstream reads that may start within any `perMs` window. Must be a positive safe integer. */
  readonly count: number;
  /** Sliding window duration in milliseconds. Must be finite and positive. */
  readonly perMs: number;
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
export function rateLimit({
  count,
  perMs,
}: RateLimitOptions): <T>(source: AsyncIterable<T>) => Stream<T> {
  integer(count, "count");
  duration(perMs, "perMs", true);
  return <T>(source: AsyncIterable<T>) =>
    create((scope) => {
      const upstream = open(source, scope);
      let starts: number[] = [];
      let head = 0;
      return {
        async next() {
          for (;;) {
            const now = scope.scheduler.now();
            if (starts.length && now - starts[starts.length - 1]! >= perMs) {
              starts.length = 0;
              head = 0;
            } else {
              while (head < starts.length && now - starts[head]! >= perMs)
                head++;
              // Keep fewer expired entries than live entries between compactions.
              if (head > 0 && head >= starts.length - head) {
                starts = starts.slice(head);
                head = 0;
              }
            }
            if (starts.length - head < count) break;
            await scope.scheduler.sleep(
              perMs - (now - starts[head]!),
              scope.signal,
            );
          }
          scope.check();
          starts.push(scope.scheduler.now());
          return upstream.next();
        },
      };
    });
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
  duration(ms, "ms");
  return <T>(source: AsyncIterable<T>) =>
    create((scope) => {
      if (ms === 0) throw new TimeoutError(ms);
      const upstream = open(source, scope);
      return {
        async next() {
          const startedAtMs = scope.scheduler.now();
          const timer = alarm(scope, ms);
          try {
            const result = await Promise.race([
              upstream.next(),
              timer.promise.then((expired): IteratorResult<T> => {
                if (expired) throw new TimeoutError(ms);
                scope.check();
                return done();
              }),
            ]);
            if (scope.scheduler.now() - startedAtMs >= ms)
              throw new TimeoutError(ms);
            return result;
          } finally {
            timer.cancel();
          }
        },
      };
    });
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
  duration(durationMs, "durationMs");
  return <T>(source: AsyncIterable<T>) =>
    create((scope) => {
      if (durationMs === 0) throw new DeadlineExceededError(durationMs);
      const expiresAtMs = scope.scheduler.now() + durationMs;
      const checkDeadline = () => {
        if (scope.scheduler.now() >= expiresAtMs) {
          throw new DeadlineExceededError(durationMs);
        }
      };
      const timer = alarm(scope, durationMs);
      scope.setCleanup(() => timer.cancel());
      void timer.promise.then(
        (expired) => {
          if (expired) scope.host.fail(new DeadlineExceededError(durationMs));
        },
        () => {},
      );
      const upstream = open(source, scope);
      return {
        async next() {
          checkDeadline();
          const result = await upstream.next();
          checkDeadline();
          if (result.done) timer.cancel();
          return result;
        },
      };
    });
}

/**
 * Options for {@link bufferTime}.
 */
export interface BufferTimeOptions {
  /** Maximum number of items per emitted batch. Must be a positive safe integer. */
  readonly maxItems: number;
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
  { maxItems }: BufferTimeOptions,
): <T>(source: AsyncIterable<T>) => Stream<T[]> {
  duration(windowMs, "windowMs", true);
  integer(maxItems, "maxItems");
  return <T>(source: AsyncIterable<T>) =>
    create((scope) => {
      const upstream = open(source, scope);
      type Arrival = { result: IteratorResult<T>; at: number };
      let pending: Promise<Arrival> | undefined;
      let ended = false;
      const read = () => {
        pending ??= upstream
          .next()
          .then((result) => ({ result, at: scope.scheduler.now() }));
        return pending;
      };
      return {
        async next() {
          if (ended) return done();
          const first = await read();
          pending = undefined;
          if (first.result.done) {
            ended = true;
            return done();
          }
          const values = [first.result.value];
          const end = first.at + windowMs;
          if (values.length === maxItems || scope.scheduler.now() >= end)
            return item(values);
          const timer = alarm(scope, end - scope.scheduler.now());
          const expiration = timer.promise.then(() => undefined);
          try {
            while (values.length < maxItems) {
              const arrival = await Promise.race([read(), expiration]);
              scope.check();
              if (!arrival) return item(values);
              // A delayed timer must not pull the next window's value into
              // this batch. Keep the settled reservation for the next demand.
              if (arrival.at >= end) return item(values);
              pending = undefined;
              if (arrival.result.done) {
                ended = true;
                return item(values);
              }
              values.push(arrival.result.value);
              if (scope.scheduler.now() >= end) return item(values);
            }
            return item(values);
          } finally {
            timer.cancel();
          }
        },
      };
    });
}
