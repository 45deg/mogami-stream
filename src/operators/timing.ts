import { create, open, type Operator } from "../core/stream.js";
import { alarm } from "../internal/alarm.js";
import { done, item } from "../internal/deferred.js";
import {
  DeadlineExceededError,
  TimeoutError,
  duration,
  integer,
} from "../internal/errors.js";

/**
 * Wait once before opening upstream, starting the wait on the first read.
 * @throws RangeError if ms is not finite and non-negative.
 * @category Timing
 */
export function delayStart<T>(ms: number): Operator<T, T> {
  duration(ms, "ms");
  return (source) =>
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
 * Emit the first value without an added delay, then space outputs at least intervalMs apart.
 *
 * Upstream read time and downstream idle time count toward the gap.
 * @throws RangeError if intervalMs is not finite and non-negative.
 * @category Timing
 */
export function spaceout<T>(intervalMs: number): Operator<T, T> {
  duration(intervalMs, "intervalMs");
  return (source) =>
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
 * Sliding-window limits for upstream read starts.
 */
export interface RateLimitOptions {
  /** Maximum read starts per sliding window; a positive safe integer. */
  readonly count: number;
  /** Sliding window in finite, positive milliseconds. */
  readonly perMs: number;
}
/**
 * Limit upstream read starts within a sliding window for this execution.
 *
 * Reads that fail or complete the input still count. Starts exactly perMs apart belong to different windows. Mapper retries do not count as additional upstream reads.
 * @throws RangeError if count is not a positive safe integer or perMs is not finite and positive.
 * @category Timing
 */
export function rateLimit<T>({
  count,
  perMs,
}: RateLimitOptions): Operator<T, T> {
  integer(count, "count");
  duration(perMs, "perMs", true);
  return (source) =>
    create((scope) => {
      const upstream = open(source, scope);
      const starts: number[] = [];
      return {
        async next() {
          for (;;) {
            const now = scope.scheduler.now();
            while (starts.length && starts[0]! <= now - perMs) starts.shift();
            if (starts.length < count) break;
            await scope.scheduler.sleep(starts[0]! + perMs - now, scope.signal);
          }
          scope.check();
          starts.push(scope.scheduler.now());
          return upstream.next();
        },
      };
    });
}

/**
 * Fail with TimeoutError if an individual upstream read exceeds ms.
 *
 * Downstream idle time does not count. Zero establishes an immediate deadline without guaranteeing a successful read. Cleanup still waits for non-cooperative work.
 * @throws RangeError if ms is not finite and non-negative.
 * @category Timing
 */
export function timeoutEach<T>(ms: number): Operator<T, T> {
  duration(ms, "ms");
  return (source) =>
    create((scope) => {
      const upstream = open(source, scope);
      return {
        async next() {
          const timer = alarm(scope, ms);
          try {
            return await Promise.race([
              upstream.next(),
              timer.promise.then((expired): IteratorResult<T> => {
                if (expired) throw new TimeoutError(ms);
                scope.check();
                return done();
              }),
            ]);
          } finally {
            timer.cancel();
          }
        },
      };
    });
}

/**
 * Fail with DeadlineExceededError when a started stage exhausts its elapsed time budget.
 *
 * Downstream idle time counts. Zero expires without opening upstream; cleanup still waits for non-cooperative work.
 * @throws RangeError if durationMs is not finite and non-negative.
 * @category Timing
 */
export function deadline<T>(durationMs: number): Operator<T, T> {
  duration(durationMs, "durationMs");
  return (source) =>
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
 * The item limit for a time buffer.
 */
export interface BufferTimeOptions {
  /** Maximum values per emitted array; a positive safe integer. */
  readonly maxItems: number;
}
/**
 * Emit a nonempty array on window expiry or when maxItems is reached.
 *
 * The window starts when its first value is observed. Flush a partial array only on normal completion. One reserved read may finish while downstream is idle; no background queue of windows accumulates.
 * @throws RangeError if windowMs is not finite and positive or maxItems is not a positive safe integer.
 * @category Timing
 */
export function bufferTime<T>(
  windowMs: number,
  { maxItems }: BufferTimeOptions,
): Operator<T, T[]> {
  duration(windowMs, "windowMs", true);
  integer(maxItems, "maxItems");
  return (source) =>
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
