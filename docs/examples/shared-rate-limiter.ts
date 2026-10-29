// #region limiter
export class LimiterQueueFullError extends Error {
  constructor() {
    super("Shared limiter waiting queue is full");
    this.name = "LimiterQueueFullError";
  }
}

export function createSharedLimiter(intervalMs: number, maxWaiting: number) {
  if (!Number.isFinite(intervalMs) || intervalMs <= 0)
    throw new RangeError("intervalMs must be finite and positive");
  if (!Number.isSafeInteger(maxWaiting) || maxWaiting < 0)
    throw new RangeError("maxWaiting must be a non-negative safe integer");

  type Waiter = {
    grant(): void;
    previous: Waiter | undefined;
    next: Waiter | undefined;
  };
  let first: Waiter | undefined;
  let last: Waiter | undefined;
  let waiting = 0;
  let nextStart = -Infinity;
  let starting = false; // A granted caller has not resumed yet.
  let timer: ReturnType<typeof setTimeout> | undefined;

  function remove(waiter: Waiter) {
    if (waiter.previous) waiter.previous.next = waiter.next;
    else first = waiter.next;
    if (waiter.next) waiter.next.previous = waiter.previous;
    else last = waiter.previous;
    waiter.previous = waiter.next = undefined;
    waiting--;
  }

  function drain() {
    if (timer !== undefined) clearTimeout(timer);
    timer = undefined;
    if (starting || waiting === 0) return;
    const remaining = nextStart - performance.now();
    if (remaining > 0) {
      // Long waits must not overflow the host's signed 32-bit timer delay.
      timer = setTimeout(drain, Math.min(remaining, 2_147_483_647));
      return;
    }
    starting = true;
    nextStart = performance.now() + intervalMs;
    const waiter = first!;
    remove(waiter);
    waiter.grant();
  }

  function acquire(signal: AbortSignal): Promise<void> {
    return new Promise((resolve, reject) => {
      signal.throwIfAborted();
      const canStart =
        !starting && waiting === 0 && performance.now() >= nextStart;
      if (!canStart && waiting >= maxWaiting) throw new LimiterQueueFullError();
      const abort = () => {
        remove(waiter);
        signal.removeEventListener("abort", abort);
        reject(signal.reason);
        drain();
      };
      const waiter: Waiter = {
        previous: last,
        next: undefined,
        grant() {
          signal.removeEventListener("abort", abort);
          resolve();
        },
      };
      signal.addEventListener("abort", abort, { once: true });
      if (last) last.next = waiter;
      else first = waiter;
      last = waiter;
      waiting++;
      drain();
    });
  }

  return {
    async run<T>(signal: AbortSignal, operation: () => Promise<T>): Promise<T> {
      await acquire(signal);
      try {
        signal.throwIfAborted(); // Cancellation may occur after the grant.
        nextStart = performance.now() + intervalMs;
        return operation(); // Start I/O immediately; do not await other work first.
      } finally {
        // Release admission, not the operation: I/O may overlap.
        starting = false;
        drain();
      }
    },
  };
}
// #endregion limiter

// #region usage
import { from, map } from "mogami-stream";

export async function collectWithSharedRetries<T, U>(
  first: readonly T[],
  second: readonly T[],
  processJob: (job: T, signal: AbortSignal) => Promise<U>,
  isTransient: (error: unknown) => boolean,
) {
  // Both executions and every retry below use this same instance.
  const limiter = createSharedLimiter(100, 8);
  const execute = (jobs: readonly T[]) =>
    from(jobs)
      .pipe(
        map(
          (job, { signal }) =>
            limiter.run(signal, () => processJob(job, signal)),
          {
            concurrency: 2,
            capacity: 2,
            retry: {
              retries: 2,
              shouldRetry: (error) =>
                !(error instanceof LimiterQueueFullError) && isTransient(error),
            },
          },
        ),
      )
      .toArray();
  return Promise.all([execute(first), execute(second)]);
}
// #endregion usage
