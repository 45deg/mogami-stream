import { defer, from, tap } from "mogami-stream";

// #region wait
/**
 * @param {AbortSignal} signal
 * @returns {Promise<void>}
 */
export function yieldToHost(signal) {
  return new Promise((resolve, reject) => {
    signal.throwIfAborted();
    const abort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", abort);
      resolve();
    }, 0);
    signal.addEventListener("abort", abort, { once: true });
  });
}
// #endregion wait

// #region count
/**
 * @param {number} count
 * @returns {<T>(source: AsyncIterable<T>) => import("mogami-stream").Stream<T>}
 */
export function yieldEvery(count = 1_024) {
  if (!Number.isSafeInteger(count) || count <= 0)
    throw new RangeError("count must be a positive safe integer");
  return (source) =>
    from(source).pipe(
      tap((_value, { index, signal }) =>
        (index + 1) % count === 0 ? yieldToHost(signal) : undefined,
      ),
    );
}
// #endregion count

// #region time
/**
 * @param {number} budgetMs
 * @returns {<T>(source: AsyncIterable<T>) => import("mogami-stream").Stream<T>}
 */
export function yieldAfter(budgetMs = 4) {
  if (!Number.isFinite(budgetMs) || budgetMs <= 0)
    throw new RangeError("budgetMs must be finite and positive");
  return (source) =>
    defer(() => {
      // Each execution owns its clock, including simultaneous iterations.
      let lastYield = performance.now();
      return from(source).pipe(
        tap((_value, { signal }) => {
          if (performance.now() - lastYield < budgetMs) return;
          return yieldToHost(signal).then(() => {
            lastYield = performance.now();
          });
        }),
      );
    });
}
// #endregion time
