import { create, type Stream } from "../core/stream.js";
import { done } from "../internal/deferred.js";

/**
 * Reads a Web Stream on demand, acquiring its reader on the first read.
 * Early termination cancels the source; completion and failure release the lock.
 * The source remains single-use. No additional input queue is created.
 * @category Sources
 */
export function fromReadableStream<T>(source: ReadableStream<T>): Stream<T> {
  return create((scope) => {
    const reader = source.getReader();
    scope.setCleanup(() => reader.cancel(scope.signal.reason));
    scope.finalizers.push(() => reader.releaseLock());
    return {
      read(receive, reject) {
        void reader
          .read()
          .then((result) => receive(result.done ? done() : result), reject);
      },
      async next() {
        const result = await reader.read();
        return result.done ? done() : result;
      },
    };
  });
}
