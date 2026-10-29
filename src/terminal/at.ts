import { create, open, type Source, type Read } from "../core/stream.js";
import { done, item } from "../internal/deferred.js";
import { integer, cancellationReason } from "../internal/errors.js";
import { RingBuffer } from "../internal/ring-buffer.js";
import { iterate, type ExecutionOptions } from "./iterate.js";

/**
 * Returns the value at a zero-based index, or undefined when out of range, after cleanup.
 *
 * Non-negative indices close upstream as soon as the value is read. Negative indices count from the end and retain at most `-index` values until completion. The selected value is awaited, like `find`; other Promise-valued items are not unwrapped.
 * @throws {RangeError} If `index` is not a safe integer.
 * @category Consumption
 */
export async function at<T>(
  source: Source<T>,
  index: number,
  options: ExecutionOptions = {},
): Promise<Awaited<T> | undefined> {
  integer(index, "index", Number.MIN_SAFE_INTEGER);
  const selection = create<Awaited<T>>((scope) => {
    const upstream = open(source, scope);
    const buffer = index < 0 ? new RingBuffer<T>(-index) : undefined;
    if (buffer) scope.finalizers.push(() => buffer.clear());
    let position = 0;
    let ended = false;
    const read: Read<Awaited<T>> = (receive, reject) => {
      if (ended) {
        receive(done());
        return;
      }
      const accept = async (result: IteratorResult<T>): Promise<void> => {
        try {
          if (result.done) {
            ended = true;
            if (!buffer || buffer.size < -index) {
              receive(done());
              return;
            }
            const value = await buffer.get(0);
            scope.check();
            receive(item(value));
            return;
          }
          if (buffer) buffer.push(result.value);
          else if (position++ === index) {
            ended = true;
            upstream.scope.stop(cancellationReason());
            void upstream.scope.close();
            const value = await result.value;
            scope.check();
            receive(item(value));
            return;
          }
          // Discard without a next() Promise per item, but still accept queued cancellation.
          await undefined;
          scope.check();
          upstream.read(accept, reject);
        } catch (error) {
          reject(error);
        }
      };
      upstream.read(accept, reject);
    };
    return { read, next: () => new Promise(read) };
  });
  const iterator = iterate(selection, options);
  try {
    const result = await iterator.next();
    // Read completion so cancellation remains observable while cleanup is pending.
    if (!result.done) await iterator.next();
    return result.done ? undefined : result.value;
  } finally {
    await iterator.closed;
  }
}
