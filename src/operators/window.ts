import {
  create,
  open,
  type Operator,
  type Stream,
  type Read,
} from "../core/stream.js";
import { done, item } from "../internal/deferred.js";
import { integer } from "../internal/errors.js";
import { RingBuffer } from "../internal/ring-buffer.js";

/**
 * Emits a fresh array of the latest `size` values after each input, once a full window is available.
 *
 * Windows overlap by `size - 1` items. Incomplete windows are omitted. Retains at most `size` values; emitted arrays can be mutated without affecting later windows.
 * @throws {RangeError} If `size` is not a positive safe integer.
 * @category Transformation
 */
export function slidingWindow(
  size: number,
): <T>(source: AsyncIterable<T>) => Stream<T[]>;
export function slidingWindow<T>(size: number): Operator<T, T[]>;
export function slidingWindow(
  size: number,
): <T>(source: AsyncIterable<T>) => Stream<T[]> {
  integer(size, "size");
  return <T>(source: AsyncIterable<T>) =>
    create<T[]>((scope) => {
      const upstream = open(source, scope);
      const buffer = new RingBuffer<T>(size);
      scope.finalizers.push(() => buffer.clear());
      const read: Read<T[]> = (receive, reject) => {
        const accept = async (result: IteratorResult<T>): Promise<void> => {
          try {
            if (result.done) {
              receive(done());
              return;
            }
            buffer.push(result.value);
            if (buffer.size === size) receive(item(buffer.snapshot()));
            else {
              // Filling a large window must yield to cancellation without recursive reads.
              await undefined;
              scope.check();
              upstream.read(accept, reject);
            }
          } catch (error) {
            reject(error);
          }
        };
        upstream.read(accept, reject);
      };
      return { read, next: () => new Promise(read) };
    });
}
