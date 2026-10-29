import {
  create,
  open,
  type Operator,
  type Stream,
  type Read,
} from "../core/stream.js";
import { done, item } from "../internal/deferred.js";
import { integer, cancellationReason } from "../internal/errors.js";
import { RingBuffer } from "../internal/ring-buffer.js";

/**
 * Emits values from `start` (inclusive) to `end` (exclusive); omitted `end` reads to completion.
 *
 * Negative bounds count from the end. A negative start waits for completion and retains at most `-start` values. With a non-negative start and negative end, output trails input by `-end` values. Non-negative bounds close upstream at `end`; ranges known to be empty never open upstream.
 * @throws {RangeError} If a supplied bound is not a safe integer.
 * @category Selection
 */
export function slice(
  start?: number,
  end?: number,
): <T>(source: AsyncIterable<T>) => Stream<T>;
export function slice<T>(start?: number, end?: number): Operator<T, T>;
export function slice(
  start = 0,
  end?: number,
): <T>(source: AsyncIterable<T>) => Stream<T> {
  integer(start, "start", Number.MIN_SAFE_INTEGER);
  if (end !== undefined) integer(end, "end", Number.MIN_SAFE_INTEGER);
  const empty =
    end === 0 ||
    (end !== undefined &&
      ((start >= 0 && end >= 0) || (start < 0 && end < 0)) &&
      end <= start);
  return <T>(source: AsyncIterable<T>) =>
    create<T>((scope) => {
      let upstream: ReturnType<typeof open<T>> | undefined;
      let position = 0;
      let ended = empty;
      const capacity =
        start < 0 ? -start : end !== undefined && end < 0 ? -end : 0;
      const buffer = capacity ? new RingBuffer<T>(capacity) : undefined;
      if (buffer) scope.finalizers.push(() => buffer.clear());
      let outputIndex = 0;
      let outputEnd = 0;
      let buffered = false;

      const read: Read<T> = (receive, reject) => {
        if (ended) {
          receive(done());
          return;
        }
        if (buffered) {
          receive(
            outputIndex < outputEnd ? item(buffer!.get(outputIndex++)) : done(),
          );
          return;
        }
        upstream ??= open(source, scope);
        const accept = async (result: IteratorResult<T>): Promise<void> => {
          try {
            if (result.done) {
              if (start < 0) {
                buffered = true;
                const offset = position - buffer!.size;
                outputEnd =
                  end === undefined
                    ? buffer!.size
                    : Math.max(
                        0,
                        Math.min(position, end < 0 ? position + end : end) -
                          offset,
                      );
                receive(
                  outputIndex < outputEnd
                    ? item(buffer!.get(outputIndex++))
                    : done(),
                );
              } else receive(done());
              return;
            }
            const index = position++;
            if (start < 0) buffer!.push(result.value);
            else if (index >= start) {
              if (!buffer) {
                if (position === end) {
                  ended = true;
                  upstream!.scope.stop(cancellationReason());
                  void upstream!.scope.close();
                }
                receive(result);
                return;
              }
              if (buffer.size === capacity) {
                const value = buffer.get(0);
                buffer.push(result.value);
                receive(item(value));
                return;
              }
              buffer.push(result.value);
            }
            await undefined;
            scope.check();
            upstream!.read(accept, reject);
          } catch (error) {
            reject(error);
          }
        };
        upstream.read(accept, reject);
      };
      return { read, next: () => new Promise(read) };
    });
}
