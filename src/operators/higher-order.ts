import {
  create,
  open,
  type Operator,
  type OperatorContext,
  type Source,
} from "../core/stream.js";
import { ChildSlot } from "../internal/child-slots.js";
import { notifications } from "../internal/notifications.js";
import { done, item } from "../internal/deferred.js";
import { integer } from "../internal/errors.js";

/**
 * Bounds for concurrently owned child iterables, including their cleanup.
 */
export interface HigherOrderOptions {
  /** Positive safe-integer child limit, from reservation through cleanup. Defaults to 1. */
  readonly concurrency?: number;
}

/** With one child, ordering needs no slots or background input pump. */
function combineSequentially<T>(): Operator<Source<T>, T> {
  return (source) =>
    create((scope) => {
      const outer = open(source, scope);
      let child: ReturnType<typeof open<T>> | undefined;
      return {
        async next() {
          for (;;) {
            if (!child) {
              const result = await new Promise<IteratorResult<Source<T>>>(
                outer.read,
              );
              if (result.done) return done();
              child = open(result.value, scope);
            }
            const result = await new Promise<IteratorResult<T>>(child.read);
            if (!result.done) return result;
            // Cleanup still occupies the single child slot.
            await child.scope.close();
            child = undefined;
          }
        },
      };
    });
}

function combineChildren<T>(
  ordered: boolean,
  options: HigherOrderOptions,
): Operator<Source<T>, T> {
  const concurrency = options.concurrency ?? 1;
  integer(concurrency, "concurrency");
  if (concurrency === 1) return combineSequentially();
  return (source) =>
    create((scope) => {
      const outer = open(source, scope);
      const changed = notifications(scope);
      const slots: ChildSlot<T>[] = [];
      let pulling = false;
      let filling = false;
      let outerDone = false;
      let demand = false;
      let cursor = 0;
      let failure: { error: unknown } | undefined;
      let refill: ChildSlot<T> | undefined;

      function fail(error: unknown): void {
        pulling = false;
        failure = { error };
        scope.reportError(error);
        changed.notify();
      }

      function fill(): void {
        if (filling) return;
        filling = true;
        try {
          // Owned reads track completion through child setup. A synchronous
          // source refills in this loop instead of recursively calling fill.
          while (
            demand &&
            !pulling &&
            !outerDone &&
            !failure &&
            !scope.signal.aborted &&
            slots.length < concurrency
          ) {
            pulling = true;
            outer.read((result) => {
              pulling = false;
              try {
                scope.check();
                if (result.done) {
                  outerDone = true;
                  changed.notify();
                } else {
                  const child = new ChildSlot(
                    result.value,
                    scope,
                    changed.notify,
                  );
                  slots.push(child);
                  child.start();
                }
                fill();
              } catch (error) {
                fail(error);
              }
            }, fail);
          }
        } finally {
          filling = false;
        }
      }

      return {
        async next() {
          demand = true;
          try {
            for (;;) {
              scope.check();
              const version = changed.version;
              if (failure) throw failure.error;
              refill?.start();
              refill = undefined;
              fill();
              let retired = false;
              for (let offset = 0; offset < slots.length; offset++) {
                const index = ordered
                  ? offset
                  : (cursor + offset) % slots.length;
                const slot = slots[index]!;
                if (slot.failure) throw slot.failure.error;
                if (ordered && index > 0) break;
                if (!slot.result) continue;
                const result = slot.consume();
                if (result.done) {
                  // A completed child retains its slot until cleanup has finished.
                  await slot.reader.scope.close();
                  slots.splice(index, 1);
                  cursor = slots.length ? index % slots.length : 0;
                  retired = true;
                  break;
                }
                cursor = (index + 1) % slots.length;
                refill = slot;
                return result;
              }
              if (retired) continue;
              if (outerDone && slots.length === 0) return done();
              // A synchronous refill may have notified while we inspected slots.
              if (changed.version !== version) continue;
              await changed.promise;
            }
          } finally {
            demand = false;
          }
        },
      };
    });
}

/**
 * Flatten child iterables, interleaving their values when concurrency exceeds one.
 *
 * Each child has one read/result slot and occupies concurrency from reservation through cleanup.
 * @throws RangeError if concurrency is not a positive safe integer.
 * @category Higher-order
 */
export function flatten<T>(
  options: HigherOrderOptions = {},
): Operator<Source<T>, T> {
  return combineChildren(false, options);
}
/**
 * Flatten child iterables in child order, optionally reading children concurrently.
 *
 * Completed successors retain their slots until earlier children retire, bounding work behind a blocked head.
 * @throws RangeError if concurrency is not a positive safe integer.
 * @category Higher-order
 */
export function concat<T>(
  options: HigherOrderOptions = {},
): Operator<Source<T>, T> {
  return combineChildren(true, options);
}

function project<T, U>(
  mapper: (value: T, context: OperatorContext) => Source<U>,
): Operator<T, Source<U>> {
  return (source) =>
    create((scope) => {
      const upstream = open(source, scope);
      let index = 0;
      return {
        read(receive, reject) {
          upstream.read((result) => {
            try {
              receive(
                result.done
                  ? done()
                  : item(
                      mapper(result.value, {
                        index: index++,
                        signal: scope.signal,
                      }),
                    ),
              );
            } catch (error) {
              reject(error);
            }
          }, reject);
        },
        async next() {
          const result = await upstream.next();
          return result.done
            ? done()
            : item(
                mapper(result.value, { index: index++, signal: scope.signal }),
              );
        },
      };
    });
}

/**
 * Map each value synchronously to a child iterable and flatten the children.
 *
 * Use defer inside the child for asynchronous preparation. The child concurrency defaults to one.
 * @throws RangeError if concurrency is not a positive safe integer.
 * @category Higher-order
 */
export function flatMap<T, U>(
  mapper: (value: T, context: OperatorContext) => Source<U>,
  options: HigherOrderOptions = {},
): Operator<T, U> {
  const combine = flatten<U>(options);
  return (source) => combine(project(mapper)(source));
}
/**
 * Map each value synchronously to a child iterable and emit children in input order.
 *
 * Use defer inside the child for asynchronous preparation. Concurrent children retain bounded read/result slots.
 * @throws RangeError if concurrency is not a positive safe integer.
 * @category Higher-order
 */
export function concatMap<T, U>(
  mapper: (value: T, context: OperatorContext) => Source<U>,
  options: HigherOrderOptions = {},
): Operator<T, U> {
  const combine = concat<U>(options);
  return (source) => combine(project(mapper)(source));
}
