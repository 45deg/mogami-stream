import {
  create,
  open,
  type Operator,
  type OperatorContext,
  type Read,
  type Source,
} from "../core/stream.js";
import { ChildSlot } from "../internal/child-slots.js";
import { notifications } from "../internal/notifications.js";
import { done, item } from "../internal/deferred.js";
import { integer } from "../internal/errors.js";

/**
 * Concurrency options for higher-order stream operators.
 */
export interface HigherOrderOptions {
  /** Maximum number of active child sources at once (counted from slot reservation through child cleanup). Must be a positive safe integer. Defaults to `1`. */
  readonly concurrency?: number;
}

/** With one child, ordering needs no slots or background input pump. */
function combineSequentially<T, U>(
  mapper: (value: T, context: OperatorContext) => Source<U>,
): Operator<T, U> {
  return (source) =>
    create((scope) => {
      const outer = open(source, scope);
      let child: ReturnType<typeof open<U>> | undefined;
      let index = 0;
      const readNext: Read<U> = (receive, reject) => {
        if (!child) {
          outer.read((result) => {
            if (result.done) {
              receive(done());
              return;
            }
            try {
              scope.check();
              child = open(
                mapper(result.value, {
                  index: index++,
                  signal: scope.signal,
                }),
                scope,
              );
            } catch (error) {
              reject(error);
              return;
            }
            readNext(receive, reject);
          }, reject);
          return;
        }
        const current = child;
        current.read((result) => {
          if (!result.done) {
            receive(result);
            return;
          }
          // Retain the slot through cleanup; this also breaks synchronous
          // recursion when a long sequence of children is empty.
          void current.scope.close().then(() => {
            child = undefined;
            readNext(receive, reject);
          }, reject);
        }, reject);
      };
      return {
        read: readNext,
        next: () => new Promise(readNext),
      };
    });
}

function combineChildren<T>(
  ordered: boolean,
  options: HigherOrderOptions,
): Operator<Source<T>, T> {
  const concurrency = options.concurrency ?? 1;
  integer(concurrency, "concurrency");
  if (concurrency === 1)
    return combineSequentially((source: Source<T>) => source);
  return (source) =>
    create((scope) => {
      const outer = open(source, scope);
      const changed = notifications(scope);
      const slots: ChildSlot<T>[] = [];
      const cleanupStates = new Map<ChildSlot<T>, "pending" | "finished">();
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
                const result = slot.result;
                if (result.done) {
                  // A completed child retains its slot until cleanup has finished.
                  if (ordered) await slot.reader.scope.close();
                  else {
                    const cleanup = cleanupStates.get(slot);
                    if (!cleanup) {
                      cleanupStates.set(slot, "pending");
                      void slot.reader.scope.close().then(() => {
                        cleanupStates.set(slot, "finished");
                        changed.notify();
                      }, fail);
                    }
                    // Unordered siblings can still deliver while this slot closes.
                    if (cleanup !== "finished") continue;
                    cleanupStates.delete(slot);
                  }
                  slots.splice(index, 1);
                  cursor = slots.length ? index % slots.length : 0;
                  retired = true;
                  break;
                }
                cursor = (index + 1) % slots.length;
                refill = slot;
                return slot.consume();
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
 * Flattens a stream of child sources, interleaving values when `concurrency > 1`.
 *
 * Each active child holds a single read/result slot and occupies a concurrency slot until its cleanup finishes.
 * A child's cleanup does not block values from other active children.
 * @throws {RangeError} If `concurrency` is not a positive safe integer.
 * @category Higher-order
 */
export function flatten<T>(
  options: HigherOrderOptions = {},
): Operator<Source<T>, T> {
  return combineChildren(false, options);
}
/**
 * Flattens a stream of child sources strictly in child order, while optionally opening up to `concurrency` children ahead.
 *
 * Later children that complete early retain their concurrency slots until earlier children finish, keeping buffered work bounded behind a slow leading child.
 * @throws {RangeError} If `concurrency` is not a positive safe integer.
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
      const read: Read<Source<U>> = (receive, reject) => {
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
      };
      return { read, next: () => new Promise(read) };
    });
}

function projectChildren<T, U>(
  mapper: (value: T, context: OperatorContext) => Source<U>,
  ordered: boolean,
  options: HigherOrderOptions,
): Operator<T, U> {
  const concurrency = options.concurrency ?? 1;
  integer(concurrency, "concurrency");
  if (concurrency === 1) return combineSequentially(mapper);
  const combine = combineChildren<U>(ordered, { concurrency });
  return (source) => combine(project(mapper)(source));
}

/**
 * Maps each value synchronously to a child source and flattens the children concurrently.
 *
 * Wrap asynchronous setup inside {@link defer} within the returned child source. Defaults to `concurrency: 1`.
 * @throws {RangeError} If `concurrency` is not a positive safe integer.
 * @category Higher-order
 */
export function flatMap<T, U>(
  mapper: (value: T, context: OperatorContext) => Source<U>,
  options: HigherOrderOptions = {},
): Operator<T, U> {
  return projectChildren(mapper, false, options);
}
/**
 * Maps each value synchronously to a child source and emits values in child order.
 *
 * Wrap asynchronous setup inside {@link defer} within the returned child source. Defaults to `concurrency: 1`.
 * @throws {RangeError} If `concurrency` is not a positive safe integer.
 * @category Higher-order
 */
export function concatMap<T, U>(
  mapper: (value: T, context: OperatorContext) => Source<U>,
  options: HigherOrderOptions = {},
): Operator<T, U> {
  return projectChildren(mapper, true, options);
}
