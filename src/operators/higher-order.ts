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

export interface HigherOrderOptions {
  readonly concurrency?: number;
}

function combineChildren<T>(
  ordered: boolean,
  options: HigherOrderOptions,
): Operator<Source<T>, T> {
  const concurrency = options.concurrency ?? 1;
  integer(concurrency, "concurrency");
  return (source) =>
    create((scope) => {
      const outer = open(source, scope);
      const changed = notifications(scope);
      const slots: ChildSlot<T>[] = [];
      let pulling = false;
      let outerDone = false;
      let demand = false;
      let cursor = 0;
      let failure: { error: unknown } | undefined;
      let refill: ChildSlot<T> | undefined;

      function fill(): void {
        if (
          !demand ||
          pulling ||
          outerDone ||
          scope.signal.aborted ||
          slots.length >= concurrency
        )
          return;
        pulling = true;
        void scope
          .observe(
            (async () => {
              const result = await outer.next();
              pulling = false;
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
            })(),
          )
          .catch((error) => {
            pulling = false;
            failure = { error };
            changed.notify();
          });
      }

      return {
        async next() {
          demand = true;
          try {
            for (;;) {
              scope.check();
              const notification = changed.promise;
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
              await notification;
            }
          } finally {
            demand = false;
          }
        },
      };
    });
}

export function flatten<T>(
  options: HigherOrderOptions = {},
): Operator<Source<T>, T> {
  return combineChildren(false, options);
}
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

export function flatMap<T, U>(
  mapper: (value: T, context: OperatorContext) => Source<U>,
  options: HigherOrderOptions = {},
): Operator<T, U> {
  const combine = flatten<U>(options);
  return (source) => combine(project(mapper)(source));
}
export function concatMap<T, U>(
  mapper: (value: T, context: OperatorContext) => Source<U>,
  options: HigherOrderOptions = {},
): Operator<T, U> {
  const combine = concat<U>(options);
  return (source) => combine(project(mapper)(source));
}
