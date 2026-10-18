import { create, open, type Source, type Stream } from "../core/stream.js";
import { ChildSlot } from "../internal/child-slots.js";
import { notifications } from "../internal/notifications.js";
import { done, item } from "../internal/deferred.js";

/**
 * Tuple of value types corresponding to the supplied source tuple.
 */
export type SourceValues<S extends readonly Source<unknown>[]> = {
  -readonly [K in keyof S]: S[K] extends Source<infer T> ? T : never;
};

/**
 * Open and drain inputs in sequence, waiting for each input to close before opening the next.
 * @category Combination
 */
export function chain<T>(...sources: Source<T>[]): Stream<T> {
  return create((scope) => {
    let index = 0;
    let upstream: ReturnType<typeof open<T>> | undefined;
    return {
      async next() {
        while (index < sources.length) {
          upstream ??= open(sources[index]!, scope);
          const result = await upstream.next();
          if (!result.done) return result;
          await upstream.scope.close();
          upstream = undefined;
          index++;
        }
        return done();
      },
    };
  });
}

/**
 * Read inputs concurrently with one read/result slot per input.
 *
 * Ready inputs are selected in rotating order. No new read starts while downstream is idle.
 * @category Combination
 */
export function merge<T>(...sources: Source<T>[]): Stream<T> {
  return create((scope) => {
    const changed = notifications(scope);
    const slots = sources.map(
      (source) => new ChildSlot(source, scope, changed.notify),
    );
    let started = false;
    let refill: ChildSlot<T> | undefined;
    let cursor = 0;
    return {
      async next() {
        for (;;) {
          scope.check();
          const version = changed.version;
          if (!started) {
            started = true;
            slots.forEach((slot) => slot.start());
          }
          refill?.start();
          refill = undefined;
          for (let remaining = slots.length; remaining > 0; remaining--) {
            if (cursor === slots.length) cursor = 0;
            const slot = slots[cursor]!;
            if (slot.failure) throw slot.failure.error;
            if (!slot.result) {
              cursor++;
              continue;
            }
            const result = slot.consume();
            if (result.done) {
              // Keep the cursor on the successor shifted into this position.
              slots.splice(cursor, 1);
              continue;
            }
            cursor++;
            refill = slot;
            return result;
          }
          if (!slots.length) return done();
          // A synchronous refill may have notified while we inspected slots.
          if (changed.version !== version) continue;
          await changed.promise;
        }
      },
    };
  });
}

/**
 * Emit typed tuples of corresponding values, stopping at the shortest input.
 *
 * Other inputs are closed; discovering the shortest input can consume one extra value from another input.
 * @category Combination
 */
export function zip<const S extends readonly Source<unknown>[]>(
  ...sources: S
): Stream<SourceValues<S>> {
  return create((scope) => {
    const changed = notifications(scope);
    const slots = sources.map(
      (source) => new ChildSlot(source, scope, changed.notify),
    );
    return {
      async next() {
        if (!slots.length) return done();
        slots.forEach((slot) => slot.start());
        for (;;) {
          scope.check();
          const version = changed.version;
          for (const slot of slots) {
            if (slot.failure) throw slot.failure.error;
            if (slot.result?.done) return done();
          }
          if (slots.every((slot) => slot.result)) {
            return item(
              slots.map((slot) => slot.consume().value) as SourceValues<S>,
            );
          }
          // A synchronous refill may have notified while we inspected slots.
          if (changed.version !== version) continue;
          await changed.promise;
        }
      },
    };
  });
}

/**
 * Controls whether latest-value tuples may include unseen inputs.
 */
export interface ZipLatestOptions {
  /** Emit before every input has a value, using undefined for unseen inputs. Defaults to false. */
  readonly partial?: boolean;
}
/**
 * Tuple of source value types, including undefined for inputs that have not emitted.
 */
export type PartialSourceValues<S extends readonly Source<unknown>[]> = {
  -readonly [K in keyof S]: S[K] extends Source<infer T>
    ? T | undefined
    : never;
};
export function zipLatest<const S extends readonly Source<unknown>[]>(
  sources: S,
  options: { readonly partial: true },
): Stream<PartialSourceValues<S>>;
export function zipLatest<const S extends readonly Source<unknown>[]>(
  sources: S,
  options?: { readonly partial?: false },
): Stream<SourceValues<S>>;
export function zipLatest<const S extends readonly Source<unknown>[]>(
  sources: S,
  options: ZipLatestOptions,
): Stream<PartialSourceValues<S>>;
/**
 * Emit tuples of the latest observed values from each input.
 *
 * By default, wait for every input to emit before producing a tuple. Partial mode emits earlier and uses undefined for unseen inputs. Updates may coalesce before initialization; accepted updates are retained afterward until consumed.
 * @category Combination
 */
export function zipLatest(
  sources: readonly Source<unknown>[],
  { partial = false }: ZipLatestOptions = {},
): Stream<unknown[]> {
  return create((scope) => {
    const changed = notifications(scope);
    const slots = sources.map(
      (source) => new ChildSlot(source, scope, changed.notify),
    );
    const seen = new Set<number>();
    const ended = new Set<number>();
    const latest: unknown[] = Array(sources.length).fill(undefined);
    let started = false;
    let refill: ChildSlot<unknown> | undefined;
    let cursor = 0;
    return {
      async next() {
        for (;;) {
          scope.check();
          const version = changed.version;
          if (!started) {
            started = true;
            slots.forEach((slot) => slot.start());
          }
          refill?.start();
          refill = undefined;
          const scanStart = cursor;
          for (let offset = 0; offset < slots.length; offset++) {
            const index = (scanStart + offset) % slots.length;
            if (ended.has(index)) continue;
            const slot = slots[index]!;
            if (slot.failure) throw slot.failure.error;
            if (!slot.result) continue;
            const result = slot.consume();
            if (result.done) {
              ended.add(index);
              if (!partial && !seen.has(index)) return done();
            } else {
              seen.add(index);
              latest[index] = result.value;
              cursor = (index + 1) % slots.length;
              if (partial || seen.size === slots.length) {
                refill = slot;
                return item([...latest]);
              }
              slot.start();
            }
          }
          if (ended.size === slots.length) return done();
          // A synchronous refill may have notified while we inspected slots.
          if (changed.version !== version) continue;
          await changed.promise;
        }
      },
    };
  });
}
