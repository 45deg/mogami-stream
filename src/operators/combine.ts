import { create, open, type Source, type Stream } from "../core/stream.js";
import { ChildSlot } from "../internal/child-slots.js";
import { notifications } from "../internal/notifications.js";
import { done, item } from "../internal/deferred.js";

/**
 * Tuple of element types extracted from a tuple of {@link Source} inputs.
 */
export type SourceValues<S extends readonly Source<unknown>[]> = {
  -readonly [K in keyof S]: S[K] extends Source<infer T> ? T : never;
};

/**
 * Concatenates multiple sources sequentially, waiting for each source to close before opening the next.
 * @category Combination
 */
export function chain<const S extends readonly Source<unknown>[]>(
  ...sources: S
): Stream<SourceValues<S>[number]>;
export function chain<T>(...sources: Source<T>[]): Stream<T>;
export function chain(...sources: Source<unknown>[]): Stream<unknown> {
  return create((scope) => {
    let index = 0;
    let upstream: ReturnType<typeof open<unknown>> | undefined;
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
 * Merges multiple sources concurrently, holding at most one pending read or buffered result per source.
 *
 * Ready sources are polled in round-robin order, and no new reads start while downstream is idle.
 * @category Combination
 */
export function merge<const S extends readonly Source<unknown>[]>(
  ...sources: S
): Stream<SourceValues<S>[number]>;
export function merge<T>(...sources: Source<T>[]): Stream<T>;
export function merge(...sources: Source<unknown>[]): Stream<unknown> {
  return create((scope) => {
    const changed = notifications(scope);
    const slots = sources.map(
      (source) => new ChildSlot(source, scope, changed.notify),
    );
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
 * Combines corresponding values from each source into typed tuples, stopping as soon as any source completes.
 *
 * Remaining sources are closed when the shortest source finishes. Detecting that a source has ended may require reading one extra value from sibling sources.
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
 * Options for {@link zipLatest}.
 */
export interface ZipLatestOptions {
  /** When `true`, emits tuples before all sources have produced their first value, filling unseen positions with `undefined`. Defaults to `false`. */
  readonly partial?: boolean;
}
/**
 * Tuple of element types for {@link zipLatest} in partial mode, where each position may also be `undefined`.
 */
export type PartialSourceValues<S extends readonly Source<unknown>[]> = {
  -readonly [K in keyof S]: S[K] extends Source<infer T>
    ? T | undefined
    : never;
};
/**
 * Emits tuples containing the latest value observed from each source.
 *
 * By default (`partial: false`), waits until every source has emitted at least once before producing a tuple. With `partial: true`, emits immediately and fills unseen positions with `undefined`. Updates arriving before the first tuple is ready may coalesce; afterward, reads are driven by downstream demand.
 * @category Combination
 */
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
