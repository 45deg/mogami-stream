import { create, open, type Source, type Stream } from "../core/stream.js";
import { ChildSlot, notifications } from "../internal/child-slots.js";
import { done, item } from "../internal/deferred.js";

export type SourceValues<S extends readonly Source<unknown>[]> = {
  -readonly [K in keyof S]: S[K] extends Source<infer T> ? T : never;
};

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
          upstream = undefined;
          index++;
        }
        return done();
      },
    };
  });
}

export function merge<T>(...sources: Source<T>[]): Stream<T> {
  return create((scope) => {
    const changed = notifications(scope);
    const slots = sources.map(
      (source) => new ChildSlot(source, scope, changed.notify),
    );
    const ended = new Set<number>();
    let cursor = 0;
    return {
      async next() {
        for (;;) {
          scope.check();
          const notification = changed.promise;
          slots.forEach((slot, i) => {
            if (!ended.has(i)) slot.start();
          });
          const scanStart = cursor;
          for (let offset = 0; offset < slots.length; offset++) {
            const index = (scanStart + offset) % slots.length;
            if (ended.has(index)) continue;
            const slot = slots[index]!;
            if (slot.failure) throw slot.failure.error;
            if (!slot.result) continue;
            const result = slot.consume();
            if (result.done) ended.add(index);
            else {
              cursor = (index + 1) % slots.length;
              return result;
            }
          }
          if (ended.size === slots.length) return done();
          await notification;
        }
      },
    };
  });
}

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
          const notification = changed.promise;
          for (const slot of slots) {
            if (slot.failure) throw slot.failure.error;
            if (slot.result?.done) return done();
          }
          if (slots.every((slot) => slot.result)) {
            return item(
              slots.map((slot) => slot.consume().value) as SourceValues<S>,
            );
          }
          await notification;
        }
      },
    };
  });
}

export interface ZipLatestOptions {
  readonly partial?: boolean;
}
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
    let cursor = 0;
    return {
      async next() {
        for (;;) {
          scope.check();
          const notification = changed.promise;
          slots.forEach((slot, index) => {
            if (!ended.has(index)) slot.start();
          });
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
              if (partial || seen.size === slots.length)
                return item([...latest]);
              slot.start();
            }
          }
          if (ended.size === slots.length) return done();
          await notification;
        }
      },
    };
  });
}
