import { create, open, type Source, type Stream } from "../core/stream.js";
import { done, item } from "../internal/deferred.js";

export function from<T>(source: Source<T>): Stream<T> {
  return create((scope) => {
    let reader: ReturnType<typeof open<T>> | undefined;
    return {
      next() {
        reader ??= open(source, scope);
        return reader.next();
      },
    };
  });
}

export function defer<T>(
  factory: (context: {
    readonly signal: AbortSignal;
  }) => Source<T> | PromiseLike<Source<T>>,
): Stream<T> {
  return create((scope) => {
    let reader: ReturnType<typeof open<T>> | undefined;
    return {
      async next() {
        if (!reader) {
          const source = await scope.observe(
            Promise.resolve().then(() => {
              scope.check();
              return factory({ signal: scope.signal });
            }),
          );
          scope.check();
          reader = open(source, scope);
        }
        return reader.next();
      },
    };
  });
}

export function of<T>(...values: T[]): Stream<T> {
  return from(values);
}
export function empty<T = never>(): Stream<T> {
  return from<T>([]);
}

export function range(start: number, end: number, step = 1): Stream<number> {
  for (const [name, value] of [
    ["start", start],
    ["end", end],
    ["step", step],
  ] as const) {
    if (!Number.isSafeInteger(value))
      throw new RangeError(`${name} must be a safe integer.`);
  }
  if (step === 0) throw new RangeError("step must not be zero.");
  return create(() => {
    let current = start;
    let exhausted = false;
    return {
      async next() {
        if (exhausted || (step > 0 ? current >= end : current <= end))
          return done();
        const value = current;
        // Test the next bound without overflowing a safe integer.
        const remaining = BigInt(end) - BigInt(current);
        if (step > 0 ? BigInt(step) >= remaining : BigInt(step) <= remaining)
          exhausted = true;
        else current += step;
        return item(value);
      },
    };
  });
}
