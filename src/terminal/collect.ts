import {
  create,
  open,
  type Source,
  type OperatorContext,
} from "../core/stream.js";
import { done } from "../internal/deferred.js";
import { iterate, type ExecutionOptions } from "./iterate.js";

/**
 * Consumes all values without retaining them and resolves after cleanup.
 * @category Consumption
 */
export async function drain<T>(
  source: Source<T>,
  options: ExecutionOptions = {},
): Promise<void> {
  const iterator = iterate(source, options);
  try {
    while (!(await iterator.next()).done) {
      /* Discard values without invoking a per-item callback. */
    }
  } finally {
    await iterator.closed;
  }
}

/**
 * Counts all values without retaining them and resolves after cleanup. Empty sources return zero.
 * @category Consumption
 */
export async function count<T>(
  source: Source<T>,
  options: ExecutionOptions = {},
): Promise<number> {
  const iterator = iterate(source, options);
  let total = 0;
  try {
    while (!(await iterator.next()).done) total++;
    return total;
  } finally {
    await iterator.closed;
  }
}

/**
 * Collects values directly into a Set and resolves after cleanup.
 *
 * Uses SameValueZero equality, preserves insertion order, and does not unwrap Promise-valued items.
 * @category Consumption
 */
export async function toSet<T>(
  source: Source<T>,
  options: ExecutionOptions = {},
): Promise<Set<T>> {
  const iterator = iterate(source, options);
  const values = new Set<T>();
  try {
    for (;;) {
      const result = await iterator.next();
      if (result.done) return values;
      values.add(result.value);
    }
  } finally {
    await iterator.closed;
  }
}

/**
 * Collects values into a Map by sequentially awaiting `keySelector` for each item and resolves after cleanup.
 *
 * Duplicate keys keep the last value without changing key insertion order. Values are not unwrapped; keys use SameValueZero equality. Empty sources return an empty Map.
 * @category Consumption
 */
export async function toMap<T, K>(
  source: Source<T>,
  keySelector: (value: T, context: OperatorContext) => K,
  options: ExecutionOptions = {},
): Promise<Map<Awaited<K>, T>> {
  const values = new Map<Awaited<K>, T>();
  const collection = create<never>((scope) => {
    const upstream = open(source, scope);
    let index = 0;
    return {
      async next() {
        for (;;) {
          const result = await upstream.next();
          if (result.done) return done();
          scope.check();
          const key = await keySelector(result.value, {
            index: index++,
            signal: scope.signal,
          });
          scope.check();
          values.set(key, result.value);
        }
      },
    };
  });
  await drain(collection, options);
  return values;
}
