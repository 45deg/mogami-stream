import type { Source } from "../core/stream.js";
import { iterate, type ExecutionOptions } from "./iterate.js";

/**
 * Collects all values from `source` into an array and waits for cleanup before resolving.
 *
 * Accepts synchronous and asynchronous iterables. If both stream processing and cleanup fail, the primary processing error is thrown.
 * @category Consumption
 */
export async function toArray<T>(
  source: Source<T>,
  options: ExecutionOptions = {},
): Promise<T[]> {
  const iterator = iterate(source, options);
  const values: T[] = [];
  try {
    for (;;) {
      const result = await iterator.next();
      if (result.done) return values;
      values.push(result.value);
    }
  } finally {
    await iterator.closed;
  }
}
