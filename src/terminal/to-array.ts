import type { Source } from "../core/stream.js";
import { iterate, type ExecutionOptions } from "./iterate.js";

/**
 * Collect all values in memory and wait for cleanup before settling.
 *
 * Accepts synchronous and asynchronous iterables. Processing errors remain primary if cleanup also fails.
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
