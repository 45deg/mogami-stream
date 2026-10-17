import type { Source } from "../core/stream.js";
import { iterate, type ExecutionOptions } from "./iterate.js";

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
