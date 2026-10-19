// #region collect
import { from } from "mogami-stream";

export async function collectValues() {
  const numbers = from([1, 2, 3]);
  const values = await numbers.toArray(); // [1, 2, 3]
  const total = await numbers.reduce((sum, value) => sum + value, 0); // 6
  return { values, total };
}
// #endregion collect

// #region each
export async function processValues<T>(
  input: Iterable<T> | AsyncIterable<T>,
  save: (value: T, signal: AbortSignal) => Promise<void>,
  signal: AbortSignal,
) {
  await from(input).forEach((value, { signal }) => save(value, signal), {
    signal,
  });
}
// #endregion each

// #region match
export async function checkValues() {
  const numbers = from([1, 2, 3]);
  const first = await numbers.find((value) => value > 1); // 2
  const any = await numbers.some((value) => value > 2); // true
  const all = await numbers.every((value) => value > 0); // true
  return { first, any, all };
}
// #endregion match
