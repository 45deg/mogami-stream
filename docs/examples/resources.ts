import { defer, take } from "mogami-stream";

interface Cursor<T> {
  rows(options: { signal: AbortSignal }): AsyncIterable<T>;
  close(): Promise<void>;
}

export async function readFirstRows<T>(
  openCursor: (options: { signal: AbortSignal }) => Promise<Cursor<T>>,
  signal: AbortSignal,
) {
  const pages = defer(({ signal }) =>
    (async function* () {
      const cursor = await openCursor({ signal });
      try {
        yield* cursor.rows({ signal });
      } finally {
        await cursor.close();
      }
    })(),
  );

  const runA = await pages.pipe(take(10)).toArray({ signal });
  const runB = await pages.pipe(take(10)).toArray({ signal });
  return { runA, runB };
}
