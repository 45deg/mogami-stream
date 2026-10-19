import { catchError, defer } from "mogami-stream";

interface Cursor {
  read(signal: AbortSignal): Promise<IteratorResult<string>>;
  close(): Promise<void>;
}

type OpenCursor = (signal: AbortSignal) => Promise<Cursor>;

// The source borrows the cursor. Only the registered release callback closes it.
async function* rows(cursor: Cursor, signal: AbortSignal) {
  while (true) {
    const result = await cursor.read(signal);
    if (result.done) return;
    yield result.value;
  }
}

export function readWithFallback(
  openCursor: OpenCursor,
  openBackup: OpenCursor,
) {
  return defer(async ({ signal, acquire }) => {
    const cursor = await acquire(openCursor, (cursor) => cursor.close());
    return rows(cursor, signal);
  }).pipe(
    catchError(async (_error, { signal, acquire }) => {
      const backup = await acquire(openBackup, (cursor) => cursor.close());
      return rows(backup, signal);
    }),
  );
}
