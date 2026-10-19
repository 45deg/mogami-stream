import { bufferTime, chunks, from } from "mogami-stream";

export async function writeBatches<T>(
  rows: AsyncIterable<T>,
  insertMany: (batch: T[], signal: AbortSignal) => Promise<void>,
  signal: AbortSignal,
) {
  await from(rows)
    .pipe(chunks(100))
    .forEach((batch, { signal }) => insertMany(batch, signal), { signal });
}

export async function writeTimedBatches<T>(
  rows: AsyncIterable<T>,
  insertMany: (batch: T[], signal: AbortSignal) => Promise<void>,
  signal: AbortSignal,
) {
  await from(rows)
    .pipe(bufferTime(1_000, { maxItems: 100 }))
    .forEach((batch, { signal }) => insertMany(batch, signal), { signal });
}
