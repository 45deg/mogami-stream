import { from, map } from "mogami-stream";

export function processWithRetry<T, U>(
  jobs: AsyncIterable<T>,
  processJob: (job: T, signal: AbortSignal) => Promise<U>,
  isTransient: (error: unknown) => boolean,
) {
  return from(jobs).pipe(
    map((job, { signal }) => processJob(job, signal), {
      concurrency: 4,
      capacity: 4,
      retry: {
        retries: 3,
        shouldRetry: isTransient,
        delayMs: (_error, { attempt }) =>
          Math.min(200 * 2 ** (attempt - 1), 5_000),
      },
    }),
  );
}
