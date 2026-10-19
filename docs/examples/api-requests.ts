import { from, map, rateLimit } from "mogami-stream";

export function crawl(urls: AsyncIterable<string>, signal: AbortSignal) {
  return from(urls)
    .pipe(
      rateLimit({ count: 5, perMs: 1_000 }),
      map(
        async (url, { signal }) => {
          const response = await fetch(url, { signal });
          if (!response.ok) {
            await response.body?.cancel();
            throw new Error(`HTTP ${response.status}`);
          }
          return { url, text: await response.text() };
        },
        {
          concurrency: 5,
          capacity: 5,
          ordered: false,
        },
      ),
    )
    .forEach(
      ({ url, text }) => {
        console.log(url, text.length);
      },
      { signal },
    );
}
