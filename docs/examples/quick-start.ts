import { from } from "mogami-stream";

const urls = ["https://example.com/a", "https://example.com/b"];
const controller = new AbortController();

const texts = from(urls)
  .rateLimit({ count: 5, perMs: 1_000 })
  .map(
    async (url, { signal }) => {
      const response = await fetch(url, { signal });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return response.text();
    },
    { concurrency: 5, capacity: 5, ordered: false },
  )
  .take(20);

const results = await texts.toArray({ signal: controller.signal });
console.log(results);
