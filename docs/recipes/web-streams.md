# Web Streams

Use `fromReadableStream` to process a fetch body with mogami-stream, and `toReadableStream` to return the result as an HTTP response. Both functions are exported from `mogami-stream` and `mogami-stream/core`; streams also expose `.toReadableStream(options)`.

```ts
import { fromReadableStream } from "mogami-stream";

export function relay(response: Response, signal: AbortSignal): Response {
  if (!response.body) return new Response(null, { status: response.status });
  const body = fromReadableStream(response.body)
    .map((chunk) => chunk)
    .toReadableStream({ signal });
  return new Response(body, { status: response.status });
}
```

HTTP bodies require byte chunks such as `Uint8Array`. These adapters preserve chunk values; they do not encode objects or strings, and the output is a default stream, not a BYOB byte stream.

## Ownership and cancellation

`fromReadableStream(source)` acquires a reader on the first read. Normal exhaustion releases the lock without cancelling the source. Early termination, including `take`, cancels the reader and then releases its lock after pending work and cancellation finish. Read failures also release the lock. An already locked source fails when consumption starts and remains owned by its existing reader.

The input is single-use: wrapping it does not make it replayable. Use `defer` with a factory that creates a new source when each execution needs fresh data.

`toReadableStream(source, { signal })` creates one execution. Cancelling the returned stream forwards the reason upstream and waits for cleanup. Cleanup failures during cancellation reject the cancellation promise with an `AggregateError`; processing failures error the output with the primary reason. The returned Web Stream does not expose the execution's `CloseReport` or its full set of suppressed errors.

An external abort stops the execution even when the output is idle. Pending reads reject on cancellation; an idle output is errored when execution cleanup finishes. Work that ignores cancellation can delay cleanup. If you use `getReader()`, release your output reader's lock yourself after consumption.

## Demand and buffering

The input adapter adds no queue and calls `reader.read()` only when requested. The output uses `highWaterMark: 0`: construction and acquiring a reader do not start reading, and at most one upstream read is active. Multiple pending Web reads are served in order. Cancellation before the first pull never opens the source.

The original Web Stream may maintain its own queue, and concurrent operators may retain in-flight work and results within their configured capacity. The zero output high-water mark does not remove those buffers or change their limits.
