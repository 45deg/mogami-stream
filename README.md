# mogami-stream

TypeScript operators for standard `AsyncIterable` pipelines, with bounded concurrency, explicit cancellation, and zero runtime dependencies. The working package name comes from this repository. The implementation follows the supplied v0.3 design.

```ts
import { from, map, rateLimit, take, toArray } from "mogami-stream";

const texts = from(urls).pipe(
  rateLimit({ count: 5, perMs: 1_000 }),
  map(
    async (url, { signal }) => {
      const response = await fetch(url, { signal });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return response.text();
    },
    { concurrency: 5, capacity: 5, ordered: false },
  ),
  take(20),
);

const results = await toArray(texts, { signal });
```

`rateLimit` limits starts of upstream `next()` calls in this execution. It does not coordinate HTTP requests across executions or count retries inside a mapper.

## Execution and cleanup

Construction, `.pipe()`, and iterator acquisition are lazy. The first `next()` starts execution. `take(0)` and an already aborted external signal never open the input. `from` preserves the input's replayability; `defer` creates a fresh input for each execution.

```ts
import { defer, iterate } from "mogami-stream";

const source = defer(({ signal }) => openResource(signal));
const iterator = iterate(source, { signal: controller.signal });

try {
  for await (const value of iterator) {
    await consume(value);
  }
} finally {
  const report = await iterator.closed;
  // report.kind: "completed" | "cancelled" | "failed"
  // report.reason: the primary error or cancellation reason
  // report.suppressedErrors: additional processing and cleanup failures
}
```

`cancel(reason)` immediately requests a stop. Pending `next()` rejects without waiting for cleanup. `return()` sends the stop request before awaiting cleanup. `closed` always resolves with a report, including when cleanup fails. `throw(error)` stops execution with that error instead of injecting it into the upstream generator.

Only one public `next()` may be pending. A second overlapping request rejects with `TypeError`. A failure or cancellation is reported once through `next()`; later reads return `done: true`. `toArray` and `reduce` wait for cleanup before settling and preserve a primary processing error.

Non-cooperative promises and input reads can keep `closed`, `return()`, and terminal consumers pending indefinitely. Async generators cannot forcibly interrupt an arbitrary `await`. Pass the supplied signal to external I/O through `defer` or callbacks. Signal propagation through arbitrary third-party wrappers is limited to that boundary.

`take(n)` starts closing its owned upstream before returning the nth value. It leaves downstream callbacks' signals active. Already started effects may still complete. `finalize` runs once for a started stage, after its children and outstanding work finish. Nested finalizers run from the inner stage outward.

## Concurrency and demand

`map(mapper, { concurrency = 1, capacity = concurrency, ordered = true })` awaits synchronous or asynchronous callback results. Its context contains `{ index, signal }`; indexes start at zero for each operator execution.

Both limits must be positive safe integers, and `capacity >= concurrency`. The total number of reserved input reads, running mappers, and completed outputs stays within `capacity`. A reservation also occupies a concurrency slot. At most one upstream read is pending per input.

Ordered map preserves input order, including when the first mapper is slow. Unordered map emits in observed completion order. Any observed mapper failure stops the execution immediately, even behind an unfinished earlier result.

Work advances while a downstream read is pending. Once a value is delivered, no new input reservations are made until the next request. An already reserved read and its associated work may finish. Each concurrent stage has its own bounds; these limits do not include external source buffers or a terminal `toArray` result.

## Public API

All stream factories and operators return a `Stream<T>`, which implements `AsyncIterable<T>` and adds `.pipe()`. Operators also work with arbitrary `AsyncIterable` inputs. Creation and combination functions accept synchronous `Iterable` inputs too. The standalone `pipe(source, ...operators)` and `.pipe(...operators)` both infer types for zero through eight operators. Split longer pipelines into multiple `.pipe()` calls.

| API                                                             | Behavior                                                                                                                                                |
| --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `from(source)`, `defer(({ signal }) => source)`                 | Wrap an existing input or create one per execution; deferred factories may return a promise of an input.                                                |
| `of(...values)`, `empty()`, `range(start, end, step = 1)`       | Create inputs; range excludes `end` and requires safe integers and nonzero step.                                                                        |
| `map(mapper, options?)`, `filter(predicate)`                    | Transform values or select them; synchronous type predicates retain narrowing.                                                                          |
| `take(n)`, `skip(n)`, `takeWhile(predicate)`                    | Select a prefix, skip values, or stop when a predicate becomes false.                                                                                   |
| `chunks(size)`                                                  | Emit arrays of at most `size` values, including a final partial array on normal completion.                                                             |
| `chain(...sources)`                                             | Open and drain inputs in sequence.                                                                                                                      |
| `merge(...sources)`                                             | Read inputs concurrently with one slot per input and rotating selection among ready inputs.                                                             |
| `zip(...sources)`                                               | Emit typed tuples; stop at the shortest input and close the others.                                                                                     |
| `zipLatest(sources, { partial = false }?)`                      | Emit tuples of latest values; by default wait for all first values. Partial mode includes `undefined` for unseen inputs and reflects that in its types. |
| `flatten({ concurrency = 1 }?)`, `concat({ concurrency = 1 }?)` | Flatten a stream of inputs, concurrently or in child order.                                                                                             |
| `flatMap(mapper, options?)`, `concatMap(mapper, options?)`      | Map to child inputs and flatten. Child factories synchronously return an iterable; asynchronous preparation belongs inside a deferred child.            |
| `tap(effect)`, `finalize(effect)`                               | Await a per-value effect or an execution cleanup effect.                                                                                                |
| `iterate(source, { signal }?)`                                  | Return a controlled iterator with `cancel()` and `closed`.                                                                                              |
| `toArray(source, options?)`                                     | Collect all values in memory.                                                                                                                           |
| `reduce(source, reducer, initial, options?)`                    | Sequential reduction with a required initial accumulator; reducer receives `(accumulator, value, context)`.                                             |

Compared with the aiostream API described in the supplied design, this library requires finite concurrency (default 1), defaults `zipLatest` to `partial: false`, and names its timing operators `delayStart` and `timeoutEach`.

`filter`, `takeWhile`, `tap`, and `reduce` callbacks run sequentially. `take` and `skip` require non-negative safe integers. `chunks` requires a positive safe integer. Invalid numeric options throw `RangeError` when the operator is constructed.

Higher-order concurrency counts children from reservation through retirement, including cleanup. Each child has one read/result slot. `concat` retains completed successors until earlier children retire, so a blocked head cannot accumulate unlimited empty successors. `zip` may consume one extra value from another input before discovering the shortest input has ended. `zipLatest` coalesces updates before initialization; afterward it retains each accepted update until demand consumes it.

Values that happen to be promises retain their identity in `from`, selection, and combination operators. Only callback results that explicitly require awaiting, such as `map` results, are resolved. Native `for await` over a synchronous iterable has its own adaptation rules; use this library's `from` when promise identity matters.

## Time operators

| API                                  | Contract                                                                                                                                                            |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `spaceout(intervalMs)`               | First output is immediate; later outputs are at least `intervalMs` apart on the scheduler clock. Slow upstream reads and downstream idle time count toward the gap. |
| `rateLimit({ count, perMs })`        | Limit upstream read starts within a sliding window. Starts exactly `perMs` apart belong to different windows; reads that fail or finish the input still count.      |
| `delayStart(ms)`                     | Wait once before opening upstream.                                                                                                                                  |
| `timeoutEach(ms)`                    | Fail with `TimeoutError` if an individual upstream read times out. Downstream idle time does not count.                                                             |
| `deadline(durationMs)`               | Fail with `DeadlineExceededError` after the stage's elapsed time budget, including downstream idle. Zero expires without opening upstream.                          |
| `bufferTime(windowMs, { maxItems })` | Emit a nonempty array when its first value's window expires or its item limit is reached. Flush a final partial array only on normal completion.                    |

Durations are finite, non-negative milliseconds; `perMs` and `windowMs` must be positive. `count` and `maxItems` must be positive safe integers. `timeoutEach(0)` establishes an immediate deadline and does not guarantee a successful read.

`bufferTime` can flush while one upstream read is pending. After delivery, that reservation may produce one value retained for the next request. No further reads start during downstream idle, and no queue of completed windows grows in the background. Window timestamps describe when this library observes values, rather than when external push events occurred.

All waits are abortable. Timers and listeners are released on completion or stop. Long waits are split into host-safe timer intervals. Time uses `performance.now()` and has no wall-clock fallback. A private per-execution scheduler supports deterministic tests. Host clock behavior and timer scheduling determine precision, particularly in Cloudflare Workers.

## Development and validation

```sh
pnpm install --frozen-lockfile
pnpm check
pnpm test:bun
pnpm test:package
pnpm exec playwright install chromium
pnpm test:browser
pnpm test:cloudflare
```

Run `pnpm bench --items=5000 --samples=5 --warmup=2` to measure selection, transformation, combination, timing, lifecycle, and terminal operations with the default scheduler. The benchmark checks output values, warms each case, and reports median timings. Timed map cases use at most 128 items, spacing and rate-limit waits use at most 64, and CPU callback cases use at most 256; other cases use `--items`. It samples heap usage and timer delay separately from timing; sampled peak heap can miss short-lived allocations, and retained heap deltas after GC can be negative. Use `--baseline=/absolute/path/to/dist/index.js` for alternating before/after runs, `--case=map-sequential` to focus a case, or `--json=/tmp/benchmark.json` to save measurements. Results depend on the host, runtime, callbacks, and workload.

The browser runner uses installed Google Chrome on macOS when available; otherwise it uses Playwright Chromium. Set `PLAYWRIGHT_EXECUTABLE_PATH` to select another compatible executable. The Workers test uses local Miniflare/workerd, requires no account, and never deploys. Only the `workerd` dependency is allowed an install-time build script.

Node.js, Bun, browser main threads, Dedicated Workers, Service Workers, and Cloudflare Workers are targets. Deno, CommonJS, stream/event adapters, Worker creation, and the `switch`, `debounce`, `throttle`, `sample`, and `interval` families are outside the initial scope.

ESM JavaScript and declarations are emitted to `dist`. The runtime imports only its own distribution modules. No timers or background work start during import.
