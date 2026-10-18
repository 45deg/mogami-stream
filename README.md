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

## API reference

Generate the API reference from the TypeScript public entry point and its documentation comments with [TypeDoc](https://typedoc.org/):

```sh
pnpm install --frozen-lockfile
pnpm run docs
```

Open `docs/api/index.html` in a browser. The generated reference includes public functions, overloads, options, callback contexts, and Stream methods. Generated HTML is ignored by Git and excluded from the npm package. Run `pnpm run docs` after changing public signatures or documentation comments. CI also runs the generator and treats warnings as errors.

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

Only one public `next()` may be pending. A second overlapping request rejects with `TypeError`. A failure or cancellation is reported once through `next()`; later reads return `done: true`. `toArray`, `reduce`, `forEach`, `find`, `some`, and `every` wait for cleanup before settling and preserve a primary processing error.

Non-cooperative promises and input reads can keep `closed`, `return()`, and terminal consumers pending indefinitely. Async generators cannot forcibly interrupt an arbitrary `await`. Pass the supplied signal to external I/O through `defer` or callbacks. Signal propagation through arbitrary third-party wrappers is limited to that boundary.

Pipelines do not automatically yield to host tasks. Continuously ready inputs and callbacks can delay timers, I/O, and cancellation initiated by those tasks. Sources, callbacks, or consumers that need host-task responsiveness must yield explicitly.

`take(n)` starts closing its owned upstream before returning the nth value. It leaves downstream callbacks' signals active. Already started effects may still complete. `finalize` runs once for a started stage, after its children and outstanding work finish. Nested finalizers run from the inner stage outward.

## Concurrency and demand

`map(mapper, { concurrency = 1, capacity = concurrency, ordered = true })` awaits synchronous or asynchronous callback results. Its context contains `{ index, signal, attempt }`; attempts start at one and indexes start at zero for each operator execution.

Both limits must be positive safe integers, and `capacity >= concurrency`. The total number of reserved input reads, running mappers, and completed outputs stays within `capacity`. A reservation also occupies a concurrency slot. At most one upstream read is pending per input.

Ordered map preserves input order, including when the first mapper is slow. Unordered map emits in observed completion order. Any observed mapper failure stops the execution immediately, even behind an unfinished earlier result.

Work advances while a downstream read is pending. Once a value is delivered, no new input reservations are made until the next request. An already reserved read and its associated work may finish. Each concurrent stage has its own bounds; these limits do not include external source buffers or a terminal `toArray` result.

## Public API

All stream factories and operators return a `Stream<T>`, which implements `AsyncIterable<T>` and adds `.pipe()`, `.iterate(options?)`, `.toArray(options?)`, `.reduce(reducer, initial, options?)`, `.forEach(effect, options?)`, `.find(predicate, options?)`, `.some(predicate, options?)`, and `.every(predicate, options?)`. Calling `stream.toArray({ signal })` is equivalent to `toArray(stream, { signal })`; `stream.reduce(reducer, initial, options)` is equivalent to `reduce(stream, reducer, initial, options)`. `stream.iterate(options)` is equivalent to `iterate(stream, options)`. The corresponding standalone terminal functions also accept arbitrary synchronous and asynchronous iterables. Operators also work with arbitrary `AsyncIterable` inputs. Creation and combination functions accept synchronous `Iterable` inputs too. The standalone `pipe(source, ...operators)` and `.pipe(...operators)` both infer types for zero through eight operators. Split longer pipelines into multiple `.pipe()` calls.

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

`filter`, `takeWhile`, `tap`, `reduce`, `scan`, `forEach`, `find`, `some`, and `every` callbacks run sequentially. `take` and `skip` require non-negative safe integers. `chunks` requires a positive safe integer. Invalid numeric options throw `RangeError` when the operator is constructed.

Higher-order concurrency counts children from reservation through retirement, including cleanup. Each child has one read/result slot. `concat` retains completed successors until earlier children retire, so a blocked head cannot accumulate unlimited empty successors. `zip` may consume one extra value from another input before discovering the shortest input has ended. `zipLatest` coalesces updates before initialization; afterward it retains each accepted update until demand consumes it.

Values that happen to be promises retain their identity in `from`, selection, and combination operators. Only callback results that explicitly require awaiting, such as `map` results, are resolved. Native `for await` over a synchronous iterable has its own adaptation rules; use this library's `from` when promise identity matters.

Additional APIs:

| API                                                                       | Behavior                                                                      |
| ------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| `forEach(source, effect, options?)`                                       | Await sequential effects without retaining results.                           |
| `find(source, predicate, options?)`                                       | Return the first match or `undefined`, with synchronous type-guard narrowing. |
| `some(source, predicate, options?)`, `every(source, predicate, options?)` | Short-circuit boolean tests.                                                  |
| `scan(reducer, initial)`                                                  | Emit each updated accumulator.                                                |
| `fromTimeline(events)`                                                    | Replay a demand-driven schedule of values, completion, or failure.            |

## Retry and settled mapping

`map(mapper, { retry, ...options })` retries only a failed mapper invocation for the same input. The optional `retry` object has type `RetryOptions` and requires an explicit finite non-negative safe-integer `retries` budget. Omit `retry` to disable retries. `retries: 2` allows at most three attempts. Context is `{ index, signal, attempt }`: `index` stays fixed for that input and `attempt` starts at one. Each execution starts fresh.

```ts
const results = from(jobs).pipe(
  map((job, { signal, attempt }) => processJob(job, { signal, attempt }), {
    concurrency: 4,
    capacity: 4,
    retry: {
      retries: 2,
      shouldRetry: (error) => isTransient(error),
      delayMs: (_error, { attempt }) =>
        Math.min(200 * 2 ** (attempt - 1), 5_000),
    },
  }),
);
```

`shouldRetry(error, context)` may return a boolean or PromiseLike<boolean>; by default every mapper failure is eligible. `delayMs` is a finite non-negative number or a synchronous callback returning one; its default is zero. Both hooks receive the attempt that just failed and run only when retry budget remains. A false decision propagates the mapper error. A hook failure or invalid computed delay fails `map`; in `mapSettled`, it becomes the item's rejected outcome. Exhaustion propagates the final mapper error.

All attempts, retry decisions, and retry waits for one item occupy the same concurrency and capacity slot. Waits use the execution scheduler and are abortable. Cancellation prevents further attempts; outstanding non-cooperative mapper or policy work still delays cleanup. Already reserved items can finish retrying while downstream is idle, but no new input is reserved. Upstream and cleanup failures are not retried. Retry hooks cannot recover execution cancellation.

An upstream `rateLimit` counts input reads, not individual attempts inside `map`. Apply request limiting inside the mapper when retries must share that limit. Retrying an effect can repeat a successful operation whose response was lost; use an appropriate retry predicate and idempotency mechanism for that operation.

`mapSettled(mapper, options?)` accepts the same mapper context and options as `map`. It emits `SettledResult<Awaited<U>>` for each input:

```ts
type SettledResult<T> =
  | { readonly status: "fulfilled"; readonly value: T }
  | { readonly status: "rejected"; readonly reason: unknown };

const outcomes = from(jobs).pipe(
  mapSettled((job, { signal }) => processJob(job, { signal }), {
    concurrency: 4,
    ordered: false,
    retry: { retries: 2 },
  }),
);
```

With `retry`, each item is retried before producing its outcome: eventual success produces a fulfilled outcome, while retry exhaustion or a false retry decision produces a rejected outcome with the final mapper error. Synchronous throws, rejected promises, and rejected thenables from the mapper are handled alike without stopping other items. Upstream failures, cleanup failures, and execution cancellation still fail or cancel the execution normally. Late mapper failures after cancellation remain in the close report rather than becoming output values. Both operators share the same `MapOptions`, including `retry`, and preserve the bounds, ordering defaults, laziness, and cleanup behavior of `map`. Their mapper context is `MapContext`; even without retries, it includes `attempt: 1`.

## Consumption and running aggregates

`forEach(source, effect, options?)` awaits each effect without collecting values. Effects receive `{ index, signal }`; their return values are discarded. `find(source, predicate, options?)` returns the first matching value, `some` tests whether any value matches, and `every` tests whether all values match. Each also has an equivalent Stream method. Synchronous type predicates narrow the result of `find`.

These terminal operations accept `{ signal }`, wait for cleanup, and preserve the primary processing error. A decisive predicate result closes upstream without reading the remainder. Empty inputs produce `undefined` for `find`, `false` for `some`, and `true` for `every`. A successful match whose value is `undefined` also returns `undefined` from `find`.

`scan(reducer, initial)` is an operator that emits the updated accumulator after each input, whereas `reduce` returns only the final accumulator. It awaits the reducer and supplies `{ index, signal }`. The initial value itself is not emitted; an empty input emits nothing. Accumulation and callback indexes restart for each execution. As with `reduce`, the initial accumulator is used by reference; use immutable updates or `defer` when each execution needs a fresh mutable accumulator.

```ts
await from(jobs).forEach(async (job, { signal }) => save(job, signal));
const totals = from([1, 2, 3]).pipe(scan((sum, value) => sum + value, 0));
// totals emits 1, 3, 6
const first = await from([1, 2, 3]).find((value) => value > 1); // 2
```

## Timeline sources

`fromTimeline(events)` creates a replayable, demand-driven source from a readonly array of `TimelineEvent<T>` values:

```ts
const source = fromTimeline([
  { at: 0, kind: "value", value: "A" },
  { at: 100, kind: "value", value: "B" },
  { at: 150, kind: "complete" },
]);
// An error terminator is { at: 150, kind: "error", error }.
```

Times are finite, non-negative milliseconds from the first read of this source stage in each execution, in non-decreasing order. Equal-time events retain array order. A completion or error event may appear only as the final event. Without a terminator, the source completes after the last value; an empty timeline completes immediately. Invalid schedules throw at construction.

Construction snapshots the event records, retaining the identity of values and errors, including Promise values. No background queue or timers run while downstream is idle. An overdue event is delivered on the next request. Waits use the execution scheduler and are abortable. This models a pull source; it does not simulate loss or buffering of external push events.

The internal `createTestClock()` utility supports `advanceBy(ms)`, `advanceTo(timeMs)`, and `pendingTimers`. It plugs into `iterateWithScheduler` so timelines and time operators share one clock. Advances process chained timers chronologically, preserve registration order for equal deadlines, and drain promise continuations between deadlines using a host turn. Time cannot move backwards, and advances cannot overlap. These testing seams are internal and are not exported from the package entry point.

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

Run `pnpm bench --json=/tmp/mogami-bench.json` for workload benchmarks against native JavaScript, or `pnpm bench:compare --json=/tmp/mogami-compare.json` for comparisons with applicable third-party APIs. The suite separates ordered batch results from incremental consumption and covers JSON record processing, variable-latency simulated I/O, a slow sink, early termination, and in-flight cancellation. Every run verifies exact output order and values, concurrency limits, and source cleanup. Reports include time to first item, total time, observed read-ahead, raw samples, runtime, and installed package versions. See [benchmark methodology and options](benchmarks/README.md) for conditions and limitations.

The browser runner uses installed Google Chrome on macOS when available; otherwise it uses Playwright Chromium. Set `PLAYWRIGHT_EXECUTABLE_PATH` to select another compatible executable. The Workers test uses local Miniflare/workerd, requires no account, and never deploys. Only the `workerd` dependency is allowed an install-time build script.

Node.js, Bun, browser main threads, Dedicated Workers, Service Workers, and Cloudflare Workers are targets. Deno, CommonJS, stream/event adapters, Worker creation, and the `switch`, `debounce`, `throttle`, `sample`, and `interval` families are outside the initial scope.

ESM JavaScript and declarations are emitted to `dist`. The runtime imports only its own distribution modules. No timers or background work start during import.
