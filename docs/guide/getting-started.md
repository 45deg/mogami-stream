# Getting started

## 1. Installation

Install `mogami-stream` with your preferred package manager:

::: code-group

```sh [npm]
npm install mogami-stream
```

```sh [pnpm]
pnpm add mogami-stream
```

```sh [Bun]
bun add mogami-stream
```

:::

`mogami-stream` ships pure ESM JavaScript and TypeScript declarations (`sideEffects: false`) with zero runtime dependencies. Supported environments include Node.js 22+, Bun, browser main threads, Dedicated Workers, Service Workers, and Cloudflare Workers.

## 2. Building and consuming a pipeline

Start with a finite array and double each value:

<<< @/examples/basics.ts

`from` and `.pipe()` only describe the work. Calling `.toArray()` starts reading and returns a Promise for `[2, 4, 6]` after processing and cleanup finish.

| Term              | In this example             | Role                                                                   |
| ----------------- | --------------------------- | ---------------------------------------------------------------------- |
| Source            | `from([1, 2, 3])`           | Supplies values as a `Stream<number>`.                                 |
| Operator          | `map((value) => value * 2)` | Returns a function that transforms a source when applied by `.pipe()`. |
| Terminal consumer | `.toArray()`                | Reads the stream and collects its values.                              |

[`Stream<T>`](/mogami-stream/api/interfaces/Stream.html){target="_self"} implements JavaScript's standard `AsyncIterable<T>` protocol: a consumer asks for the next value with `next()` and awaits its arrival. You can also consume a stream with `for await...of`.

Values flow from the source, through operators from left to right, to the consumer. **Upstream** means toward the source; **downstream** means toward the consumer. Here, `from(...)` is upstream of `map`, and `.toArray()` is downstream of it.

See [When to use mogami-stream](../index.md#when-to-use) for examples of tasks that benefit from its concurrency and lifecycle controls.

## 3. Adding asynchronous work

For concurrent API requests, replace the synchronous mapper with an async callback and set the concurrency limit. This example also adds a read-rate limit and early termination:

<<< @/examples/quick-start.ts

1. **`from(urls)`** wraps any synchronous `Iterable` or `AsyncIterable` into a `Stream`. For large or unbounded datasets, pass an async generator or streaming cursor rather than materializing an array up front.
2. **`rateLimit({ count: 5, perMs: 1_000 })`** restricts upstream read starts to at most 5 per 1,000 ms sliding window within this execution.
3. **`map(mapper, { concurrency: 5, capacity: 5, ordered: false })`** runs up to 5 mapper callbacks concurrently, limits total occupied slots (reads + running mappers + buffered results) to 5, and emits results as soon as each call finishes.
4. **`take(20)`** stops the pipeline after 20 emitted values and initiates upstream teardown before delivering the 20th value.
5. **`toArray(texts, { signal })`** pulls items from the pipeline, forwards `controller.signal` to every stage and callback, and waits for upstream cleanup before resolving.

## Lazy evaluation

Stream construction, `.pipe()` composition, and iterator creation (`iterate(stream)`) are strictly lazy. No upstream iterator is opened and no timer or callback runs until the first `next()` read occurs.

- If `take(0)` is in the pipeline or the external `AbortSignal` is already aborted before the first read, the execution finishes immediately without opening the upstream source.
- Each consumption of a `Stream` runs an independent execution with its own stage state, concurrency slots, and indices.

## Fluent methods vs. standalone functions

Every terminal consumer is available both as a method on `Stream<T>` and as a standalone function that accepts any sync `Iterable<T>` or `AsyncIterable<T>`:

| Goal                                         | `Stream<T>` method                    | Standalone function                          |
| -------------------------------------------- | ------------------------------------- | -------------------------------------------- |
| Process items sequentially without buffering | `stream.forEach(effect, { signal })`  | `forEach(source, effect, { signal })`        |
| Collect all items from a finite stream       | `stream.toArray({ signal })`          | `toArray(source, { signal })`                |
| Fold items into a single aggregate           | `stream.reduce(fn, init, { signal })` | `reduce(source, fn, init, { signal })`       |
| Short-circuit on the first match             | `stream.find(predicate, { signal })`  | `find(source, predicate, { signal })`        |
| Short-circuit boolean checks                 | `stream.some(pred)` / `.every(pred)`  | `some(source, pred)` / `every(source, pred)` |
| Manual iteration with `cancel()` & `closed`  | `stream.iterate({ signal })`          | `iterate(source, { signal })`                |
| Standard loop consumption                    | `for await (const x of stream)`       | `for await (const x of from(source))`        |

See [Terminal consumers](./consumers.md) for examples and guidance on choosing one.

Both `stream.pipe(...ops)` and the standalone `pipe(source, ...ops)` infer types across up to 8 operators per call; chain `.pipe()` calls for longer pipelines. Value-independent operators (`take`, `skip`, `chunks`, timing operators, and `finalize`) remain generic when stored in a variable, so `const firstFive = take(5)` can be reused across streams of different element types.

## Replayability: `from` vs. `defer`

`from(source)` preserves the replayability of the underlying input:

- Wrapping an array or `Set` (`from([1, 2, 3])`) produces a stream that can be iterated multiple times.
- Wrapping a generator object (`from(myGenerator())`) produces a single-use stream because the generator is already instantiated and exhausted after one run.

Use [`defer`](/mogami-stream/api/functions/defer.html){target="_self"} to create a fresh source for each execution and receive its `AbortSignal`. For resources opened during asynchronous factory setup, use [`context.acquire(acquire, release)`](./lifecycle.md#owning-resources-during-asynchronous-setup) to register ownership even if cancellation occurs before setup finishes.

### Owning and releasing a resource

The following function accepts an application-provided `openCursor` adapter. Its cursor exposes rows and an asynchronous close operation; adapt these methods to your database client:

<<< @/examples/resources.ts

Each consumption opens a fresh cursor on the first read. The async generator owns that cursor and releases it in `finally` on normal completion, failure, or early termination with `take(10)`. The two runs are sequential, and each `toArray` waits for its cursor to close before settling. An aborted signal also stops the execution, so the second run will not open a cursor if the signal is already aborted.

The library calls the opened iterator's `return()` during early cleanup and awaits it. It cannot infer that an unrelated session, file handle, or socket needs closing. Keep acquisition and release together in a generator's `try/finally`, as above, or use `context.acquire` for resources opened before returning the source. A `finalize` attached to a returned source cannot protect resources allocated during setup if cancellation prevents that source from being opened.

Pass the execution's `signal` through to the cursor's underlying I/O, including row reads where your driver supports it. If a pending read ignores cancellation, cleanup must wait for it to settle before `finally` can run. If `openCursor` fails before returning a cursor, the adapter is responsible for releasing any partially acquired resources.

Next, learn how [concurrency, capacity, and demand](./concurrency.md) govern in-flight work, or how [cancellation and cleanup](./lifecycle.md) manage teardown.
