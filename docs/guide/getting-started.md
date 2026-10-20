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

[`from`](../operators.md#operator-from) and transformation methods such as [`.map()`](../operators.md#operator-map) only describe the work. Calling [`.toArray()`](../operators.md#operator-toarray) starts reading and returns a Promise for `[2, 4, 6]` after processing and cleanup finish.

| Term              | In this example                                              | Role                                                          |
| ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------- |
| Source            | [`from([1, 2, 3])`](../operators.md#operator-from)           | Supplies values as a `Stream<number>`.                        |
| Transformation    | [`.map((value) => value * 2)`](../operators.md#operator-map) | Returns a new stream that doubles each input value when read. |
| Terminal consumer | [`.toArray()`](../operators.md#operator-toarray)             | Reads the stream and collects its values.                     |

[`Stream<T>`](/mogami-stream/api/interfaces/Stream.html){target="_self"} implements JavaScript's standard `AsyncIterable<T>` protocol: a consumer asks for the next value with `next()` and awaits its arrival. You can also consume a stream with `for await...of`.

Values flow from the source, through operators from left to right, to the consumer. **Upstream** means toward the source; **downstream** means toward the consumer. Here, [`from(...)`](../operators.md#operator-from) is upstream of [`map`](../operators.md#operator-map), and [`.toArray()`](../operators.md#operator-toarray) is downstream of it.

See [When to use mogami-stream](../index.md#when-to-use) for examples of tasks that benefit from its concurrency and lifecycle controls.

## 3. Asynchronous work and stopping

Make the callback asynchronous and set `concurrency` to bound simultaneous work.

<<< @/examples/quick-start.ts

This fetches up to five URLs concurrently and emits results in completion order. [`rateLimit`](../operators.md#operator-ratelimit) limits this execution's upstream reads to five per second. [`take(20)`](../operators.md#operator-take) limits output count; place it before [`map`](../operators.md#operator-map) if you also need to limit started requests to 20.

Call `controller.abort()` to stop. Passing the callback's `signal` to `fetch` propagates cancellation to in-flight requests. [`toArray`](../operators.md#operator-toarray) settles after cleanup finishes.

Arrays can be read again, but an instantiated generator is exhausted after one consumption. Use [`defer`](../operators.md#operator-defer) when each execution needs a fresh source.

Continue with [Building a pipeline](./building-pipelines.md). See [Choosing operators](./operators.md) for selection guidance and [Cancellation and cleanup](./lifecycle.md) for releasing resources.
