---
title: TypeScript operators for safe asynchronous stream processing
---

# mogami-stream

TypeScript operators for safe asynchronous stream processing

## Usage

### 1. Install

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

### 2. Run a pipeline

<<< @/examples/basics.ts

In this pipeline:

- `from` creates a source from the array.
- `.map(...)` describes how to transform each value; it does not start processing yet.
- `.toArray()` consumes the stream and resolves to `[2, 4, 6]` after cleanup.

Continue with [Getting started](./guide/getting-started.md) for the basic terms and a concurrent API request example.

[Get started](./guide/getting-started.md) · [Operators](./operators.md) · [API reference](/mogami-stream/api/index.html){target="_self"} · [日本語ドキュメント](./ja/index.md)

## When to use mogami-stream {#when-to-use}

- Fetching many URLs with limits on concurrent requests and buffered results.
- Transforming database rows or async generator values as they arrive, without collecting the entire input in memory.
- Processing WebSocket or SSE events with a bounded queue and an explicit overflow policy.
- Retrying individual failed items, or collecting each item's success or failure while continuing to process the rest.
- Stopping once enough results are available or the user cancels, propagating cancellation to I/O and waiting for resource cleanup.

## Design

- **Bounded work and buffering.** `map` and `mapSettled` bound active work with `concurrency` and cap the sum of pending reads, running callbacks, and buffered outputs with `capacity`. Higher-order operators (`flatten`, `concat`, `flatMap`, `concatMap`) similarly bound active child streams and prevent unbounded queues behind slow children.
- **Demand-driven execution.** Pipelines are lazy and do no work until the first `next()` call. Concurrent stages initiate new upstream reads only while a downstream read is pending; once a value is delivered, no further upstream reads start until the next pull.
- **Separate cancellation and teardown.** `iterator.cancel(reason)` rejects a pending `next()` immediately and propagates cancellation upstream via `AbortSignal`, while `iterator.closed` resolves with a `CloseReport` once all in-flight operations and finalizers finish.
- **Awaited cleanup and error preservation.** Asynchronous `finalize` callbacks run from inner stages outward after upstream stages and in-flight work settle. Terminal consumers wait for cleanup before resolving or rejecting, prioritizing the primary processing error and recording secondary errors in `suppressedErrors`.
- **Standard iterable interoperability.** Wrap any synchronous `Iterable` or `AsyncIterable`, chain typed methods such as `.map()` and `.filter()`, and consume streams with native `for await...of` loops or built-in terminal consumers.

## Documentation map

- **[Getting started](./guide/getting-started.md)** — Installation, a minimal pipeline, concurrent requests, and cancellation.
- **[Building a pipeline](./guide/building-pipelines.md)** — Processing order, values and types, count limits, and consumption through one example.
- **[Choosing operators](./guide/operators.md)** — Combination choices, accumulated state, and Promise-valued items.
- **[Operators](./operators.md)** — Descriptions, examples, and output grouped by category.
- **[Terminal consumers](./guide/consumers.md)** — Collecting, aggregating, processing each item, and stopping early.
- **[Concurrency, capacity, and demand](./guide/concurrency.md)** — Interactive slot state machine, `ordered: true` vs. `false`, backpressure, and higher-order stream bounds.
- **[Timing](./guide/timing.md)** — Read rates, output spacing, timeouts, and time-based batching.
- **[Error handling](./guide/errors.md)** — Stopping on errors, per-item results, retries, fallback sources, and cleanup errors.
- **[Cancellation and cleanup](./guide/lifecycle.md)** — Stopping an execution, finalizer ordering, and resource ownership.
- **[Recipes](./recipes/api-requests.md)** — [API requests](./recipes/api-requests.md), [WebSocket & SSE](./recipes/events.md), [Database batches](./recipes/database.md), [Timeline testing](./recipes/testing.md), [Event loop yielding](./recipes/event-loop.md), [Shared request limits](./recipes/shared-rate-limiter.md), and [Results and cleanup errors](./recipes/terminal-report.md).
- **[Benchmarks](./benchmarks.md)** — Node.js and Bun performance comparisons and reproduction commands.
