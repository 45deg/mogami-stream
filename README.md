# mogami-stream

[![npm version](https://img.shields.io/npm/v/mogami-stream)](https://www.npmjs.com/package/mogami-stream)
[![CI](https://github.com/45deg/mogami-stream/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/45deg/mogami-stream/actions/workflows/ci.yml)
[![Documentation](https://img.shields.io/badge/docs-English%20%7C%20日本語-blue)](https://45deg.github.io/mogami-stream/)
[![MIT license](https://img.shields.io/npm/l/mogami-stream)](https://github.com/45deg/mogami-stream/blob/main/LICENSE)

TypeScript operators for safe asynchronous stream processing

- **Bounded work and buffering.** `map` limits concurrent work with `concurrency` and caps the total of pending reads, running callbacks, and buffered results with `capacity`. Results are emitted in input order or completion order.
- **Bounded branching.** `broadcast` shares every item with all active branches; `route` selects one destination per item, with optional type guards, a default destination, and an explicit fallback policy.
- **Demand-driven execution.** Pipelines start on the first read. Concurrent stages start new upstream reads only while downstream is waiting for a value; work already in flight may finish within the stage's bounds.
- **Separate cancellation and cleanup completion.** `cancel()` rejects a pending `next()` immediately and passes the stop request to upstream stages through `AbortSignal`. Await `closed` for in-flight work and cleanup to finish.
- **Awaited cleanup with error reports.** Async `finalize` callbacks run after upstream stages and in-flight work finish. Terminal consumers wait for cleanup before settling. `closed` reports completion, cancellation, or failure, preserving the primary reason and collecting additional errors in `suppressedErrors`.
- **Standard iterable interop.** Wrap synchronous iterables and async generators, chain typed methods such as `.map()` and `.filter()`, and consume the result with `for await...of` or terminal methods such as `.toArray()`.

```ts
import { from } from "mogami-stream";

const urls = ["https://example.com/a", "https://example.com/b"];
const controller = new AbortController();

const texts = from(urls)
  .rateLimit({ count: 5, perMs: 1_000 })
  .map(
    async (url, { signal }) => {
      const response = await fetch(url, { signal });
      if (!response.ok) {
        await response.body?.cancel();
        throw new Error(`HTTP ${response.status}`);
      }
      return response.text();
    },
    { concurrency: 5, capacity: 5, ordered: false },
  )
  .take(20);

const results = await texts.toArray({ signal: controller.signal });
```

In the example above, `rateLimit` paces how often upstream `next()` reads start within a single pipeline execution. It does not coordinate requests across separate executions or count retry attempts inside `map`.

## Installation

```sh
npm install mogami-stream
```

With pnpm:

```sh
pnpm add mogami-stream
```

With Bun:

```sh
bun add mogami-stream
```

Requires Node.js 22 or later when running on Node.js. The package exports ESM JavaScript and TypeScript declarations.

## Guides

[English](https://45deg.github.io/mogami-stream/) · [日本語](https://45deg.github.io/mogami-stream/ja/) · [API reference](https://45deg.github.io/mogami-stream/api/)

- [Getting started](https://45deg.github.io/mogami-stream/guide/getting-started.html)
- [Choosing operators](https://45deg.github.io/mogami-stream/guide/operators.html)
- [Concurrency and demand](https://45deg.github.io/mogami-stream/guide/concurrency.html)
- [Cancellation and cleanup](https://45deg.github.io/mogami-stream/guide/lifecycle.html)
- [Error handling and retries](https://45deg.github.io/mogami-stream/guide/errors.html)

Each consumption starts a separate execution; `from` preserves the input's replayability and `defer` creates a fresh source. `toArray` retains all output in memory. Pass callback signals to I/O: terminal consumers wait for cleanup, so work that ignores cancellation can delay settlement. See the [event-loop recipe](https://45deg.github.io/mogami-stream/recipes/event-loop.html) for long synchronous pipelines.

Use `mogami-stream/core` for standalone operators without fluent operator methods, allowing bundlers to remove unused operators. See [composition and entry points](https://45deg.github.io/mogami-stream/guide/building-pipelines.html#entry-points).

## Benchmarks

AsyncIterable inputs and Promise-returning transforms, without I/O waits. Top to bottom: serial map, ordered map (concurrency 8, capacity 16), and a 13-stage map/filter pipeline.

![Node.js and Bun elapsed times for sequential async map, bounded async map, and a 13-stage async pipeline.](benchmarks/results/readme/comparison.svg)

100,000 inputs · Node.js v26.13.0 / Bun v1.4.3 · Apple M1 Pro · Normal GC. Median of 7 runs after 5 warmups. Seconds; lower is faster. Each panel has its own scale. [Methodology and reproduction](benchmarks/README.md).

## Development

See [CONTRIBUTING.md](https://github.com/45deg/mogami-stream/blob/main/CONTRIBUTING.md) for source builds, documentation editing, and validation commands.

## Related Projects

- [aiostream](https://github.com/vxgmichel/aiostream) - Composable operators for Python async iterables; inspired mogami-stream's operator API.
- [IxJS](https://github.com/ReactiveX/IxJS) - Operators for JavaScript synchronous and asynchronous iterables (included in this repository's benchmarks).
- [iter-tools](https://github.com/iter-tools/iter-tools) - Utilities for synchronous and asynchronous iterables (included in this repository's benchmarks).
- [p-map](https://github.com/sindresorhus/p-map) - Concurrent Promise mapping, including streaming with `pMapIterable` (included in this repository's benchmarks).
- [RxJS](https://github.com/ReactiveX/rxjs) - Reactive event stream operators based on Observables.
