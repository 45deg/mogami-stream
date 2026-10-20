# Terminal consumers

A pipeline built with [`.pipe()`](../operators.md#operator-pipe) starts executing when a consumer begins reading it. Choose a consumer to collect output, compute one result, or process items individually.

## Choose a consumer

| Goal                                        | API                                                                    | Result                          |
| ------------------------------------------- | ---------------------------------------------------------------------- | ------------------------------- |
| Collect every item into an array            | [`toArray`](/mogami-stream/api/functions/toArray.html){target="_self"} | An array of all items           |
| Compute a sum or another aggregate          | [`reduce`](/mogami-stream/api/functions/reduce.html){target="_self"}   | The final accumulator           |
| Process each item without collecting output | [`forEach`](/mogami-stream/api/functions/forEach.html){target="_self"} | `Promise<void>`                 |
| Find the first matching value               | [`find`](/mogami-stream/api/functions/find.html){target="_self"}       | The first match, or `undefined` |
| Check whether any item matches              | [`some`](/mogami-stream/api/functions/some.html){target="_self"}       | `true` if any item matches      |
| Check whether every item matches            | [`every`](/mogami-stream/api/functions/every.html){target="_self"}     | `true` if all items match       |

These consumers return promises. Each is available as a method, such as [`stream.toArray({ signal })`](../operators.md#operator-toarray), and as a standalone function, such as [`toArray(source, { signal })`](../operators.md#operator-toarray). Standalone functions also accept ordinary synchronous and asynchronous iterables.

## Collect or aggregate

<<< @/examples/consumers.ts#collect

[`toArray`](../operators.md#operator-toarray) retains every output in memory, so use it for finite inputs. [`reduce`](../operators.md#operator-reduce) folds items sequentially from an initial value and returns that value for an empty input. The example uses an array as its source, so the same stream can be read again.

## Process items individually

<<< @/examples/consumers.ts#each

[`forEach`](../operators.md#operator-foreach) waits for each callback before reading the next value and does not build an output array. Both [`forEach`](../operators.md#operator-foreach) and [`reduce`](../operators.md#operator-reduce) run callbacks sequentially. To process items concurrently, use an upstream [`map(..., { concurrency })`](../operators.md#operator-map) stage.

## Stop when the answer is known

<<< @/examples/consumers.ts#match

[`find`](../operators.md#operator-find) and [`some`](../operators.md#operator-some) close upstream on the first match; [`every`](../operators.md#operator-every) closes it on the first non-match. For empty inputs, [`find`](../operators.md#operator-find) returns `undefined`, [`some`](../operators.md#operator-some) returns `false`, and [`every`](../operators.md#operator-every) returns `true`.

## Cancellation and cleanup

Pass `{ signal }` to cancel the execution. Forward the callback's `signal` into I/O to propagate the stop request to in-flight work.

Consumers settle after cleanup finishes. If processing and cleanup both fail, the primary processing error takes precedence. I/O that does not support cancellation must still finish before the consumer settles.

For manual consumption, use `for await...of` or [`iterate`](/mogami-stream/api/functions/iterate.html){target="_self"}. Acquiring an iterator does not start execution; its first `next()` does. See [Cancellation and cleanup](./lifecycle.md) for `cancel()`, `return()`, and `closed`.

For secondary errors, use the [collection report recipe](../recipes/terminal-report.md). It also explains how to observe a timeout before cleanup completes.
