# Choosing operators

Use the [catalog](../operators.md) for individual examples and API links. Choose how values should flow before setting concurrency or timing limits.

| Goal                           | Operators                                                                                                                                 | Behavior                                                                                                                                     |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| Transform or select items      | [`map`](../operators.md#operator-map), [`filter`](../operators.md#operator-filter), [`filterMap`](../operators.md#operator-filtermap)     | [`map`](../operators.md#operator-map) supports concurrency; the others run sequentially.                                                     |
| Limit output or group items    | [`take`](../operators.md#operator-take), [`chunks`](../operators.md#operator-chunks), [`bufferTime`](../operators.md#operator-buffertime) | Count at the stage's position; see [processing order](./building-pipelines.md).                                                              |
| Join known sources in sequence | [`chain(a, b)`](../operators.md#operator-chain)                                                                                           | Closes each source before opening the next.                                                                                                  |
| Interleave known sources       | [`merge(a, b)`](../operators.md#operator-merge)                                                                                           | Reads sources concurrently.                                                                                                                  |
| Flatten a stream of sources    | [`concat`](../operators.md#operator-concat), [`flatten`](../operators.md#operator-flatten)                                                | [`concat`](../operators.md#operator-concat) preserves child order; [`flatten`](../operators.md#operator-flatten) interleaves ready children. |
| Create a source per item       | [`concatMap`](../operators.md#operator-concatmap), [`flatMap`](../operators.md#operator-flatmap)                                          | Ordered or interleaved flattening after a synchronous mapper. Use [`defer`](../operators.md#operator-defer) for async setup.                 |
| Pair corresponding values      | [`zip(a, b)`](../operators.md#operator-zip)                                                                                               | Stops when the shortest source ends.                                                                                                         |
| Combine current values         | [`zipLatest([a, b])`](../operators.md#operator-ziplatest)                                                                                 | Waits for every source's first value; `partial: true` fills missing values with `undefined`.                                                 |

[`concat`](../operators.md#operator-concat), [`flatten`](../operators.md#operator-flatten), [`concatMap`](../operators.md#operator-concatmap), and [`flatMap`](../operators.md#operator-flatmap) default to `concurrency: 1`. Increase it to overlap children; ordered variants still deliver each child's output in sequence. See [concurrency bounds](./concurrency.md#higher-order-stream-bounds).

For pacing and deadlines, use the [timing guide](./timing.md). For retries and fallback sources, use [error handling](./errors.md). Choose a [terminal consumer](./consumers.md) to start reading.

[`broadcast`](./branching.md#broadcast) sends the same values to every active branch; [`route`](./branching.md#route) sends each item to at most one branch. Both run the source once and bound each branch queue.

## Accumulated state

[`scan`](../operators.md#operator-scan) emits each updated accumulator; [`reduce`](../operators.md#operator-reduce) returns only the final one. Both await the initial value and reducer results. An empty input emits nothing from [`scan`](../operators.md#operator-scan), while [`reduce`](../operators.md#operator-reduce) returns the initial value.

Objects are passed by reference, not copied. Use immutable updates for snapshots. If you mutate state, create it inside [`defer`](../operators.md#operator-defer) for each execution and capture each result before the next mutation:

<<< @/examples/value-semantics.ts#state

## Promise-valued items

[`from`](../operators.md#operator-from) and selection/combination operators preserve Promise objects as items. Unlike native `for await` over a synchronous iterable, they do not unwrap those items. Use [`map`](../operators.md#operator-map) to await them:

<<< @/examples/value-semantics.ts#promises

[`find`](../operators.md#operator-find) receives the original item in its predicate, but awaits a matched Promise when returning it.

## Select ranges and moving windows

`slice(start, end)` selects a range with an inclusive start and exclusive end. Omitting `start` uses zero; omitting `end` reads to completion. Bounds must be safe integers: fractions, `NaN`, and infinities throw `RangeError`. With non-negative bounds, `slice(2, 5)` skips two items, emits three, and closes upstream before delivering the final selected item. Ranges known to be empty, such as `slice(5, 2)`, do not open upstream.

Negative bounds count from the end. `slice(2, -3)` emits after a three-item delay while retaining only those three trailing items. `slice(-5)` and `slice(-5, -2)` wait for input completion and retain at most five items. A negative start therefore requires finite input.

`slidingWindow(size)` emits overlapping arrays, advancing one item at a time. It waits for a full window and omits incomplete windows; `size` must be a positive safe integer. Unlike `chunks`, adjacent windows share items. Each output array is independent, although its elements retain their original identities. Internal storage is bounded by `size`; producing each snapshot takes O(size) work.

```ts
const averages = await from([1, 2, 3, 4, 5])
  .slidingWindow(3)
  .map(
    (window) => window.reduce((sum, value) => sum + value, 0) / window.length,
  )
  .toArray(); // [2, 3, 4]
```

Use [`at`](./consumers.md) when only one position is needed, or finish a selected range with `toArray()`, `count()`, or `drain()`.
