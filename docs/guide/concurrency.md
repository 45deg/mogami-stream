# Concurrency, capacity, and demand

[`map`](/mogami-stream/api/functions/map.html){target="_self"} and [`mapSettled`](/mogami-stream/api/functions/mapSettled.html){target="_self"} accept `{ concurrency = 1, capacity = concurrency, ordered = true }`. Both `concurrency` and `capacity` must be positive safe integers satisfying `capacity >= concurrency`.

| Parameter     | What counts toward the bound                                                                       |
| ------------- | -------------------------------------------------------------------------------------------------- |
| `concurrency` | Reserved upstream reads + active mapper callbacks (including per-item `shouldRetry` and backoff)   |
| `capacity`    | Reserved upstream reads + active mapper callbacks + completed outputs waiting for downstream reads |

At most one upstream `next()` read is in flight at any time. Setting `capacity > concurrency` allows fast items that finish out of order to buffer while new mapper calls start on later inputs, without exceeding `concurrency` simultaneous mapper executions.

Each mapper invocation receives a [`MapContext`](/mogami-stream/api/interfaces/MapContext.html){target="_self"} `{ index, signal, attempt }`, where `index` is zero-based per execution and `attempt` starts at `1`.

## Interactive slot walkthrough

Step through a single `map` stage configured with `concurrency: 2`, `capacity: 3`, and `ordered: true`:

<SlotDiagram />

## Output ordering: `ordered: true` vs. `ordered: false`

Suppose inputs `A`, `B`, and `C` start in order, and `B` and `C` complete while `A` is still running:

| Mode                      | Delivery order | Buffering behavior                                                                                 |
| ------------------------- | -------------- | -------------------------------------------------------------------------------------------------- |
| `ordered: true` (default) | `A → B → C`    | `B` and `C` occupy capacity slots until `A` finishes and is delivered first.                       |
| `ordered: false`          | `B → C → A`    | `B` and `C` are delivered as soon as downstream requests them, freeing their capacity slots early. |

In `map`, an unhandled error from any active mapper stops the stage immediately and aborts sibling mappers via their `signal`—even under `ordered: true` when an earlier item is still in flight. Use `mapSettled` when individual mapper failures should be emitted as `{ status: "rejected", reason }` objects rather than failing the stream.

## Demand-driven backpressure

Concurrent stages pull from upstream only while a downstream `next()` request is pending:

1. When downstream calls `next()`, the stage starts upstream reads and mapper calls up to `concurrency` and `capacity`.
2. As soon as one result is delivered to satisfy that `next()` call, the stage stops starting new upstream reads.
3. Reads and mapper calls that were already in flight (including retry loops for those items) may finish and store their outputs in their reserved `capacity` slots, but no additional inputs are pulled until the next downstream `next()`.

Each stage enforces its bounds independently. A pipeline with two concurrent `map` stages maintains two separate slot pools. Stage bounds do not govern memory held inside an external source or results accumulated by terminal collectors like `toArray`.

## Higher-order stream bounds

Higher-order operators ([`flatten`](/mogami-stream/api/functions/flatten.html){target="_self"}, [`concat`](/mogami-stream/api/functions/concat.html){target="_self"}, [`flatMap`](/mogami-stream/api/functions/flatMap.html){target="_self"}, and [`concatMap`](/mogami-stream/api/functions/concatMap.html){target="_self"}) accept `{ concurrency = 1 }` to control how many child sources may be active at once:

- A child source occupies its concurrency slot from reservation until its teardown (`return()` and finalizers) completes.
- Each active child holds at most one pending read or buffered item.
- In ordered variants (`concat` and `concatMap`), a child that completes early retains its concurrency slot until all earlier children finish. This prevents unbounded child buffering behind a slow leading child stream.

## Event-loop scheduling

Pipelines do not insert artificial `setTimeout` ticks or macrotask yields between items. If a synchronous source and synchronous callbacks run continuously without awaiting I/O, they can monopolize the event loop. Insert explicit yields in the source, operator callback, or consumer when processing large CPU-bound synchronous sequences alongside timers or network I/O.

See [MapOptions](/mogami-stream/api/interfaces/MapOptions.html){target="_self"} for the full option type and [Retries](./retry.md) for retry configuration.
