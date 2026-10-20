# Concurrency, capacity, and demand

[`map`](../operators.md#operator-map) and [`mapSettled`](../operators.md#operator-mapsettled) count work toward their bounds as follows. Defaults are `{ concurrency: 1, capacity: concurrency, ordered: true }`.

| Option        | What counts toward the bound                                                       |
| ------------- | ---------------------------------------------------------------------------------- |
| `concurrency` | Reserved upstream reads + active callbacks (including retry decisions and backoff) |
| `capacity`    | The above + results awaiting delivery                                              |

Both must be positive safe integers with `capacity >= concurrency`. At most one upstream read is in flight. Increasing `capacity` lets later work proceed while completed results wait behind an earlier item.

Bounds apply independently to each stage. External source buffers and arrays collected by [`toArray`](../operators.md#operator-toarray) are excluded.

## Interactive slot walkthrough

An example with `concurrency: 2`, `capacity: 3`, and `ordered: true`:

<SlotDiagram />

## Output order

For input order `A → B → C` and completion order `B → C → A`:

| Option                    | Output order | Results awaiting delivery                                        |
| ------------------------- | ------------ | ---------------------------------------------------------------- |
| `ordered: true` (default) | `A → B → C`  | `B` and `C` occupy capacity until `A` is delivered.              |
| `ordered: false`          | `B → C → A`  | Downstream requests can deliver ready results and free capacity. |

To preserve the completion order of side effects such as writes, use `concurrency: 1` or sequential [`forEach`](../operators.md#operator-foreach).

Regardless of output ordering, [`map`](../operators.md#operator-map) stops immediately on an unhandled callback error and aborts the other callbacks' signals. Use [`mapSettled`](./errors.md#item-errors) to receive failures as results and continue.

## Downstream read requests

New upstream reads start only while a downstream `next()` is pending. Once a value is delivered, new reads stop until the next request. Work already started, including retries, can continue and retain results within its reserved capacity.

A downstream [`take(n)`](../operators.md#operator-take) limits output count; additional work may already have started. Put [`take(n)`](../operators.md#operator-take) before [`map`](../operators.md#operator-map) to limit the number of input items processed.

## Higher-order stream bounds

For [`flatten`](../operators.md#operator-flatten), [`concat`](../operators.md#operator-concat), [`flatMap`](../operators.md#operator-flatmap), and [`concatMap`](../operators.md#operator-concatmap), `concurrency` bounds active child sources and defaults to `1`.

- Each child occupies a slot from reservation until cleanup (`return()` and finalizers) finishes.
- Each child holds at most one pending read or buffered item.
- [`concat`](../operators.md#operator-concat) and [`concatMap`](../operators.md#operator-concatmap) retain slots for later children that have finished reading until earlier children end. This prevents unbounded accumulation of children waiting for their turn.

See [Choosing operators](./operators.md) for selection guidance.

## Synchronous work and the event loop

Continuous synchronous work can prevent timers and I/O from running. Pipelines do not yield automatically; use the [event-loop yielding recipe](../recipes/event-loop.md) when needed.

[MapOptions](/mogami-stream/api/interfaces/MapOptions.html){target="_self"} · [MapContext](/mogami-stream/api/interfaces/MapContext.html){target="_self"} · [Retries](./errors.md#retry)
