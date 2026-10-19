# Yielding to the event loop

In a long-running synchronous pipeline, an awaited `tap` callback can yield to the host event loop so timers, I/O, and timer-driven cancellation can run. Mogami does not insert yields by default; the helpers below build opt-in yielding from existing operators.

Import `defer`, `from`, and `tap` from `mogami-stream` alongside these helpers.

## Usage

`yieldEvery` is the helper defined below. This example yields every 1,024 items.

```js
import { filter, range } from "mogami-stream";

const controller = new AbortController();
await range(0, 100_000)
  .pipe(
    yieldEvery(1_024),
    filter((value) => value % 2 === 0),
  )
  .forEach((value) => console.log(value), { signal: controller.signal });
```

## A cancellable host turn

`await Promise.resolve()` yields only to microtasks. A zero-delay timer gives the host a turn to run timers and I/O. This helper cancels the timer and removes its abort listener when the wait ends.

<<< @/examples/event-loop.mjs#wait{js}

## Yield every N items

<<< @/examples/event-loop.mjs#count{js}

`tap` awaits the callback sequentially and passes values through unchanged, including Promise-valued items. Its index starts at zero for each execution, so the same operator can be reused.

The Nth item is read before the wait and delivered after it. There is no background producer or extra queue, and no timer is scheduled while downstream is idle. Existing upstream concurrency and capacity limits still apply.

Place the yield stage before stages that may consume many inputs without emitting, such as a filter that rejects nearly everything. Adjust the count to the cost of your callbacks: 1,024 cheap items and 1,024 expensive items occupy very different amounts of time.

## Yield after a time budget

For hosts with an advancing monotonic clock, the following alternative checks elapsed time per item. `defer` gives each execution its own timestamp.

<<< @/examples/event-loop.mjs#time{js}

Use `yieldAfter(4)` in place of `yieldEvery(1_024)`. The budget includes upstream work and downstream idle time since the previous yield. It is checked only when an item reaches this stage, so a single expensive callback can exceed the budget. Cloudflare Workers advance `performance.now()` only after I/O, so use the count-based helper there.

## Choosing an interval

Hosts may clamp zero-delay timers. More frequent waits give timers more opportunities to run at the cost of total processing time; avoid yielding on every item and tune the interval to the workload.

A yield stage cannot preempt a synchronous callback while it is running—split expensive work into smaller units if needed. If the source or callbacks already await I/O, measure before adding explicit yields.
