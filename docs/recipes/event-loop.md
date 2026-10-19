# Yielding to the event loop

For a long synchronous pipeline, use an awaited `tap` callback to let timers and timer-driven cancellation run. The helpers below are application code, built from existing operators. Mogami does not add yields by default.

Import `defer`, `from`, and `tap` from `mogami-stream` alongside these helpers.

## A cancellable host turn

`await Promise.resolve()` yields only to microtasks. A zero-delay timer gives the host a turn to run timers and I/O. This helper cancels the timer and removes its abort listener when the wait ends.

<<< @/examples/event-loop.mjs#wait{js}

## Yield every N items

<<< @/examples/event-loop.mjs#count{js}

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

`tap` awaits the callback sequentially and passes values through unchanged, including Promise-valued items. Its index starts at zero for each execution, so the same operator can be reused. The Nth item is read before the wait and delivered after it. There is no background producer or extra queue, and no timer is scheduled while downstream is idle. Existing upstream concurrency and capacity limits still apply.

Put the recipe before stages that may consume many inputs without emitting, such as a filter that rejects nearly everything. Adjust the count to the cost of your callbacks: 1,024 cheap items and 1,024 expensive items occupy very different amounts of time.

## Yield after a time budget

For hosts with an advancing monotonic clock, the following alternative checks elapsed time per item. `defer` gives each execution its own timestamp.

<<< @/examples/event-loop.mjs#time{js}

Use `yieldAfter(4)` in place of `yieldEvery(1_024)`. The budget includes upstream work and downstream idle time since the previous yield. It is checked only when an item reaches this stage; one expensive callback can exceed it. Cloudflare Workers may advance `performance.now()` only after I/O, so use the count recipe there: elapsed-time checks alone may never trigger a yield.

## Choosing an interval

Yielding every item can be costly because the host may clamp zero-delay timers. In an exploratory Node.js measurement with 10,000 synchronous mapped inputs, 1,024-item intervals reduced first-timer delay from 16.94 ms to 1.88 ms while increasing total time from 16.91 ms to 29.56 ms. A dedicated operator prototype did not improve those results. See the [measurement method and raw samples](https://github.com/45deg/mogami-stream/blob/main/benchmarks/event-loop.md).

These intervals are tuning choices, not deadlines or guarantees about frame rate. A yield cannot interrupt a synchronous callback already running; split expensive work into smaller units. If the source or callbacks already await I/O, measure before adding artificial waits.
