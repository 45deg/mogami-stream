# Timeline testing

For unit tests and mocks, [`fromTimeline(events)`](/mogami-stream/api/functions/fromTimeline.html){target="_self"} creates a replayable, pull-driven stream from a scripted array of [`TimelineEvent<T>`](/mogami-stream/api/types/TimelineEvent.html){target="_self"} records to simulate delayed arrivals, timeouts, or upstream failures at specific millisecond offsets.

<<< @/examples/testing.ts

## Schedule rules

| Offset (`at`) | Event (`kind`) | Meaning                                                                              |
| ------------- | -------------- | ------------------------------------------------------------------------------------ |
| `0`           | `"value"`      | Emits `"A"` immediately on the first read (`0 ms` offset from execution start).      |
| `100`         | `"value"`      | Emits `"B"` once at least `100 ms` have elapsed since the execution's first read.    |
| `150`         | `"complete"`   | Closes the stream at `150 ms` (or use `kind: "error", error` to simulate a failure). |

- Every `at` timestamp must be a finite, non-negative millisecond offset relative to the first `next()` read of that execution, listed in non-decreasing order.
- Events with identical `at` timestamps are emitted in array order.
- A `"complete"` or `"error"` event may only appear as the last element. If omitted, the stream completes immediately after its last `"value"` event (and `fromTimeline([])` completes immediately on the first read).
- Invalid schedules throw `RangeError` or `TypeError` eagerly when `fromTimeline` is called. Event records are shallow-copied on construction while preserving value and error references.

[fromTimeline](/mogami-stream/api/functions/fromTimeline.html){target="_self"} · [TimelineEvent](/mogami-stream/api/types/TimelineEvent.html){target="_self"}
