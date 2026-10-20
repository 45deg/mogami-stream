# Timeline testing

Use [`fromTimeline`](../operators.md#operator-fromtimeline) to script value and error arrivals and assert the results. Times are milliseconds from the first read.

<<< @/examples/testing.ts#sources

These checks use Node.js `assert`:

<<< @/examples/testing.ts#assertions

This is a pull-driven source with real waits. No timer runs while downstream is idle, and completion or errors wait for the next read too. Events requested after their scheduled time are processed immediately.

Use finite, non-negative timestamps in non-decreasing order. Equal times follow array order; completion or error may appear only last. Without a terminal event, the source completes after its final value. Invalid schedules throw during construction. See [fromTimeline](/mogami-stream/api/functions/fromTimeline.html){target="_self"} for the full contract.
