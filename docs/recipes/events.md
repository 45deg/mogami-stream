# WebSocket and SSE

Use [`fromCallback`](../operators.md#operator-fromcallback) and [`fromEvent`](../operators.md#operator-fromevent) to receive events as streams. These examples keep at most 64 queued values and drop the oldest when the queue is full.

<<< @/examples/events.ts#adapters

Both adapters remove their listeners during cleanup.

Use `overflow: "error"` to fail on overflow, or `"drop-newest"` to discard incoming values instead.

## Consume and close the connection

The connection's owner closes it in `finally`. SSE receives until the supplied signal aborts; the WebSocket adapter also ends on a close event.

<<< @/examples/events.ts#usage
