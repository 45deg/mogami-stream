# WebSocket and SSE

Use `fromCallback` and `fromEvent` to receive events as streams. These examples keep at most 64 queued values and drop the oldest when the queue is full.

<<< @/examples/events.ts

The WebSocket adapter handles messages, errors, and closure, then removes its listeners during cleanup. The SSE adapter receives messages until the supplied signal aborts. The caller owns and closes each connection.

Use `overflow: "error"` to fail on overflow, or `"drop-newest"` to discard incoming values instead.

[fromCallback](/mogami-stream/api/functions/fromCallback.html){target="_self"} · [fromEvent](/mogami-stream/api/functions/fromEvent.html){target="_self"}
