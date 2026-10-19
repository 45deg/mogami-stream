# WebSocket and SSE

Use `fromCallback` and `fromEvent` to receive events as streams. These examples keep at most 64 queued values and drop the oldest when the queue is full.

<<< @/examples/events.ts

The WebSocket adapter handles messages, errors, and closure, then removes its listeners during cleanup. The SSE adapter receives messages until the supplied signal aborts. The caller owns and closes each connection.

Use `overflow: "error"` to fail on overflow, or `"drop-newest"` to discard incoming values instead.

The optional synchronous `onDrop(info)` hook reports intentional overflow loss. `info.count` is a live, cumulative count for that execution; `info.overflow` identifies the drop policy and `info.signal` identifies the execution. Keeping `info` retains no dropped values. Reentrant drops update the count without recursive notifications. A synchronous hook throw fails the execution; an earlier cancellation or failure keeps its original reason. Return values, including promises, are ignored.

[fromCallback](/mogami-stream/api/functions/fromCallback.html){target="_self"} · [fromEvent](/mogami-stream/api/functions/fromEvent.html){target="_self"}
