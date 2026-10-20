# Database batch writes

Use [`chunks(100)`](../operators.md#operator-chunks) to group at most 100 rows, then await each write before requesting the next batch. Use [`bufferTime`](../operators.md#operator-buffertime) to flush by time as well as count.

<<< @/examples/database.ts

Normal completion flushes a final partial batch; failure or cancellation discards an incomplete batch.

[chunks](/mogami-stream/api/functions/chunks.html){target="_self"} · [bufferTime](/mogami-stream/api/functions/bufferTime.html){target="_self"}
