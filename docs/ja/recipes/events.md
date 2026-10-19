# WebSocket と SSE

`fromCallback` と `fromEvent` でイベントをストリームとして受信します。キューの上限は64件で、満杯になったら最も古い値を捨てます。

<<< @/examples/events.ts

WebSocket は `message`・`error`・`close` を接続し、終了時にリスナーを解除します。SSE は `message` を受信し、渡した `signal` で停止します。接続自体を閉じるのは呼び出し側です。

値を捨てたくない場合は `overflow: "error"`、新しい値を捨てる場合は `"drop-newest"` を指定します。

[fromCallback](/mogami-stream/api/functions/fromCallback.html){target="_self"} · [fromEvent](/mogami-stream/api/functions/fromEvent.html){target="_self"}
