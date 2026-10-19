# WebSocket と SSE

`fromCallback` と `fromEvent` でイベントをストリームとして受信します。キューの上限は64件で、満杯になったら最も古い値を捨てます。

<<< @/examples/events.ts

WebSocket は `message`・`error`・`close` を接続し、終了時にリスナーを解除します。SSE は `message` を受信し、渡した `signal` で停止します。接続自体を閉じるのは呼び出し側です。

値を捨てたくない場合は `overflow: "error"`、新しい値を捨てる場合は `"drop-newest"` を指定します。

同期フック `onDrop(info)` を指定すると、overflow による破棄を観測できます。`info.count` はその実行の累積件数で、同じ `info` から常に最新の値を読めます。`info.overflow` は破棄方式、`info.signal` は対象の実行を示します。`info` を保持しても破棄された値は残りません。フックから再入して破棄が起きた場合は、件数だけを更新し、通知は再帰的に呼びません。フックが同期的に例外を投げると実行は失敗しますが、先にキャンセルや失敗が起きていれば、その理由を維持します。Promise を含め、フックの返り値は扱いません。

[fromCallback](/mogami-stream/api/functions/fromCallback.html){target="_self"} · [fromEvent](/mogami-stream/api/functions/fromEvent.html){target="_self"}
