# WebSocket と SSE

[`fromCallback`](../operators.md#operator-fromcallback) と [`fromEvent`](../operators.md#operator-fromevent) でイベントをストリームとして受信します。キューの上限は64件で、満杯になったら最も古い値を捨てます。

<<< @/examples/events.ts#adapters

どちらのアダプタも終了時にリスナーを解除します。

満杯時に失敗させる場合は `overflow: "error"`、新しい値を捨てる場合は `"drop-newest"` を指定します。

## 接続を消費して閉じる

接続の所有者が `finally` で閉じます。SSE は渡された `signal` が abort されるまで受信を続け、WebSocket のアダプタは `close` イベントでも終了します。

<<< @/examples/events.ts#usage
