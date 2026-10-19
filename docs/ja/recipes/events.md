# WebSocket と SSE

`fromCallback` と `fromEvent` でイベントをストリームとして受信します。キューの上限は64件で、満杯になったら最も古い値を捨てます。

<<< @/examples/events.ts

WebSocket は `message`・`error`・`close` を接続し、終了時にリスナーを解除します。SSE は `message` を受信し、渡した `signal` で停止します。接続自体を閉じるのは呼び出し側です。

値を捨てたくない場合は `overflow: "error"`、新しい値を捨てる場合は `"drop-newest"` を指定します。

## 破棄された値の件数を調べる {#observing-dropped-values}

同期コールバック `onDrop(info)` を指定すると、`fromCallback` や `fromEvent` でキューの上限超過により破棄された値の件数を記録できます。実行ごとに、最初の破棄が発生した時点で凍結（`Object.freeze`）された `PushDropInfo` オブジェクトが1つ生成されます：

- `count` は意図的に破棄された値の累積件数で、`Number.MAX_SAFE_INTEGER` で飽和します。通知時点のスナップショットではなくライブカウンタであるため、`info` を保持しておけば実行終了後も最新の件数を参照できます。
- `overflow` は破棄ポリシー（バッファ内の古い値1件を追い出す `"drop-oldest"`、または新しい値1件を拒否する `"drop-newest"`）を示します。既定の `"error"` による容量超過エラーや、失敗・キャンセル時のバッファ破棄、正常完了、終了後の呼び出しはカウントおよび通知の対象外です。
- `signal` はその実行の `AbortSignal` であり、並行する複数の購読を識別できます。カウントは実行ごとに独立して集計されます。

### フック内からの再入呼び出し

`onDrop` はキューの更新が確定した後に呼ばれます。`onDrop` の中から再入して値を追加し、さらに破棄が発生した場合も、同じ `info.count` が即座に更新されます（このとき `onDrop` の再帰呼び出しや通知のキューイングは行われないため、追加後の合計は `info.count` を読んで確認してください）。再入ではない次の破棄からは通常どおり通知されます。破棄された値そのものや履歴は保持されず、`onDrop` を省略した場合はカウント処理も行われません。

### 例外とキャンセル

`onDrop` が同期的に例外を投げると、実行は失敗状態に移行し、バッファを空にしてクリーンアップを行います（例外が `observer.next()` の呼び出し元へ直接抜けることはありません）。すでにキャンセルまたは失敗していた場合は最初の理由が主因として維持され、新しい例外は `closed.suppressedErrors` に記録されます。`onDrop` は同期専用であり、TypeScript の型定義では戻り値 `undefined` が要求され、実行時に Promise などの値を返しても無視されます（待機されません）。また、外側の `observer.next()` は、`onDrop` 内で値の置き換えや完了・失敗・キャンセルが行われた場合でも、呼び出し時点で決まった受理結果（`true` / `false`）をそのまま返します。終了後の後続の呼び出しはすべて `false` を返します。

[fromCallback](/mogami-stream/api/functions/fromCallback.html){target="_self"} · [fromEvent](/mogami-stream/api/functions/fromEvent.html){target="_self"}
