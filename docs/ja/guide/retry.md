# リトライ

`map` と `mapSettled` の `retry` オプションで、失敗した項目だけを再試行できます。

<<< @/examples/retry.ts

`retries: 3` は初回を含めて最大4回の試行です。`shouldRetry` で再試行するエラーを選び、`delayMs` で待機時間を指定します。上の例では失敗した試行番号に応じて 200・400・800 ms 待ちます。

各試行で項目の `index` は変わらず、`attempt` は1から増えます。`shouldRetry` は同期・非同期の判定に対応し、`false` を返すと再試行を打ち切ります。`delayMs` は固定のミリ秒数でも指定できます。

## 並行数・キャンセル・レート制限

再試行の判定とバックオフ中も、その項目は同じ並行スロットを使用します。キャンセルは待機を中断し、上流の読み取りエラーや後処理のエラーは再試行しません。

上流に置いた `rateLimit` は `map` 内部の再試行を数えません。再試行も含めてリクエスト回数を制限する場合は、処理関数の中で共有のレートリミッタを使います。

## 再試行後も失敗した場合

`map` は最後のエラーで実行を停止します。`mapSettled` は失敗を `{ status: "rejected", reason }` として出力し、他の項目の処理を続けます。上流の失敗やキャンセルは、どちらの場合も実行を停止します。

[RetryOptions](/mogami-stream/api/interfaces/RetryOptions.html){target="_self"} · [MapOptions](/mogami-stream/api/interfaces/MapOptions.html){target="_self"}
