# API 取得

URL を非同期 Iterable で渡し、最大5件を並行取得します。結果は完了順に処理し、全件をメモリに保持しません。

<<< @/examples/api-requests.ts

[`rateLimit`](../operators.md#operator-ratelimit) はこの実行内の上流読み取り開始数を毎秒5回までに制限します。`signal` を `fetch` に渡してキャンセルを伝えます。再試行が必要な場合は[リトライ](../guide/errors.md#retry)を参照してください。
