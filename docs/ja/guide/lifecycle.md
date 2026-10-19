# キャンセルと後処理

## ストリームの停止方法

[`iterate(source, { signal })`](/mogami-stream/api/functions/iterate.html){target="_self"} で取得できる [`StreamIterator`](/mogami-stream/api/interfaces/StreamIterator.html){target="_self"} を使うと、実行の停止方法とクリーンアップ完了の待機を明示的に制御できます：

| メソッド / プロパティ     | 挙動                                                                                                                                 |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `iterator.cancel(reason)` | 即時停止を要求し、各ステージの `AbortSignal` を abort する。待機中の `next()` はクリーンアップ完了を待たずに即座に reject される。   |
| `iterator.return()`       | 停止を要求し、すべてのステージのクリーンアップと `finalize` が完了するのを待ってから `{ done: true, value: undefined }` で解決する。 |
| `iterator.throw(error)`   | 指定した `error` で実行を失敗させ、クリーンアップ完了を待つ（上流ジェネレータの `.throw()` へは転送しない）。                        |
| `iterator.closed`         | すべてのクリーンアップ完了後に [`CloseReport`](/mogami-stream/api/types/CloseReport.html){target="_self"} で必ず解決する Promise。   |

1つの `StreamIterator` に対して同時に待機できる `next()` は1つだけであり、並行して2つ目の `next()` を呼ぶと `TypeError` で reject されます。エラーまたはキャンセルが発生すると、待機中の `next()`（または次の1回の `next()`）がその理由で reject され、以降のすべての `next()` は `{ done: true, value: undefined }` で解決します。

<<< @/examples/lifecycle.ts

### `AbortSignal` による協調的キャンセル

JavaScript の非同期イテレータは、任意の `await` を外部から強制終了させることはできません。`cancel()`、`return()`、`take(n)`、または外部 `AbortSignal` によってステージが停止する際、`mogami-stream` は `defer` や各オペレータ（`map`、`filter`、`forEach` など）のコールバックに渡している `signal` を abort します。

`fetch`、ファイル I/O、DB クエリ、タイマーなどの外部 I/O には必ずこの `signal` を渡してください。I/O 側が `signal` を無視して完了しない場合、`return()`、`closed`、および終端コンシューマはその処理が決着するまで待機し続けます。

## `CloseReport` の構造とエラー優先順位

`iterator.closed` はクリーンアップ中に例外が投げられた場合でも決して reject されず、すべてのステージの後処理が終わった時点で以下の判別共用体（Discriminated Union）として解決します：

```ts
type CloseReport =
  | {
      readonly kind: "completed";
      readonly suppressedErrors: readonly unknown[];
    }
  | {
      readonly kind: "cancelled";
      readonly reason: unknown;
      readonly suppressedErrors: readonly unknown[];
    }
  | {
      readonly kind: "failed";
      readonly reason: unknown;
      readonly suppressedErrors: readonly unknown[];
    };
```

- **`reason`**（`"cancelled"` および `"failed"` の場合）：キャンセルの理由、または停止の引き金となった最初の主エラーを保持します。
- **`suppressedErrors`**：主たる終了理由が確定した後に、残りの実行中タスクやクリーンアップ処理（`return()` や `finalize`）から発生した副次的なエラーを配列として収集します。

組み込みの終端コンシューマ（`toArray`、`forEach`、`reduce`、`find`、`some`、`every`）はすべてクリーンアップ完了まで待機し、クリーンアップ時のエラーよりも最初の処理エラーを優先して throw します。なお、`iterator.closed` を await するだけでは停止は要求されないため、手動イテレーションを途中で抜ける際は必ず `return()` または `cancel()` を呼んでください。

## `finalize` の実行順序と `take(n)` のクローズタイミング

[`finalize(effect)`](/mogami-stream/api/functions/finalize.html){target="_self"} はステージの終了時に実行される同期・非同期のクリーンアップ処理を登録します：

- 登録した関数は、**そのステージが実際に開始された場合（少なくとも1回 `next()` が到達した場合）に限り、実行ごとに1回だけ**呼ばれます。
- 上流の子ステージのクローズと、そのステージ内の進行中タスクがすべて完了した**後**に実行され、戻り値の Promise も完了まで await されます。
- パイプライン内で複数の `finalize` がネストしている場合、**内側（上流側）から外側（下流側）の順**に実行されます。

また、`take(n)` は `n` 件目の要素を受け取った時点で—その `n` 件目を下流へ引き渡す前に—上流ステージのクローズ処理を開始します。このとき下流ステージの `signal` は有効なまま維持されるため、下流のオペレータやコンシューマは最後の `n` 件目の要素を正常に処理できます。

## `catchError` によるエラー回復境界

[`catchError(handler)`](/mogami-stream/api/functions/catchError.html){target="_self"} は上流ソースの初期化エラーや処理エラーを捕捉し、1回だけ代替ソース（Fallback Source）へ切り替えます：

1. 上流でエラーが発生すると、`catchError` はまず上流の進行中タスクと上流の `finalize` がすべて完了するのを待ちます。
2. その後、`handler(error, { signal })` を呼び出し、返された代替ソース（またはその Promise）から読み取りを再開します。
3. 以下のエラーは `catchError` では**捕捉されません**：
   - 実行全体のキャンセル（`cancel()`、`return()`、外部 `AbortSignal`）、
   - 上流のクリーンアップ処理（`return()` や `finalize`）で発生したエラー、
   - `handler` 自身が投げたエラー、および切り替え後の代替ソースから発生したエラー（代替ソース自体にも回復が必要な場合は、下流にもう1段 `catchError` をつなぎます）、
   - `catchError` より下流のステージで発生したエラー。

[StreamIterator](/mogami-stream/api/interfaces/StreamIterator.html){target="_self"} · [CloseReport](/mogami-stream/api/types/CloseReport.html){target="_self"} · [finalize](/mogami-stream/api/functions/finalize.html){target="_self"} · [catchError](/mogami-stream/api/functions/catchError.html){target="_self"}
