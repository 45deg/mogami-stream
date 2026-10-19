# エラー処理

失敗したときに実行を止めるか、他の項目を続けるか、再試行するかを選びます。

| 方針                                           | 使う機能                        |
| ---------------------------------------------- | ------------------------------- |
| 項目の処理が失敗したら止める                   | `map`                           |
| 各項目の成功・失敗を受け取り、他の項目を続ける | `mapSettled`                    |
| 失敗した項目を再試行する                       | `map` / `mapSettled` の `retry` |
| 上流が失敗したら代替ソースへ切り替える         | `catchError`                    |

## 項目の失敗で止めるか続けるか {#item-errors}

`map` はコールバックのエラーで実行を停止します。`mapSettled` は失敗を `{ status: "rejected", reason }` として出力し、他の項目の処理を続けます。上流の失敗やキャンセルは、どちらの場合も実行を停止します。

<<< @/examples/operators/mapSettled.ts

この例では、入力が2の項目は失敗し、他の項目は処理を続けます。

## 失敗した項目を再試行する {#retry}

`map` と `mapSettled` の `retry` オプションで、失敗した項目だけを再試行できます。

<<< @/examples/retry.ts

`retries: 3` は初回を含めて最大4回の試行です。`shouldRetry` で再試行するエラーを選び、`delayMs` で待機時間を指定します。上の例では失敗した試行番号に応じて 200・400・800 ms 待ちます。

各試行で項目の `index` は変わらず、`attempt` は1から増えます。`shouldRetry` は同期・非同期の判定に対応し、`false` を返すと再試行を打ち切ります。`delayMs` は固定のミリ秒数でも指定できます。

### 並行数・キャンセル・レート制限

再試行の判定とバックオフ中も、その項目は同じ並行スロットを使用します。キャンセルは待機を中断し、上流の読み取りエラーや後処理のエラーは再試行しません。

上流に置いた `rateLimit` は、失敗や完了確認を含む上流の読み取り回数を実行ごとに制限します。`map` 内部の再試行や、別の実行との合算には対応しません。外部呼び出しの各試行で共通の制限を守るには、処理関数の中で共有のレートリミッタを使います。

開始間隔と待機列を複数の実行で共有するには、[共有レートリミッタのレシピ](../recipes/shared-rate-limiter.md)を参照してください。再試行も同じ待機列に入ります。

### 再試行後も失敗した場合

再試行を打ち切ると、`map` は最後のエラーで実行を停止します。`mapSettled` はその項目を `rejected` として出力し、他の項目を続けます。

## 代替ソースへ切り替える {#catch-error}

[`catchError(handler)`](/mogami-stream/api/functions/catchError.html){target="_self"} は上流ソースの初期化エラーや処理エラーを捕捉し、1回だけ代替ソースへ切り替えます。

<<< @/examples/operators/catchError.ts

1. 上流でエラーが発生すると、`catchError` はまず上流の進行中タスクと上流の `finalize` がすべて完了するのを待ちます。
2. その後、`handler(error, { signal, acquire })` を呼び出します。ハンドラは代替ソース、またはその Promise を返せます。回復中に取得するリソースも、[`defer` のリソース所有権](./lifecycle.md#非同期の準備処理でリソースを管理する)と同じ規則に従います。
3. 以下のエラーは `catchError` では**捕捉されません**：
   - 実行全体のキャンセル（`cancel()`、`return()`、外部 `AbortSignal`）、
   - 上流のクリーンアップ処理（`return()` や `finalize`）で発生したエラー、
   - `handler` 自身が投げたエラー、および切り替え後の代替ソースから発生したエラー（代替ソース自体にも回復が必要な場合は、下流にもう1段 `catchError` をつなぎます）、
   - `catchError` より下流のステージで発生したエラー。

## 後処理のエラーと終了状態を調べる {#cleanup-errors}

`iterator.closed` はクリーンアップ中に例外が投げられても reject しません。上流のイテレータと `finalize` コールバックがすべて終了すると、次の判別共用体で解決します。

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

`acquire` で登録した解放処理の失敗にも、この規則が適用されます。すでにキャンセルや主エラーがあれば、その理由を置き換えず `suppressedErrors` に入ります。先にキャンセルや失敗がなければ、クリーンアップの失敗で実行が失敗し、その例外が `reason` になります。`suppressedErrors` には重複して入りません。

主たる終了理由と `Object.is` で一致するエラーは除外します。副次的なエラー同士は `Array.includes` の等価判定（SameValueZero）で重複を除き、最初に観測した順に記録します。オブジェクトは参照が同じ場合に重複とみなし、`NaN` や `undefined` もそれぞれ1回だけ記録します。

組み込みの終端コンシューマ（`toArray`、`forEach`、`reduce`、`find`、`some`、`every`）はすべてクリーンアップ完了まで待機し、クリーンアップ時のエラーよりも最初の処理エラーを優先して throw します。`iterator.closed` を await するだけでは停止は要求されません。手動で停止するには [`return()` または `cancel()`](./lifecycle.md#ストリームの停止方法) を呼びます。

結果と副次的なエラーを一緒に受け取るには、[終端の結果と終了報告のレシピ](../recipes/terminal-report.md)を参照してください。

[RetryOptions](/mogami-stream/api/interfaces/RetryOptions.html){target="_self"} · [MapOptions](/mogami-stream/api/interfaces/MapOptions.html){target="_self"} · [catchError](/mogami-stream/api/functions/catchError.html){target="_self"} · [CloseReport](/mogami-stream/api/types/CloseReport.html){target="_self"}
