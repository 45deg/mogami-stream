# キャンセルと後処理

## ストリームの停止方法

[`iterate(source, { signal })`](/mogami-stream/api/functions/iterate.html){target="_self"} で取得できる [`StreamIterator`](/mogami-stream/api/interfaces/StreamIterator.html){target="_self"} で、実行を停止したり、クリーンアップの完了を待ったりできます。

| メソッド / プロパティ     | 挙動                                                                                                                                          |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `iterator.cancel(reason)` | 即時停止を要求し、各ステージの `AbortSignal` を abort する。待機中の `next()` はクリーンアップ完了を待たずに即座に reject される。            |
| `iterator.return()`       | 停止を要求し、各ステージの `AbortSignal` を abort する。上流のクリーンアップと [`finalize`](../operators.md#operator-finalize) の完了を待つ。 |
| `iterator.throw(error)`   | 失敗による停止を要求し、クリーンアップ完了を待たずに reject する（上流ジェネレータの `.throw()` へは転送しない）。                            |
| `iterator.closed`         | すべてのクリーンアップ完了後に [`CloseReport`](/mogami-stream/api/types/CloseReport.html){target="_self"} で必ず解決する Promise。            |

1つの `StreamIterator` に対して同時に待機できる `next()` は1つだけです。並行して2つ目の `next()` を呼ぶと `TypeError` で reject されます。エラーまたはキャンセルが発生すると、待機中の `next()`（または次の1回の `next()`）がその理由で reject され、以降のすべての `next()` は `{ done: true, value: undefined }` で解決します。

`return()` は正常に終了すると `{ done: true, value: undefined }` で解決し、クリーンアップだけが失敗した場合も含め、実行が失敗していれば主エラーで reject します。キャンセル後はクリーンアップが失敗しても `{ done: true, value: undefined }` で解決し、そのエラーは `closed` の解決値の `suppressedErrors` に残ります。

後から `throw(error)` を呼んでも、すでに確定したキャンセル理由や主エラーが優先されます。`throw()` の後に後処理の完了を待つには、`closed` を await してください。

<<< @/examples/lifecycle.ts

### `AbortSignal` による協調的キャンセル

JavaScript の非同期イテレータは、任意の `await` を外部から強制終了できません。`cancel()`、`return()`、[`take(n)`](../operators.md#operator-take)、または外部 `AbortSignal` によってステージが停止する際、`mogami-stream` は [`defer`](../operators.md#operator-defer) や各オペレータ（[`map`](../operators.md#operator-map)、[`filter`](../operators.md#operator-filter)、[`forEach`](../operators.md#operator-foreach) など）のコールバックに渡している `signal` を abort します。

`fetch`、ファイル I/O、DB クエリ、タイマーなどの外部 I/O には必ずこの `signal` を渡してください。I/O 側が `signal` を無視して完了しない場合、`return()`、`closed`、および終端コンシューマはその処理が終わるまで待機し続けます。

## `finalize` の実行順序と `take(n)` のクローズタイミング

[`finalize(effect)`](/mogami-stream/api/functions/finalize.html){target="_self"} はステージの終了時に実行される同期・非同期のクリーンアップ処理を登録します。

- 登録した関数はそのステージが開始された場合（少なくとも1回 `next()` が到達した場合）に限り、実行ごとに1回だけ呼ばれます。
- 上流の子ステージのクローズと、そのステージ内の進行中タスクがすべて完了した後に実行され、戻り値の Promise も完了まで await されます。
- パイプライン内で複数の [`finalize`](../operators.md#operator-finalize) がネストしている場合、内側（上流側）から外側（下流側）の順に実行されます。

[`take(n)`](../operators.md#operator-take) は `n` 件目の要素を受け取ると、その要素を下流へ渡す前に上流ステージを閉じ始めます。このとき下流ステージの `signal` は有効なまま維持されるため、下流のオペレータやコンシューマは最後の `n` 件目の要素を正常に処理できます。

## ジェネレータでリソースを管理する

[`defer`](../operators.md#operator-defer) で実行ごとに新しいジェネレータを作り、取得と解放を `try/finally` にまとめます。

<<< @/examples/resources.ts

各実行は最初の読み取りでカーソルを開き、正常終了・失敗・早期終了時に閉じます。行の読み取りにも `signal` を渡してください。取得が途中で失敗した場合の解放は `openCursor` 側で行います。

## 非同期の準備処理でリソースを管理する {#owning-resources-during-asynchronous-setup}

[`defer`](../operators.md#operator-defer) のファクトリと [`catchError`](../operators.md#operator-catcherror) のハンドラは `signal` と `acquire(acquire, release)` を持つ `ResourceContext` を受け取ります。ソースを返す前にカーソル、セッション、ファイルなどを開く場合は `acquire` を使います。

<<< @/examples/resource-ownership.ts

### 取得と解放のタイミング

ファクトリまたはハンドラの実行中に `context.acquire` を呼び、await してください。取得用のコールバックにはステージの `signal` が渡されます。取得を始める前にステージが停止していれば、コールバックは呼ばれません。

取得に成功すると、キャンセルの確認より先に `release(resource)` が登録されます。そのため、キャンセル後に取得が完了したリソースも解放されます。この場合、`acquire` の Promise はキャンセル理由で reject し、ソースは開かれません。

完了・失敗・途中終了のいずれでも、子ソースが閉じ、ステージ内で実行中の処理が終わった後に、登録された解放処理をそれぞれ1回だけ呼びます。解放処理は登録と逆の順序で実行され、`closed` と終端コンシューマはその完了を待ち、取得や解放が完了しなければ後処理も待機し続けます。ファクトリまたはハンドラの処理が終わった後は、そのコンテキストで新たな取得を始められません。

### 取得・解放時のエラー

取得に失敗すると、そのリソースを所有するステージが失敗します。上流の [`defer`](../operators.md#operator-defer) で起きた失敗は [`catchError`](../operators.md#operator-catcherror) で処理できます。取得用のコールバックが例外を投げる場合、途中まで確保したリソースの解放はそのコールバックの責任です。

解放時のエラーには[後処理のエラー優先順位](./errors.md#cleanup-errors)が適用されます。

### 所有権と二重解放

取得に成功した `acquire` の呼び出しごとに、ステージがリソースを所有します。返すソースはそのリソースを借りて使うため、イテレータの `return()` や `finally` で重ねて解放しないでください。例の `rows` ジェネレータは独立して閉じ、その後で登録済みのコールバックがカーソルを閉じます。同じリソースを二重に登録しないでください。

すでに開いたイテレータを扱う場合は、その `return()` を解放処理として登録します。そのイテレータ自体を返す代わりに、`next()` を呼び出すソースを返し、解放を登録済みの処理に任せます。

通常のソースを返す場合、ライブラリがそのイテレータを所有するのは、ソースを開いた後です。キャンセル後に返された通常のソースは調べることも、開くことも、閉じることもしません。イテレータの取得によって、別の処理が始まる可能性があるためです。ファクトリやハンドラが直接確保したリソースは、所有権を明示的に登録するまで、そのコールバックが管理します。

## `CloseReport` の構造とエラー優先順位

`iterator.closed` は後処理の完了後に終了状態を返します。[エラー処理](./errors.md#cleanup-errors)で `CloseReport` の構造とエラーの優先順位を説明しています。

## `catchError` によるエラー回復境界

上流の失敗から代替ソースへ切り替える方法と、捕捉できるエラーの範囲は、[エラー処理の `catchError`](./errors.md#catch-error)を参照してください。

## 終端の結果と終了報告を取得する

結果と副次的なエラーをまとめて取得する方法は[終端の結果と終了報告のレシピ](../recipes/terminal-report.md)を参照してください。タイムアウト通知と後処理の完了を分ける例もあります。

[StreamIterator](/mogami-stream/api/interfaces/StreamIterator.html){target="_self"} · [CloseReport](/mogami-stream/api/types/CloseReport.html){target="_self"} · [finalize](/mogami-stream/api/functions/finalize.html){target="_self"} · [catchError](/mogami-stream/api/functions/catchError.html){target="_self"}
