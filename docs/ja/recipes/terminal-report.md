# 終端の結果と終了報告を取得する

終端処理の結果と副次的なエラー（`suppressedErrors`）の両方が必要な場合は、`iterate()` で取得したイテレータを保持しつつ、同じイテレータを `toArray` に渡して消費します。次のヘルパーは、成功・失敗の結果と `CloseReport` をまとめて返し、いずれの場合もクリーンアップ完了まで待機します：

<<< @/examples/terminal-report.ts#collect

## 結果とエラーの扱い

JavaScript では `undefined` も throw できるため、`reason` の真偽値ではなく `status` で成否を判定します。失敗時の `reason` には主たる失敗またはキャンセルの理由が入り、`report.suppressedErrors` には処理中やクリーンアップ中に起きた副次的なエラーが格納されます（クリーンアップのみが失敗した場合も `status: "rejected"` となります）。`collectWithReport` はストリームのエラーを直接 throw せず結果オブジェクトとして返すため、呼び出し側で必ず `status` を確認してください。

`await source.toArray(options)` から置き換える場合は、`await collectWithReport(source, options)` を呼び出して `report` を確認したうえで、`status` に応じて `value` を返すか `reason` を再スローします。キャンセル用の `{ signal }` はヘルパーに渡して同一イテレータに適用し、レポート取得のために別実行を開始したり同じイテレータを並行して読み取ったりしないでください。

## コールバックのエラーも取得する

生成済みの `StreamIterator` を `reduce`・`forEach`・`find`・`some`・`every` に渡すと外側に別の実行が作られるため、それら終端関数のコールバック内で起きた例外は元のイテレータの `closed` レポートには記録されません。コールバックのエラーも1つの `CloseReport` にまとめたい場合は、処理を `map` や `tap` などのパイプラインオペレータ内に配置してから `iterate()` を呼び出してください。

最終結果または主エラーだけが必要な箇所では、通常の終端メソッドをそのまま使えます。

## タイムアウト通知とクリーンアップ完了を分ける

終端コンシューマの Promise はクリーンアップ完了まで解決・拒否されないため、キャンセルに応じない処理が残っているとタイムアウトを即座に検知できません。タイムアウト発生を即座に受け取りつつ、後処理の完了も別途待機したい場合は、`iterator.next()` と `iterator.closed` を分けて扱います：

<<< @/examples/terminal-report.ts#timeout

待機中の `next()` はタイムアウト発生時点で即座に `TimeoutError` で reject される一方、`closed` は保留中の処理と `finalize` がすべて完了してから解決し、後から発生した両方のエラーをレポートに記録します。なお、終端コンシューマを `Promise.race` で包んでも元の実行はキャンセルされず、クリーンアップも保証されません。必ず `next()` の reject を処理したうえで、最終的に `closed` を await してください。

終了状態とエラーの優先順位については、[`CloseReport` の説明](../guide/errors.md#cleanup-errors)を参照してください。
