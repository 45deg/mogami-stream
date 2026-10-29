# 終端処理

[`.pipe()`](../operators.md#operator-pipe) で組み立てたストリームは、終端処理が値を読み取り始めると実行されます。結果を配列に集める、1つの値に集約する、1件ずつ処理するなど、用途に合わせて選びます。

## 処理を選ぶ

| 目的                          | API                                                                    | 結果                             |
| ----------------------------- | ---------------------------------------------------------------------- | -------------------------------- |
| 全件を配列に集める            | [`toArray`](/mogami-stream/api/functions/toArray.html){target="_self"} | 全要素の配列                     |
| 合計など1つの値に集約する     | [`reduce`](/mogami-stream/api/functions/reduce.html){target="_self"}   | 最終的な累積値                   |
| 結果を蓄積せず1件ずつ処理する | [`forEach`](/mogami-stream/api/functions/forEach.html){target="_self"} | `Promise<void>`                  |
| 条件に合う最初の値を得る      | [`find`](/mogami-stream/api/functions/find.html){target="_self"}       | 最初の一致。なければ `undefined` |
| 条件に合う値があるか調べる    | [`some`](/mogami-stream/api/functions/some.html){target="_self"}       | 1件でも一致すれば `true`         |
| 全件が条件に合うか調べる      | [`every`](/mogami-stream/api/functions/every.html){target="_self"}     | 全件が一致すれば `true`          |
| 出力を集めず完了を待つ        | [`drain`](/mogami-stream/api/functions/drain.html){target="_self"}     | `Promise<void>`                  |
| 全件数を数える                | [`count`](/mogami-stream/api/functions/count.html){target="_self"}     | `Promise<number>`                |
| 指定位置の値を得る            | [`at`](/mogami-stream/api/functions/at.html){target="_self"}           | 値。範囲外なら `undefined`       |
| 重複を除いて集める            | [`toSet`](/mogami-stream/api/functions/toSet.html){target="_self"}     | `Set<T>`                         |
| キーごとに値を集める          | [`toMap`](/mogami-stream/api/functions/toMap.html){target="_self"}     | `Map<K, T>`                      |

これらは Promise を返し、[`stream.toArray({ signal })`](../operators.md#operator-toarray) のようなメソッドと [`toArray(source, { signal })`](../operators.md#operator-toarray) のような同名の関数の両方を使えます。関数形式は通常の同期・非同期 Iterable も受け取ります。

## 配列に集める・集約する

<<< @/examples/consumers.ts#collect

[`toArray`](../operators.md#operator-toarray) は出力をすべてメモリに保持するため有限の入力に使い、[`reduce`](../operators.md#operator-reduce) は初期値から順番に集約して入力が空なら初期値を返します。上の例では配列をソースにしているため、同じストリームを読み直せます。

## 1件ずつ処理する

<<< @/examples/consumers.ts#each

[`forEach`](../operators.md#operator-foreach) はコールバックの完了を待ってから次の値を読み取ります。結果の配列は作りません。[`forEach`](../operators.md#operator-foreach) と [`reduce`](../operators.md#operator-reduce) のコールバックは逐次実行されるため、処理を並行化したい場合は上流に [`map(..., { concurrency })`](../operators.md#operator-map) を置きます。

## 条件で早期終了する

<<< @/examples/consumers.ts#match

[`find`](../operators.md#operator-find) は最初の一致、[`some`](../operators.md#operator-some) は最初の一致、[`every`](../operators.md#operator-every) は最初の不一致で上流を閉じます。入力が空の場合は、[`find`](../operators.md#operator-find) が `undefined`、[`some`](../operators.md#operator-some) が `false`、[`every`](../operators.md#operator-every) が `true` を返します。

## 位置で選ぶ・数える・収集する

`at(0)` は先頭の値を読んだ時点で上流を閉じ、`at(-1)` は最後まで読んで末尾の値を返します。範囲外なら `undefined` を返します。位置には安全な整数を指定し、小数・`NaN`・無限大は `RangeError` で拒否されます。負の位置では末尾からの距離を上限に値を保持し、入力の完了を待ちます。`find` と同様、`at` は選択した値を await します。クリーンアップの待機中にキャンセルした場合は、その後クリーンアップが失敗しても、キャンセル理由で拒否されます。

`drain()` は出力を保持せずにパイプラインを実行し、`count()` は件数を返します（空なら 0）。条件に合う件数は `filter(predicate).count()`、件数を制限した実行は `take(n).drain()` と書けます。

`toSet()` は SameValueZero による比較で重複を除きます。`toMap(keySelector)` はキーを 1 件ずつ await し、元の項目を格納します。同じキーには最後の値を残し、キーの挿入順は変えません。セレクタは `{ index, signal }` を受け取るので、キャンセル可能な I/O にも signal を渡してください。どちらも Promise の項目をそのまま保持し、入力が空なら空のコレクションを返します。

## キャンセルと後処理

`{ signal }` で実行をキャンセルできます。コールバックに渡される `signal` を I/O にも渡すと、実行中の処理にも停止を伝えられます。

終端処理は後処理が終わってから解決・拒否され、停止に対応しない I/O が残っている場合はその完了も待ちます。処理と後処理の両方が失敗した場合は、主な処理エラーを優先します。

手動で読み取る場合は `for await...of`、または [`iterate`](/mogami-stream/api/functions/iterate.html){target="_self"} を使います。[`iterate`](../operators.md#operator-iterate) は取得しただけでは実行せず、最初の `next()` で開始します。`cancel()`・`return()`・`closed` の使い分けは[キャンセルと後処理](./lifecycle.md)を参照してください。

副次的なエラーも取得する場合は、[結果と終了報告の取得レシピ](../recipes/terminal-report.md)を参照してください。クリーンアップ完了前にタイムアウトを観測する方法も説明しています。
