# オペレータの使い分け

各オペレータの使用例と API リファレンスへのリンクは[一覧](../operators.md)にあります。並行数や時間制限を設定する前に、値の流し方を選びます。

| 目的                           | オペレータ                                                                                                                                | 動作                                                                                                                                        |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| 変換・絞り込み                 | [`map`](../operators.md#operator-map)、[`filter`](../operators.md#operator-filter)、[`filterMap`](../operators.md#operator-filtermap)     | [`map`](../operators.md#operator-map) は並行実行に対応。他の2つは逐次実行。                                                                 |
| 件数制限・バッチ化             | [`take`](../operators.md#operator-take)、[`chunks`](../operators.md#operator-chunks)、[`bufferTime`](../operators.md#operator-buffertime) | 配置した位置で件数を数える。[処理順](./building-pipelines.md)を参照。                                                                       |
| 指定したソースを順に連結       | [`chain(a, b)`](../operators.md#operator-chain)                                                                                           | 前のソースを閉じてから次を開く。                                                                                                            |
| 指定したソースの値を混ぜて出力 | [`merge(a, b)`](../operators.md#operator-merge)                                                                                           | ソースを並行して読み取る。                                                                                                                  |
| 子ソースのストリームを平坦化   | [`concat`](../operators.md#operator-concat)、[`flatten`](../operators.md#operator-flatten)                                                | [`concat`](../operators.md#operator-concat) は子の順序を維持。[`flatten`](../operators.md#operator-flatten) は準備できた子から出力。        |
| 項目ごとに子ソースを生成       | [`concatMap`](../operators.md#operator-concatmap)、[`flatMap`](../operators.md#operator-flatmap)                                          | 同期コールバックで生成し、子の順序どおりに、または準備できた子から出力。非同期の準備には [`defer`](../operators.md#operator-defer) を使う。 |
| 同じ位置の値を組にする         | [`zip(a, b)`](../operators.md#operator-zip)                                                                                               | 最も要素数の少ないソースが終了すると停止。                                                                                                  |
| 最新値を組にする               | [`zipLatest([a, b])`](../operators.md#operator-ziplatest)                                                                                 | 全ソースの初回出力を待つ。`partial: true` なら未出力を `undefined` で埋める。                                                               |

[`concat`](../operators.md#operator-concat)・[`flatten`](../operators.md#operator-flatten)・[`concatMap`](../operators.md#operator-concatmap)・[`flatMap`](../operators.md#operator-flatmap) の既定値は `concurrency: 1` です。並行数を増やしても、[`concat`](../operators.md#operator-concat) と [`concatMap`](../operators.md#operator-concatmap) は子ソースの順序どおりに出力します。[並行数の上限](./concurrency.md#子ストリームの並行数)も参照してください。

読み取りペースや期限は[時間制御](./timing.md)、再試行や代替ソースは[エラー処理](./errors.md)で説明しています。読み取りを始めるには[終端処理](./consumers.md)を選びます。

[`broadcast`](./branching.md#broadcast) は離脱していない全分岐に同じ値を配り、[`route`](./branching.md#route) は各項目を最大 1 つの分岐に送ります。どちらもソースを 1 回だけ実行し、分岐ごとのキューに容量の上限を設けます。

## 集約する状態

[`scan`](../operators.md#operator-scan) は更新のたびに累積値を出力し、[`reduce`](../operators.md#operator-reduce) は最後の値だけを返します。どちらも初期値と集約コールバックの結果を await し、入力が空なら [`scan`](../operators.md#operator-scan) は何も出力せず、[`reduce`](../operators.md#operator-reduce) は初期値を返します。

オブジェクトは複製されず、参照が渡されます。各時点の状態を残すなら、新しいオブジェクトを返してください。状態を直接変更する場合は、[`defer`](../operators.md#operator-defer) 内で実行ごとに作り、次の変更前に必要な値を取り出します。

<<< @/examples/value-semantics.ts#state

## Promise を要素として扱う

[`from`](../operators.md#operator-from) と選択・合成オペレータは Promise オブジェクトを要素として保持します。同期 Iterable に対する標準の `for await` と異なり、要素の Promise を解決値に置き換えません。解決値を得るには [`map`](../operators.md#operator-map) を使います。

<<< @/examples/value-semantics.ts#promises

[`find`](../operators.md#operator-find) の条件関数には元の要素が渡りますが、一致した Promise を返す際にはその解決を待ちます。

## 範囲と移動窓を選ぶ

`slice(start, end)` は開始位置を含み、終了位置を含まない範囲を選択します。`start` の省略時は 0、`end` の省略時は入力の完了まで読み取ります。位置には安全な整数を指定します。小数・`NaN`・無限大は `RangeError` になります。非負の位置を指定した `slice(2, 5)` は先頭の 2 件を飛ばして 3 件を出力し、最後の選択値を渡す前に上流を閉じます。`slice(5, 2)` のように空と判断できる範囲では、上流を開きません。

負の位置は末尾から数えます。`slice(2, -3)` は末尾判定用の 3 件だけを保持し、3 件遅れて順次出力します。`slice(-5)` と `slice(-5, -2)` は最大 5 件を保持して入力の完了を待ちます。そのため、開始位置が負の場合は有限の入力が必要です。

`slidingWindow(size)` は 1 件ずつ窓をずらしながら、重なりのある配列を出力します。指定件数が揃うまで待ち、不完全な窓は出力しません。`size` には正の安全な整数を指定します。`chunks` と異なり、隣り合う窓で項目が重なります。各出力配列は独立していますが、配列内の要素は元の参照を維持します。内部で保持する値は最大 `size` 件で、各配列の生成には O(size) の処理が必要です。

```ts
const averages = await from([1, 2, 3, 4, 5])
  .slidingWindow(3)
  .map(
    (window) => window.reduce((sum, value) => sum + value, 0) / window.length,
  )
  .toArray(); // [2, 3, 4]
```

1 つの位置だけを取得する場合は [`at`](./consumers.md) を使います。選択した範囲は `toArray()`・`count()`・`drain()` などで消費できます。
