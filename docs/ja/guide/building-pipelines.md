# パイプラインの組み立て方

タスクの一覧から、未完了のタスクを先頭から2件選び、タイトルの前後の空白を取り除きます。「選ぶ」「変換する」「件数を決める」をそれぞれオペレータに対応させて組み立てます。

## オペレータの適用順

<<< @/examples/building-pipelines.ts#pipeline

[`filter`](../operators.md#operator-filter) で未完了のタスクを選び、[`map`](../operators.md#operator-map) でタイトルを整え、[`take(2)`](../operators.md#operator-take) で結果を2件に制限します。完了済みのタスクは [`map`](../operators.md#operator-map) に届かないため、そのタイトルを整える処理は行いません。

オペレータはメソッドの記述順に適用されます。この例の [`map`](../operators.md#operator-map) はタスクを文字列に変えるので、その後では `task.done` を参照できません。タスクの状態による絞り込みは変換前に置きます。

## 各ステージの値と型

`Stream<T>` の `T` はその段階で流れる値の型です。各段階から出る値を追うと、次のコールバックが何を受け取るか分かります。

| 段階                                             | 値                           | 型                  |
| ------------------------------------------------ | ---------------------------- | ------------------- |
| [`from(tasks)`](../operators.md#operator-from)   | タスクのオブジェクト         | `Stream<Task>`      |
| [`filter(...)`](../operators.md#operator-filter) | 未完了のタスクのオブジェクト | `Stream<Task>`      |
| [`map(...)`](../operators.md#operator-map)       | 前後の空白を除いたタイトル   | `Stream<string>`    |
| [`take(2)`](../operators.md#operator-take)       | 最大2件のタイトル            | `Stream<string>`    |
| [`.toArray()`](../operators.md#operator-toarray) | タイトルを集めた配列         | `Promise<string[]>` |

この例の [`filter`](../operators.md#operator-filter) は対象を選び、[`map`](../operators.md#operator-map) は値と型を変えます。[`take`](../operators.md#operator-take) は流れる値の型を変えません。`await` した `result` は `string[]` です。

## `take` の位置と取得件数

[`take(2)`](../operators.md#operator-take) が数えるのはその位置に届いた値です。未完了のタスクを2件選ぶなら、最初の例のように [`filter`](../operators.md#operator-filter) より後ろに置きます。未完了が2件に満たなければ、得られる結果も2件未満です。

先に入力を2件に制限すると、対象は元の配列の先頭2件だけになります。

<<< @/examples/building-pipelines.ts#input-limit

2件目は完了済みなので、結果は `"Review draft"` の1件です。後にある `"Book room"` は読み取りません。制限したいのが入力件数か、条件に合う件数かで、[`take`](../operators.md#operator-take) の位置を選びます。

## 配列への収集と1件ずつの処理

最初の例の [`.toArray()`](../operators.md#operator-toarray) は結果を配列にまとめて使う場合に向いています。このパイプラインでは [`take(2)`](../operators.md#operator-take) があるので、配列に入るのは最大2件です。件数を制限せずに使う場合は、全結果をメモリに保持できるか確認してください。

配列が不要なら、同じ `titles` を [`forEach`](../operators.md#operator-foreach) で1件ずつ処理できます。

<<< @/examples/building-pipelines.ts#consume

この呼び出しは配列 `tasks` から新たに読み取る別の実行です。[`forEach`](../operators.md#operator-foreach) は処理結果の配列を作らず、非同期のコールバックも1件ずつ完了を待ちます。

集約や条件による早期終了は[終端処理](./consumers.md)、非同期処理の同時実行は[並行数・容量・需要](./concurrency.md)を参照してください。外部 I/O を扱う場合の停止と解放は[キャンセルと後処理](./lifecycle.md)で説明しています。

## `.pipe()` と独自オペレータ

複数の独自オペレータは [`.pipe(a, b)`](../operators.md#operator-pipe) で渡せます。各オペレータは直前のソースを受け取って新しいストリームを返します。返されたストリームにも変換メソッドを適用できます。

<<< @/examples/building-pipelines.ts#custom-operators

## エントリポイントと型推論 {#entry-points}

`mogami-stream/core` は関数形式のオペレータ、[`.pipe()`](../operators.md#operator-pipe)、終端メソッドを提供します。[`.map()`](../operators.md#operator-map) などの変換メソッドを含まないため、バンドラーが未使用オペレータを除去しやすくなります。

<<< @/examples/core.ts

実行と後処理の仕組みはどちらのエントリでも共通です。core のストリームで変換メソッドを使うには、`mogami-stream` から読み込んだ [`from()`](../operators.md#operator-from) に渡します。import するだけでは、既存のストリームにメソッドは追加されません。

メソッドチェーンの段数に上限はありません。[`.pipe()`](../operators.md#operator-pipe) と関数形式の [`pipe`](../operators.md#operator-pipe) は1回につき最大8個のオペレータまで型推論するため、長いパイプラインは [`.pipe()`](../operators.md#operator-pipe) を分けます。`const first = take(1)` のような値の型に依存しないオペレータは、異なる要素型でも再利用できます。
