# 並行数・容量・需要

[`map`](../operators.md#operator-map) と [`mapSettled`](../operators.md#operator-mapsettled) の上限は次のように数えます。既定値は `{ concurrency: 1, capacity: concurrency, ordered: true }` です。

| 設定          | 上限に含むもの                                                            |
| ------------- | ------------------------------------------------------------------------- |
| `concurrency` | 予約済みの上流読み取り + 実行中のコールバック（再試行の判定・待機を含む） |
| `capacity`    | 上記 + 出力待ちの結果                                                     |

どちらも正の安全な整数で、`capacity >= concurrency` が必要です。上流の読み取りは同時に最大1つです。`capacity` を増やすと、先行する要素の完了を待つ間も、結果を保持しながら後続の処理を進められます。

上限はステージごとに独立しています。外部ソースのバッファや [`toArray`](../operators.md#operator-toarray) が集める配列は対象外です。

## スロットの状態推移

`concurrency: 2`、`capacity: 3`、`ordered: true` の例です。

<SlotDiagram />

## 出力順序

入力が `A → B → C`、完了が `B → C → A` の場合：

| 設定                    | 出力順序    | 出力待ちの結果                           |
| ----------------------- | ----------- | ---------------------------------------- |
| `ordered: true`（既定） | `A → B → C` | `B` と `C` は `A` の出力まで容量を使う。 |
| `ordered: false`        | `B → C → A` | 下流が要求すれば出力でき、容量が空く。   |

書き込みなどの副作用の完了順も揃えるには、`concurrency: 1` または逐次実行の [`forEach`](../operators.md#operator-foreach) を使います。

[`map`](../operators.md#operator-map) は出力順の設定にかかわらず、未処理のコールバックエラーで即座に停止し、他のコールバックの `signal` も abort します。失敗を結果として受け取って処理を続けるには [`mapSettled`](./errors.md#item-errors) を使います。

## 下流からの読み取り要求

新しい上流読み取りを始めるのは、下流の `next()` が待機している間だけです。値を1件渡すと、次の要求まで新たな読み取りは止まります。開始済みの処理は再試行も含めて継続し、結果を予約済みの容量内に保持できます。

下流の [`take(n)`](../operators.md#operator-take) が制限するのは出力件数であり、追加の処理が開始済みの場合があります。処理対象を n 件に制限するには [`take(n)`](../operators.md#operator-take) を [`map`](../operators.md#operator-map) より前に置きます。

## 子ストリームの並行数

[`flatten`](../operators.md#operator-flatten)・[`concat`](../operators.md#operator-concat)・[`flatMap`](../operators.md#operator-flatmap)・[`concatMap`](../operators.md#operator-concatmap) の `concurrency` は、同時に扱う子ストリーム数です。既定値は `1` です。

- 各子ストリームは、予約から後処理（`return()`・[`finalize`](../operators.md#operator-finalize)）の完了まで1枠を使います。
- 各子ストリームが保持するのは、実行中の読み取り1つ、またはバッファ内の要素1件までです。
- [`concat`](../operators.md#operator-concat) と [`concatMap`](../operators.md#operator-concatmap) は、先行する子ストリームが終わるまで、読み終えた後続の子の枠も保持します。これにより、順番待ちの子が無制限に増えるのを防ぎます。

使い分けは[オペレータの利用ガイド](./operators.md)を参照してください。

## 同期処理とイベントループ

同期処理が続くと、タイマーや I/O の実行を妨げる場合があります。パイプラインは自動では実行を譲りません。必要に応じて、[イベントループへ処理を譲るレシピ](../recipes/event-loop.md)を使ってください。

[MapOptions](/mogami-stream/api/interfaces/MapOptions.html){target="_self"} · [MapContext](/mogami-stream/api/interfaces/MapContext.html){target="_self"} · [再試行](./errors.md#retry)
