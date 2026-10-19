# 並行数・容量・需要

[`map`](/mogami-stream/api/functions/map.html){target="_self"} と [`mapSettled`](/mogami-stream/api/functions/mapSettled.html){target="_self"} は `{ concurrency = 1, capacity = concurrency, ordered = true }` を受け取ります。`concurrency` と `capacity` はどちらも正の安全な整数（Safe Integer）であり、`capacity >= concurrency` を満たす必要があります。

| パラメータ    | 上限枠を消費するもの                                                                                         |
| ------------- | ------------------------------------------------------------------------------------------------------------ |
| `concurrency` | 予約済みの上流読み取り + 実行中の mapper コールバック（項目ごとの `shouldRetry` 判定やバックオフ待機を含む） |
| `capacity`    | 予約済みの上流読み取り + 実行中の mapper コールバック + 下流への出力を待っている完了済み結果                 |

同時に進行する上流の `next()` 読み取りは常に最大1つです。`capacity > concurrency` に設定すると、時間のかかる先行アイテムの後ろで完了したアイテムをバッファに保持しつつ、並行実行数の上限（`concurrency`）内で後続アイテムの変換を進めることができます。

各 mapper コールバックには [`MapContext`](/mogami-stream/api/interfaces/MapContext.html){target="_self"} `{ index, signal, attempt }` が渡されます。`index` は実行ごとに `0` から始まる要素番号、`attempt` は `1` から始まる試行回数です。

## スロットの状態推移（インタラクティブ図解）

`concurrency: 2`、`capacity: 3`、`ordered: true` に設定した単一の `map` ステージの動作をステップごとに確認できます：

<SlotDiagram />

## 出力順序：`ordered: true` と `ordered: false`

入力 `A`、`B`、`C` がこの順で開始され、`A` の処理中に `B` と `C` が先に完了した場合の挙動は次の通りです：

| 設定                    | 出力順序    | バッファの挙動                                                                                 |
| ----------------------- | ----------- | ---------------------------------------------------------------------------------------------- |
| `ordered: true`（既定） | `A → B → C` | 先頭の `A` が完了して出力されるまで、`B` と `C` は完了後も `capacity` スロットを占有し続ける。 |
| `ordered: false`        | `B → C → A` | 下流から要求があれば `A` の完了を待たずに `B`・`C` を即座に出力し、スロットを早期に解放する。  |

`map` では、いずれかの実行中コールバックが未処理のエラーを投げた時点でステージ全体が即座に停止し、他の実行中コールバックの `signal` も abort されます（`ordered: true` で先行アイテムがまだ実行中の場合でも即座に失敗します）。個々のアイテムの失敗でストリームを止めず、`{ status: "rejected", reason }` として結果を受け取りたい場合は `mapSettled` を使用します。

## 下流の需要（Demand）によるバックプレッシャー

並行ステージは、下流の `next()` リクエストが待機している間だけ新しい上流読み取りを開始します：

1. 下流が `next()` を呼ぶと、ステージは `concurrency` と `capacity` の上限まで上流読み取りと mapper の実行を開始します。
2. 待機中だった下流の `next()` に値が1件引き渡されると、次の `next()` が呼ばれるまで新たな上流読み取りは開始されません。
3. すでに開始されていた読み取りや実行中の mapper（リトライ待機中のものを含む）は予約済みの `capacity` スロット内で完了まで進みますが、追加の入力は引き込まれません。

各ステージはそれぞれ独立して自身の上限を管理します。並行 `map` ステージを2段つないだパイプラインでは、それぞれのステージが独立したスロット枠を持ちます。なお、外部のソース自身が持つバッファや、`toArray` などの終端コレクタが蓄積する配列のメモリはステージ上限の対象外です。

## 子ストリームの並行数

[`flatten`](/mogami-stream/api/functions/flatten.html){target="_self"} と [`concat`](/mogami-stream/api/functions/concat.html){target="_self"} は、ストリームの要素として渡された子ストリームを1本にまとめます。[`flatMap`](/mogami-stream/api/functions/flatMap.html){target="_self"} と [`concatMap`](/mogami-stream/api/functions/concatMap.html){target="_self"} は、各要素から子ストリームを生成して1本にまとめます。

これらのオペレータは `concurrency` で同時に処理する子ストリーム数を制限します。既定値は `1` です。

- 子ストリームは、スロット予約から自身のクリーンアップ（`return()` および `finalize`）が完了するまで1スロットを占有します。
- アクティブな各子ストリームが保持するのは、最大1つの読み取り中タスクまたは1件のバッファ済みアイテムのみです。
- 順序を保持する `concat` と `concatMap` では、後続の子ストリームが先に読み終わった場合でも、先行する子ストリームがすべて完了するまでスロットを解放しません。これにより、遅い先頭の子ストリームの背後で後続の子ストリームが無制限に溜まり続けるのを防ぎます。

## イベントループと同期処理の注意点

パイプラインは、要素間に人工的な `setTimeout` やマクロタスクの yield を挿入しません。同期的なソースと同期的なコールバックが I/O を挟まずに連続実行されると、ホストのイベントループを占有してタイマーや I/O を飢餓状態（Starvation）にする場合があります。大量の CPU バウンドな同期データを扱う際は、ソース・コールバック・コンシューマのいずれかで明示的にイベントループへ制御を返してください。

詳細なオプション型は [MapOptions](/mogami-stream/api/interfaces/MapOptions.html){target="_self"}、リトライ設定の実践例は [リトライ](./retry.md)を参照してください。
