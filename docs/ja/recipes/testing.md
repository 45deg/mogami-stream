# タイムラインによるテスト

テストおよびモック向けに、[`fromTimeline(events)`](/mogami-stream/api/functions/fromTimeline.html){target="_self"} は [`TimelineEvent<T>`](/mogami-stream/api/types/TimelineEvent.html){target="_self"} の配列から再実行可能な Pull 型ストリームを生成し、指定したミリ秒オフセットでの遅延到着・タイムアウト・上流エラーを再現します。

<<< @/examples/testing.ts

## スケジュールの指定ルール

| オフセット (`at`) | イベント種別 (`kind`) | 意味                                                                                      |
| ----------------- | --------------------- | ----------------------------------------------------------------------------------------- |
| `0`               | `"value"`             | 初回読み取り時（実行開始からの経過時間 `0 ms`）に `"A"` を即座に出力する。                |
| `100`             | `"value"`             | 初回読み取りから `100 ms` 以上経過した時点で `"B"` を出力する。                           |
| `150`             | `"complete"`          | `150 ms` 時点で完了する（上流の失敗を再現する場合は `kind: "error", error` を指定する）。 |

- 各イベントの `at` は、その実行の初回 `next()` 読み取りからのミリ秒オフセットを表す有限・非負の数値であり、昇順（非減少順）で並べる必要があります。
- 同じ `at` 時刻を持つ複数のイベントは、配列内の順序通りに処理されます。
- `"complete"` または `"error"` イベントは配列の末尾にのみ配置できます。省略した場合は最後の `"value"` イベントの直後に完了します（空配列 `fromTimeline([])` は初回読み取りで即座に完了します）。
- 不正なスケジュールを渡した場合は、`fromTimeline` の呼び出し時点で即座に `RangeError` または `TypeError` が投げられます。イベント配列は構築時にシャローコピーされ、値やエラーオブジェクトの参照（Promise を値として持つ要素を含む）はそのまま保持されます。

[fromTimeline](/mogami-stream/api/functions/fromTimeline.html){target="_self"} · [TimelineEvent](/mogami-stream/api/types/TimelineEvent.html){target="_self"}
