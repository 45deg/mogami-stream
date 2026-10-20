---
title: 有界な並行制御を備えた AsyncIterable パイプライン
---

# mogami-stream

安全な非同期ストリーム処理のための TypeScript オペレータ集（ゼロ依存 / 並行数・バッファ制限 / キャンセル / クリーンアップ対応）

## 使い方

### 1. インストール

::: code-group

```sh [npm]
npm install mogami-stream
```

```sh [pnpm]
pnpm add mogami-stream
```

```sh [Bun]
bun add mogami-stream
```

:::

### 2. パイプラインを実行する

<<< @/examples/basics.ts

このパイプラインは次のように動作します。

- `from` は配列からソースを作ります。
- `.map(...)` は各値の変換方法を組み立てます。この時点では処理を始めません。
- `.toArray()` がストリームを消費し、後処理の完了後に `[2, 4, 6]` を返します。

基本用語と API の並行取得の例は[はじめに](./guide/getting-started.md)を参照してください。

[はじめに](./guide/getting-started.md) · [オペレータ](./operators.md) · [API リファレンス](/mogami-stream/api/index.html){target="_self"} · [English](../index.md)

## 採用する場面 {#when-to-use}

- 大量の URL を同時リクエスト数と出力待ちの結果数に上限を設けて取得したい。
- DB の行や非同期ジェネレータの値を全件メモリに集めず、届くたびに変換したい。
- WebSocket や SSE のイベントをキューの上限と満杯時の方針を決めて処理したい。
- 失敗した項目だけを再試行したり、各項目の成功・失敗を受け取りながら残りの処理を続けたりしたい。
- 必要な件数が揃ったときやユーザーがキャンセルしたときに、I/O に停止を伝え、リソースの解放完了まで待ちたい。

## 設計

- 並行実行数とバッファの厳密な上限管理。`map` と `mapSettled` は `concurrency` で同時実行数を制限し、上流の読み取り・実行中コールバック・下流への出力待ち結果の合計を `capacity` 以下に保ちます。子ストリームを合成するオペレータ（`flatten`、`concat`、`flatMap`、`concatMap`）も同様に子ストリームの同時実行数を制限し、先行ストリームの完了を待つ間にバッファが無制限に膨らむのを防ぎます。
- 需要駆動（Demand-driven）の遅延実行。パイプラインの構築や `.pipe()` の時点では処理を開始せず、最初の `next()` 呼び出しで実行が始まります。並行ステージは下流の `next()` が待機している間だけ新しい上流読み取りを開始し、値を1件渡すと次の要求が来るまで新たな読み取りを行いません。
- 即時キャンセルとクリーンアップ完了の分離。`iterator.cancel(reason)` は待機中の `next()` を即座に reject して上流ステージへ `AbortSignal` で停止を伝えます。一方、`iterator.closed` を await すると、進行中の処理と `finalize` の完了を待って `CloseReport` を受け取れます。
- 非同期クリーンアップの待機とエラー保全。非同期の `finalize` コールバックは上流ステージと実行中の処理がすべて終わった後に、内側から外側へ順番に実行されます。終端コンシューマはクリーンアップ完了まで待機し、最初の処理エラーを優先して保持しつつ、後続のエラーを `suppressedErrors` に記録します。
- 標準 Iterable との相互運用。同期 `Iterable` や非同期ジェネレータをそのままラップでき、型推論が働く `.map()` や `.filter()` で処理をつなぎ、標準の `for await...of` や `.toArray()` などの終端メソッドで消費できます。

## ドキュメント構成

- [はじめに](./guide/getting-started.md): インストール、最小のパイプライン、リクエストの並行取得とキャンセル。
- [パイプラインの組み立て方](./guide/building-pipelines.md): 一つの例で処理順、値と型、件数制限、結果の受け取り方を確認。
- [オペレータの使い分け](./guide/operators.md): 合成方法の選択、集約する状態、Promise 要素。
- [オペレータ](./operators.md): カテゴリ別の説明、使用例と出力結果。
- [終端処理](./guide/consumers.md): 結果の収集・集約、1件ずつの処理、条件による早期終了。
- [並行数・容量・需要](./guide/concurrency.md): スロットの状態遷移図、`ordered: true` と `false` の違い、バックプレッシャー、子ストリームの並行数。
- [時間制御](./guide/timing.md): 読み取り回数、出力間隔、タイムアウト、時間によるバッチ化。
- [エラー処理](./guide/errors.md): エラーによる停止、項目ごとの結果、再試行、代替ソース、後処理のエラー。
- [キャンセルと後処理](./guide/lifecycle.md): 実行の停止方法、`finalize` の実行順序、リソースの所有権。
- [実践レシピ](./recipes/api-requests.md): [API 取得](./recipes/api-requests.md)、[WebSocket と SSE](./recipes/events.md)、[DB バッチ書き込み](./recipes/database.md)、[タイムラインによるテスト](./recipes/testing.md)、[イベントループへ処理を譲る](./recipes/event-loop.md)、[リクエスト制限の共有](./recipes/shared-rate-limiter.md)、[結果と後処理のエラー](./recipes/terminal-report.md)。
- [ベンチマーク](./benchmarks.md): Node.js / Bun での性能比較と再現手順。
