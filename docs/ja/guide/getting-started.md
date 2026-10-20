# はじめに

## 1. インストール

使っているパッケージマネージャで `mogami-stream` をインストールします。

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

`mogami-stream` は実行時の外部依存を持たない ESM パッケージ（`sideEffects: false`）で、TypeScript の型定義を同梱しています。Node.js 22 以降、Bun、ブラウザのメインスレッド、Dedicated Worker、Service Worker、Cloudflare Workers で動作します。

## 2. パイプラインの構築と消費

まず、配列の各値を2倍にします。

<<< @/examples/basics.ts

[`from`](../operators.md#operator-from) と [`.map()`](../operators.md#operator-map) などの変換メソッドは処理を組み立てるだけです。[`.toArray()`](../operators.md#operator-toarray) を呼ぶと読み取りが始まり、返された Promise は処理と後処理が終わると `[2, 4, 6]` に解決します。

| 用語                     | この例でのコード                                             | 役割                                                |
| ------------------------ | ------------------------------------------------------------ | --------------------------------------------------- |
| ソース                   | [`from([1, 2, 3])`](../operators.md#operator-from)           | 値の供給元となる `Stream<number>` を作る。          |
| 変換                     | [`.map((value) => value * 2)`](../operators.md#operator-map) | 読み取り時に各値を2倍にする新しいストリームを返す。 |
| 終端処理（コンシューマ） | [`.toArray()`](../operators.md#operator-toarray)             | ストリームを読み取り、値を配列に集める。            |

[`Stream<T>`](/mogami-stream/api/interfaces/Stream.html){target="_self"} は JavaScript 標準の `AsyncIterable<T>` プロトコルを実装しています。これは、消費側が `next()` で次の値を要求し、その到着を待つ仕組みです。`for await...of` でも読み取れます。

値はソースから出て、左から右へオペレータを通り、終端処理に届きます。ソースに近い側を**上流**、終端処理に近い側を**下流**と呼びます。この例では [`from(...)`](../operators.md#operator-from) が [`map`](../operators.md#operator-map) の上流、[`.toArray()`](../operators.md#operator-toarray) が下流です。

並行制御やリソース管理が役立つ用途は、[採用する場面](../index.md#when-to-use)を参照してください。

## 3. 非同期処理と停止

コールバックを非同期にし、`concurrency` で同時実行数を指定します。

<<< @/examples/quick-start.ts

最大5件を並行取得し、完了順に出力します。[`rateLimit`](../operators.md#operator-ratelimit) はこの実行の上流読み取りを毎秒5回までに制限します。[`take(20)`](../operators.md#operator-take) は出力件数の上限です。開始するリクエストも20件に制限したい場合は、[`take(20)`](../operators.md#operator-take) を [`map`](../operators.md#operator-map) より前に置きます。

`controller.abort()` で停止でき、コールバックの `signal` を `fetch` に渡すと実行中のリクエストにも中断を伝えます。[`toArray`](../operators.md#operator-toarray) は後処理が終わってから解決・拒否されます。

配列は繰り返し読み取れますが、生成済みのジェネレータは1回読み終えると空になります。毎回新しいソースが必要なら [`defer`](../operators.md#operator-defer) を使います。

次は[パイプラインの組み立て方](./building-pipelines.md)へ進んでください。機能の選択は[オペレータの使い分け](./operators.md)、リソースの解放は[キャンセルと後処理](./lifecycle.md)で説明しています。
