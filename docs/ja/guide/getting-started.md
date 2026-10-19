# はじめに

## 1. インストール

任意のパッケージマネージャで `mogami-stream` をインストールします：

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

`mogami-stream` はランタイム依存ゼロの純粋な ESM パッケージ（`sideEffects: false`）で、TypeScript の型定義を同梱しています。Node.js 22 以降、Bun、ブラウザのメインスレッド、Dedicated Worker、Service Worker、Cloudflare Workers で動作します。

## 2. パイプラインの構築と消費

まず、有限の配列の値をそれぞれ2倍にします。

<<< @/examples/basics.ts

`from` と `.pipe()` は処理を組み立てるだけです。`.toArray()` を呼ぶと読み取りが始まり、処理と後処理の完了後に `[2, 4, 6]` へ解決する Promise が返ります。

| 用語                     | この例でのコード            | 役割                                         |
| ------------------------ | --------------------------- | -------------------------------------------- |
| ソース                   | `from([1, 2, 3])`           | 値の供給元となる `Stream<number>` を作る。   |
| オペレータ               | `map((value) => value * 2)` | `.pipe()` がソースに適用する変換関数を返す。 |
| 終端処理（コンシューマ） | `.toArray()`                | ストリームを読み取り、値を配列に集める。     |

[`Stream<T>`](/mogami-stream/api/interfaces/Stream.html){target="_self"} は JavaScript 標準の `AsyncIterable<T>` プロトコルを実装しています。これは、消費側が `next()` で次の値を要求し、その到着を待つ仕組みです。`for await...of` でも読み取れます。

値はソースからオペレータを左から右へ通り、終端処理に届きます。ソースに近い側を**上流**、終端処理に近い側を**下流**と呼びます。この例では `from(...)` が `map` の上流、`.toArray()` が下流です。

並行制御やリソース管理が役立つ用途は、[採用する場面](../index.md#when-to-use)を参照してください。

## 3. 非同期処理を加える

API を並行取得する場合は、変換コールバックを非同期にして並行数の上限を指定します。次の例では、読み取りペースの制限と件数による早期終了も加えています。

<<< @/examples/quick-start.ts

1. **`from(urls)`** は同期 `Iterable` または `AsyncIterable` を `Stream` にラップします。大規模なデータセットでは、全件の配列を事前に作るのではなく、非同期ジェネレータやストリーミングカーソルを渡します。
2. **`rateLimit({ count: 5, perMs: 1_000 })`** はこの実行内での上流の読み取り開始回数を、1,000 ms のスライディングウィンドウあたり最大5回に制限します。
3. **`map(mapper, { concurrency: 5, capacity: 5, ordered: false })`** は最大5件の mapper コールバックを並行実行し、使用中スロットの合計（読み取り中 + 実行中 + 出力待ちバッファ）を5枠以下に制限した上で、完了した順に出力します。
4. **`take(20)`** は最大20件の値を出力した時点でパイプラインを止め、20件目を下流へ渡す前に上流ステージのクローズ処理を開始します。
5. **`toArray(texts, { signal })`** はパイプラインからの読み取りを開始し、`controller.signal` を全ステージとコールバックへ伝播させ、上流のクリーンアップが完了するまで待ってから配列を返します。

## 遅延評価（Lazy Evaluation）

ストリームの生成、`.pipe()` によるオペレータの連結、`iterate(stream)` によるイテレータの取得はすべて遅延評価されます。最初の `next()` が呼ばれるまで、上流イテレータのオープンもタイマーやコールバックの実行も行われません。

- パイプラインに `take(0)` が含まれる場合や、最初の読み取り前に外部の `AbortSignal` がすでに abort されている場合は、上流ソースを一切開かずに即座に終了します。
- `Stream` を消費するたびに、ステージの状態・並行スロット・要素インデックス（`index`）が独立した新しい実行が作られます。

## メソッドチェーンとスタンドアロン関数

すべての終端コンシューマは、`Stream<T>` のメソッドとしても、通常の同期 `Iterable<T>` や `AsyncIterable<T>` を直接受け取るスタンドアロン関数としても利用できます：

| 目的                                              | `Stream<T>` メソッド                  | スタンドアロン関数                           |
| ------------------------------------------------- | ------------------------------------- | -------------------------------------------- |
| 結果をメモリに溜めず1件ずつ逐次処理する           | `stream.forEach(effect, { signal })`  | `forEach(source, effect, { signal })`        |
| 有限ストリームの全要素を配列に集める              | `stream.toArray({ signal })`          | `toArray(source, { signal })`                |
| 1つの累積結果に集約する                           | `stream.reduce(fn, init, { signal })` | `reduce(source, fn, init, { signal })`       |
| 条件に合う最初の1件を見つけて早期終了する         | `stream.find(predicate, { signal })`  | `find(source, predicate, { signal })`        |
| 真偽値判定が確定した時点で早期終了する            | `stream.some(pred)` / `.every(pred)`  | `some(source, pred)` / `every(source, pred)` |
| `cancel()` と `closed` を使って手動で反復制御する | `stream.iterate({ signal })`          | `iterate(source, { signal })`                |
| 標準のループ構文で消費する                        | `for await (const x of stream)`       | `for await (const x of from(source))`        |

使い分けとコード例は[終端処理](./consumers.md)を参照してください。

`stream.pipe(...ops)` とスタンドアロンの `pipe(source, ...ops)` はどちらも1回の呼び出しで最大8個のオペレータまで型推論を行います。9個以上つなぐ場合は `.pipe()` をチェーンしてください。また、要素の型に依存しないオペレータ（`take`、`skip`、`chunks`、時間制御オペレータ、`finalize`）は変数に代入してもジェネリック型を維持するため、`const firstFive = take(5)` のように異なる要素型のストリーム間で再利用できます。

## 再実行性とリソース管理：`from` と `defer`

`from(source)` は元の入力ソースの再実行性（Replayability）をそのまま引き継ぎます：

- 配列や `Set`（`from([1, 2, 3])`）をラップしたストリームは、何度でも繰り返し実行できます。
- ジェネレータ関数の戻り値（`from(myGenerator())`）はすでに生成された単一イテレータであるため、1回消費すると枯渇し、2回目の実行では空になります。

[`defer`](/mogami-stream/api/functions/defer.html){target="_self"} を使うと、実行ごとに新しいソースを作り、その実行の `AbortSignal` を受け取れます。ただし、ファクトリで開いたリソースを `defer` が自動で閉じるわけではありません。

### リソースの所有権と解放

次の関数は、アプリケーション側で用意した `openCursor` を受け取ります。カーソルには行の読み取りと非同期のクローズ処理を持たせています。実際の DB クライアントに合わせて実装してください。

<<< @/examples/resources.ts

消費するたびに、最初の読み取りで新しいカーソルを開きます。この非同期ジェネレータがカーソルを所有し、正常終了・失敗・`take(10)` による早期終了のいずれでも `finally` で解放します。2回の実行は逐次行われ、それぞれの `toArray` はカーソルのクローズ完了まで待ちます。signal が abort された場合も実行を停止し、2回目の開始前に abort 済みなら新しいカーソルは開きません。

ライブラリは後処理で、返されたイテレータの `return()` を呼んで完了を待ちます。別のセッション・ファイルハンドル・ソケットまで閉じる必要があるかは判断できません。上の例のように取得と解放をジェネレータ内の `try/finally` にまとめるか、適切な解放処理を [`finalize`](./lifecycle.md) に登録してください。

カーソル内部の I/O にも実行の `signal` を渡してください。DB ドライバが対応していれば、行の読み取りにも渡します。進行中の読み取りがキャンセルを無視する場合は、その処理が決着してから `finally` を実行します。また、`openCursor` がカーソルを返す前に失敗した場合、取得途中のリソースはアダプタ側で解放する必要があります。

続いて、[並行数・容量・需要](./concurrency.md)でスロット管理の仕組みを、または[キャンセルと後処理](./lifecycle.md)でクリーンアップの仕様を確認してください。
