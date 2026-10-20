# イベントループへ処理を譲る

同期処理が長く続くパイプラインでは、[`tap`](../operators.md#operator-tap) のコールバックでイベントループへ処理を譲ることで、タイマーや I/O、タイマー起点のキャンセルを進められます。Mogami は自動では待機を挿入しないため、既存のオペレータを組み合わせて以下のヘルパーを定義します。

各ヘルパーと一緒に、`mogami-stream` から [`defer`](../operators.md#operator-defer)、[`from`](../operators.md#operator-from)、[`tap`](../operators.md#operator-tap) を import してください。

## 使用例

次の `yieldEvery` はこのページで定義するヘルパーで、1,024件ごとに処理を譲ります。

```js
import { filter, range } from "mogami-stream";

const controller = new AbortController();
await range(0, 100_000)
  .pipe(
    yieldEvery(1_024),
    filter((value) => value % 2 === 0),
  )
  .forEach((value) => console.log(value), { signal: controller.signal });
```

## キャンセル可能な待機

`await Promise.resolve()` が処理を譲る先はマイクロタスク（microtask）のみです。遅延ゼロのタイマーを使うと、ホストがタイマーや I/O を処理できます。待機が終わるとタイマーと abort リスナーを解除します。

<<< @/examples/event-loop.mjs#wait{js}

## 一定件数ごとに処理を譲る

<<< @/examples/event-loop.mjs#count{js}

[`tap`](../operators.md#operator-tap) はコールバックを逐次 await し、Promise を値として持つ要素もそのまま下流へ渡します。`index` は実行ごとに 0 から始まるため、同じオペレータを再利用できます。

N 番目の要素を上流から読み取ってから待機し、待機完了後に下流へ渡します。下流から読み取り要求がない間は、タイマーも設定されません。

ほぼすべての要素を除外する [`filter`](../operators.md#operator-filter) など、大量に読み取っても出力しない可能性があるステージより前に配置してください。同じ 1,024 件でもコールバックの重さによって占有時間は大きく異なるため、1件あたりの処理コストに合わせて件数を調整します。

## 経過時間で処理を譲る

単調増加クロックが処理中も進むホスト環境では、要素ごとに経過時間を判定する方法も使えます。[`defer`](../operators.md#operator-defer) の内側で時刻を保持することで、実行ごとの状態を分離します。

<<< @/examples/event-loop.mjs#time{js}

`yieldEvery(1_024)` の代わりに `yieldAfter(4)` を使い、前回の yield 以降の上流処理時間と下流の待機時間を含めて判定します。このステージに要素が到着した時点でしか判定しないため、1回の重いコールバックで指定時間を超えることがあります。Cloudflare Workers では I/O 後にしか `performance.now()` が進まないため、件数ベースの `yieldEvery` を使ってください。

## 間隔の選び方

遅延ゼロのタイマーでも、実行環境によって最小待機時間が設けられる場合があります。待機の間隔を短くするとタイマーが動く機会は増えますが、全体の処理時間も長くなるため、要素ごとの待機は避けて負荷に応じた間隔を選んでください。

なお、すでに実行中の同期コールバックを途中から中断することはできないため、1回あたりの処理が重い場合はより小さな単位に分割してください。ソースやコールバックがすでに I/O を await している場合は、明示的な待機を追加する前に計測して確認してください。
