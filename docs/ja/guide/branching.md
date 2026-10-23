# ストリームを分岐する

`broadcast` と `route` は、1 回のソース実行から複数の分岐を作り、分岐内の入力順を保ちます。

| API                       | 値の送り先                        |
| ------------------------- | --------------------------------- |
| [`broadcast`](#broadcast) | 全分岐に同じ値を配る              |
| [`route`](#route)         | 条件で選んだ最大 1 つの分岐に送る |

`branches` に分岐数を渡すと配列、文字列キーの配列を渡すと分岐名をキーとするオブジェクトを返します。関数形式と fluent 形式の両方で使えます。

## broadcast

<<< @/examples/operators/broadcast.ts#example

値はコピーせず、同じ参照を渡します。共有する値は変更しないでください。

## route

`select` で送り先の番号かキーを返します。判定は入力順に 1 項目ずつ実行し、非同期関数も使えます。

<<< @/examples/operators/route.ts#example

### 条件関数と型ガード

`select` の代わりに、`branches` に条件関数を渡せます。`Object.keys(branches)` の順に調べ、最初に一致した分岐に送ります。

```ts
const group = from(events).route({
  branches: {
    image: (event): event is ImageEvent => event.kind === "image",
    text: (event): event is TextEvent => event.kind === "text",
  },
  capacity: 8,
});
```

同期の型ガードを使うと、その分岐の型を絞れます。通常の真偽値や Promise を返す条件、および `default` の分岐は元の入力型になります。

### default と fallback

条件に一致しない、または選択先が離脱済みなら `default` に送ります。離脱した分岐に一致した場合も、次の条件へは進みません。`default` も使えない場合は `fallback` に従います。

| 設定                           | 動作                      |
| ------------------------------ | ------------------------- |
| `default: "other"`             | 代わりに `other` へ送る   |
| `fallback: "ignore"`（既定値） | 項目を捨てて続行          |
| `fallback: "error"`            | `RouteError` で全体が失敗 |

不正な送り先や判定中の例外・拒否は、`fallback` に関係なく全体の失敗になります。一度送った値は、後で分岐が離脱しても送り直しません。

`route` も値をコピーしません。入力に同じオブジェクトが複数含まれていれば、分岐間で参照が重なることはあります。

## 容量と読み取り

`capacity` は分岐ごとの項目数の上限です。

| API         | 上限に含むもの                                            |
| ----------- | --------------------------------------------------------- |
| `broadcast` | キュー内の値 + 読み取り予約                               |
| `route`     | キュー内の値。別に、全体で読み取り・判定・送信待ちの 1 枠 |

`broadcast` はどれかの分岐、`route` は選択先が満杯になると上流を止めます。`route` も空いている分岐へは振り替えず、後続の入力を待たせます。要求がない間は先読みしません。

すべての分岐を並行して読み取り、使わない分岐は `detach()` してください。読み取り前の分岐も上流を止めることがあります。容量はバイト数や下流の処理量を制限しません。

## 終了とキャンセル

各分岐を消費できるのは 1 回だけです。`mogami-stream` の分岐は fluent オペレータを使え、`mogami-stream/core` の分岐は `from(branch)` で包みます。

- `branch.detach()` や `for await` の `break` は、その分岐を終了し、未読の値を捨てます。他の分岐は続行します。
- `branch.cancel(reason)` はその分岐、`group.cancel(reason)` は全分岐をキャンセルします。全分岐が離脱すると上流も停止します。
- `group.close()` は全分岐を離脱させ、実行中の読み取り・判定と後処理を待ちます。

`branch.closed` は分岐単体、`group.closed` は全分岐と共有処理の終了を待ちます。後処理の失敗は `report.source` で確認します。[キャンセルと後処理](./lifecycle.md)も参照してください。

[broadcast](/mogami-stream/api/functions/broadcast.html){target="_self"} · [route](/mogami-stream/api/functions/route.html){target="_self"}
