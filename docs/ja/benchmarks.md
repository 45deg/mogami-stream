# ベンチマーク

入力10万件で、非同期反復とオペレータのオーバーヘッドを比較します。グラフはウォームアップ5回の後に7回測定した所要時間の中央値（秒）で、短いほど高速です。目盛りはグラフごとに異なります。

![逐次非同期 map、並行・入力順の map、13ステージの map/filter パイプライン。Node.js と Bun の所要時間（秒）。](/benchmarks.svg)

Node.js v24.22.0 / Bun v1.4.3 · Apple M1 Pro · 通常 GC。

| グラフ                                         | 処理内容                                                       |
| ---------------------------------------------- | -------------------------------------------------------------- |
| Sequential async map（逐次変換）               | `concurrency: 1`、`capacity: 1`                                |
| Concurrent ordered map（並行変換・入力順出力） | `concurrency: 8`、`capacity: 16`                               |
| 13-stage map/filter pipeline                   | ID の取り出しに続き、加算と97の倍数を除外するフィルタを6回適用 |

入力は非同期ジェネレータから取得し、map コールバックは `Promise.resolve()` を await します。タイマーやネットワークの待機時間は含みません。

## 再現手順

Node.js、Bun、pnpm、uv が必要です。

```sh
pnpm install --frozen-lockfile
pnpm bench:svg

# 保存済みの結果を再描画:
pnpm bench:svg --reuse
```

掲載グラフは既定の通常 GC（`--gc=none`）で測定しています。個別の測定や JSON 出力は[測定方法と CLI オプション](https://github.com/45deg/mogami-stream/blob/main/benchmarks/README.md)を参照してください。

`pnpm bench:fork` は、同期ソースと Promise を返すソースを使い、同じペースで読む分岐、読み取りが遅れる分岐、切り離した分岐がある場合の `fork` を測定します。各分岐の出力順と容量の上限も検証します。独立したプロセスでビルド間を比較する方法と、所要時間・CPU 時間の生データについては、[fork のベンチマーク手順](https://github.com/45deg/mogami-stream/blob/main/benchmarks/README.md#fork)を参照してください。
