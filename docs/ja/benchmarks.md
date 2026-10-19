# ベンチマーク

## 非同期反復とオペレータのオーバーヘッド比較

![Node.js と Bun での逐次非同期 map、並行・入力順の非同期 map、13段の非同期パイプラインの所要時間。短いほど高速。各パネルの目盛りは異なる。](/benchmarks.svg)

**測定環境:** 入力 100,000 件 · Node.js v26.5.0 / Bun v1.4.3 · Apple M1 Pro · ウォームアップ 5 回の後、7 回計測した中央値（単位は秒、短いほど高速、各パネルの横軸スケールは独立）。

| ベンチマークケース     | ワークロード内容                                                                      | 比較対象ライブラリ                     |
| ---------------------- | ------------------------------------------------------------------------------------- | -------------------------------------- |
| `async-map-serial`     | 逐次非同期 map（`concurrency: 1`、`capacity: 1`）                                     | Mogami, IxJS, iter-tools, pMapIterable |
| `async-map-concurrent` | 有界な並行非同期 map（`concurrency: 8`、`capacity: 16`、入力順出力）                  | Mogami, pMapIterable                   |
| `async-pipeline`       | 13段の非同期 `map` / `filter` パイプライン（インクリメントと `x % 97 !== 0` の6ペア） | Mogami, IxJS, iter-tools               |

入力は非同期ジェネレータから生成される数値 ID で、各 map コールバックは `await Promise.resolve()` の後に `{ id, value: id * 2 }` を返します。タイマー待機やネットワーク I/O を含めないことで、非同期イテレータのプロトコル処理とオペレータのスケジューリングオーバーヘッドを純粋に計測しています。

各「ワークロード × ライブラリ」のペアは、乱数シード（`--seed=42`）でシャッフルされた順序で毎回新しい子プロセスとして実行されます。モジュールの import、入力準備、ウォームアップ、手動 GC は計測区間外で行われます。各イテレーションでは、出力順序・値・件数・ソースのクローズ・全タスク完了、および `map` の並行数と先読み上限（コンシューマへ引き渡し中の1件を含む `capacity + 1`）がすべて検証されます。

## ベンチマークの再現手順

```sh
pnpm install --frozen-lockfile

# デフォルトのベンチマークスイート（10,000件）を実行し、JSON レポートを出力:
pnpm bench --json=/tmp/mogami-bench.json

# 特定のケースを 100,000 件で実行:
pnpm bench --case=async-pipeline --items=100000

# ビルド後、Node.js と Bun の両方で 100,000 件を測定して SVG チャートを生成（Bun と uv が必要）:
pnpm bench:svg

# 既存の JSON レポートを使って計測をスキップし、チャートだけを再描画:
pnpm bench:svg --reuse
```

すべての CLI オプション（`--items`、`--concurrency`、`--capacity`、`--warmup`、`--samples`、`--timeout-ms`）と JSON レポートの詳細は、リポジトリ内の [`benchmarks/README.md`](https://github.com/45deg/mogami-stream/blob/main/benchmarks/README.md) を参照してください。
