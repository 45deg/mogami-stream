# Web Streams

`fromReadableStream` で fetch の本文を mogami-stream に取り込み、`toReadableStream` で処理結果を HTTP レスポンスとして返せます。どちらも `mogami-stream` と `mogami-stream/core` からインポートできます。ストリームには `.toReadableStream(options)` メソッドもあります。

```ts
import { fromReadableStream } from "mogami-stream";

export function relay(response: Response, signal: AbortSignal): Response {
  if (!response.body) return new Response(null, { status: response.status });
  const body = fromReadableStream(response.body)
    .map((chunk) => chunk)
    .toReadableStream({ signal });
  return new Response(body, { status: response.status });
}
```

HTTP 本文には `Uint8Array` などのバイト列を渡します。変換時にはチャンクの値を保持し、オブジェクトや文字列のエンコードは行いません。出力は通常のストリームで、BYOB に対応したバイトストリームではありません。

## 所有権とキャンセル

`fromReadableStream(source)` は最初の読み取りで reader を取得します。最後まで読み取った場合は、入力をキャンセルせずにロックを解放します。`take` などによる早期終了では reader をキャンセルし、待機中の処理とキャンセルが完了してからロックを解放します。読み取りが失敗した場合もロックを解放します。入力がすでにロックされている場合は消費開始時に失敗し、既存の reader がロックを保持します。

入力は一度だけ消費できます。ラップしても再生可能にはなりません。実行ごとに新しいデータが必要な場合は、`defer` のファクトリーで入力を作成してください。

`toReadableStream(source, { signal })` は一つの実行を作ります。出力をキャンセルすると、その理由を上流へ伝え、クリーンアップの完了を待ちます。キャンセル中にクリーンアップが失敗すると、キャンセルの Promise は `AggregateError` で reject されます。処理の失敗は主原因で出力をエラー状態にします。返される Web Stream からは、実行の `CloseReport` や抑制されたエラーの全体を参照できません。

外部シグナルによる中断は、出力の読み取りを待っている間も実行を停止します。待機中の読み取りは中断時に reject され、読み取りのない出力はクリーンアップ完了時にエラー状態になります。キャンセルに応答しない処理があると、クリーンアップの完了が遅れることがあります。`getReader()` を使う場合は、消費後に出力側の reader のロックを自分で解放してください。

## 読み取り要求とバッファ

入力の変換処理はキューを追加せず、要求されたときだけ `reader.read()` を呼びます。出力は `highWaterMark: 0` を使います。生成時や reader の取得時には読み取りを開始せず、同時に進める上流の読み取りは一つまでです。Web 側で複数の読み取りが待機している場合は順に処理します。最初の pull より前にキャンセルした場合、入力は開きません。

元の Web Stream は独自のキューを持つ場合があります。また、並行オペレータは設定された容量の範囲内で、進行中の処理と結果を保持します。出力の high-water mark をゼロにしても、これらのバッファや上限は変わりません。
