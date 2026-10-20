# タイムラインによるテスト

[`fromTimeline`](../operators.md#operator-fromtimeline) で値を出力する時刻やエラーを発生させる時刻を指定し、結果を検証します。時刻は最初の読み取りからの経過時間をミリ秒で指定します。

<<< @/examples/testing.ts#sources

Node.js の `assert` を使った検証例です。

<<< @/examples/testing.ts#assertions

実時間で待機する pull 型のソースです。下流が読み取っていない間はタイマーが動かず、完了やエラーも次の読み取りまで通知しません。指定時刻を過ぎてから要求したイベントは直ちに処理します。

時刻には0以上の有限値を指定して昇順に並べ、同じ時刻のイベントは配列順に処理します。完了・エラーのイベントは末尾にだけ置け、省略すると最後の値の後に完了し、不正な指定は構築時に例外になります。詳しくは [fromTimeline](/mogami-stream/api/functions/fromTimeline.html){target="_self"} を参照してください。
