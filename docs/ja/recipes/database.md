# DB バッチ書き込み

`chunks(100)` で最大100件ずつにまとめ、各書き込みの完了を待って次のバッチへ進みます。時間でも区切りたい場合は `bufferTime` を使います。

<<< @/examples/database.ts

正常終了時は端数のバッチも書き込みます。失敗やキャンセル時は未完了のバッチを出力しません。トランザクションや再試行時の重複対策は DB 側で扱います。

[chunks](/mogami-stream/api/functions/chunks.html){target="_self"} · [bufferTime](/mogami-stream/api/functions/bufferTime.html){target="_self"}
