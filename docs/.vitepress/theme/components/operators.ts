export type Locale = "en" | "ja";
export const operatorGroups = {
  sources: { en: "Sources", ja: "ソース" },
  transform: { en: "Transform", ja: "変換" },
  selection: { en: "Selection & batching", ja: "選択・分割" },
  combination: { en: "Combination", ja: "合成" },
  timing: { en: "Timing", ja: "時間制御" },
  resilience: { en: "Error recovery", ja: "エラー回復" },
  lifecycle: { en: "Side effects & cleanup", ja: "副作用・後処理" },
  consumers: { en: "Consumers", ja: "終端処理" },
};

type Group = keyof typeof operatorGroups;
type Entry = {
  id: string;
  name: string;
  group: Group;
  description: Record<Locale, string>;
  page: string;
};

function entry(
  name: string,
  group: Group,
  en: string,
  ja: string,
  page = `functions/${name}.html`,
): Entry {
  return {
    id: name
      .replace(/[^a-z0-9]+/gi, "-")
      .replace(/-$/, "")
      .toLowerCase(),
    name,
    group,
    description: { en, ja },
    page,
  };
}

export const operators = [
  entry(
    "from",
    "sources",
    "Wrap a synchronous or asynchronous iterable into a Stream, preserving its replayability and Promise values.",
    "同期・非同期の Iterable を Stream にラップする。再実行可能かどうかや Promise 要素はそのまま維持される。",
  ),
  entry(
    "defer",
    "sources",
    "Create a fresh source on the first read of each execution; supports async factory setup and AbortSignal.",
    "実行ごとの初回読み取り時に新しいソースを生成する。非同期の初期化と AbortSignal による中断に対応。",
  ),
  entry(
    "of",
    "sources",
    "Create a replayable stream that emits the provided values in order.",
    "指定した値を順番に出力する、再実行可能なストリームを生成する。",
  ),
  entry(
    "empty",
    "sources",
    "Create a stream that completes immediately on the first read without emitting values.",
    "値を一切出力せず、初回読み取り時に即座に完了するストリームを生成する。",
  ),
  entry(
    "range",
    "sources",
    "Emit safe integers from start (inclusive) to end (exclusive) with a configurable step.",
    "start から end（終端を含まない）までの安全な整数列を指定ステップで出力する。",
  ),
  entry(
    "fromCallback",
    "sources",
    "Bridge push-based callbacks into a pull stream using a bounded queue and configurable overflow policy.",
    "コールバック型のイベントソースを、上限付きキューとオーバーフロー方針（error / drop-oldest / drop-newest）でストリームに変換する。",
  ),
  entry(
    "fromEvent",
    "sources",
    "Listen to EventTarget events through a bounded queue with explicit overflow handling.",
    "EventTarget のイベントを上限付きキューで受信し、ストリームとして扱う。",
  ),
  entry(
    "fromTimeline",
    "sources",
    "Replay a scripted schedule of values, completion, or errors at millisecond offsets for testing and mocking.",
    "初回読み取りからのミリ秒オフセットで値・完了・エラーをスケジュール再生するテスト・モック用ソース。",
  ),
  entry(
    "map",
    "transform",
    "Transform items with bounded concurrency, capacity limits, output ordering, and optional per-item retry policies.",
    "並行実行数（concurrency）・容量上限（capacity）・出力順序・項目単位のリトライを指定して要素を非同期変換する。",
  ),
  entry(
    "mapSettled",
    "transform",
    "Transform items like map, emitting { status: 'fulfilled', value } or { status: 'rejected', reason } without halting on mapper failures.",
    "map と同じ並行制御で変換を行い、各要素の成功・失敗を SettledResult オブジェクトとしてストリームを止めずに出力する。",
  ),
  entry(
    "filterMap",
    "transform",
    "Map items sequentially and drop only null and undefined results (preserving 0, false, and empty strings).",
    "要素を逐次変換し、結果が null または undefined の場合のみ除外する（0、false、空文字は保持）。",
  ),
  entry(
    "scan",
    "transform",
    "Fold items sequentially from an initial value and emit the updated accumulator after every step.",
    "初期値から要素を逐次集約し、ステップごとの中間アキュムレータを順次出力する。",
  ),
  entry(
    "filter",
    "selection",
    "Keep items satisfying a sequential sync or async predicate; synchronous type guards narrow the element type.",
    "同期・非同期の条件関数を逐次評価して一致する要素のみを残す。同期の型ガード関数による型の絞り込みに対応。",
  ),
  entry(
    "take",
    "selection",
    "Emit at most the first n items and begin closing upstream before delivering the nth item.",
    "先頭から最大 n 件を取得し、n 件目を下流に渡す前に上流のクローズ処理を開始する。",
  ),
  entry(
    "skip",
    "selection",
    "Discard the first n items and emit all remaining items.",
    "先頭の n 件を読み飛ばし、残りの要素をすべて出力する。",
  ),
  entry(
    "takeWhile",
    "selection",
    "Emit leading items while the predicate holds, closing upstream as soon as it returns false.",
    "条件関数が true を返す間だけ要素を出力し、false になった時点で上流を閉じて完了する。",
  ),
  entry(
    "skipWhile",
    "selection",
    "Discard leading items while the predicate holds, then emit the first non-matching item and all subsequent items.",
    "条件関数が true を返す先頭区間を読み飛ばし、最初に false となった要素以降は条件評価なしでそのまま出力する。",
  ),
  entry(
    "chunks",
    "selection",
    "Group items into arrays of up to size elements, flushing any remaining partial batch on normal completion.",
    "要素を最大 size 件ずつの配列にバッチ化する。正常完了時には端数のバッチも出力される。",
  ),
  entry(
    "dedupe",
    "selection",
    "Drop consecutive items whose keys match the immediately preceding emitted key via Object.is.",
    "直前に出力した要素と Object.is でキーが一致する連続重複を除外する（ストリーム全体の重複排除ではない）。",
  ),
  entry(
    "chain",
    "combination",
    "Concatenate multiple sources sequentially, waiting for each source to close before opening the next.",
    "複数のソースを順番に連結し、前のソースのクリーンアップ完了を待ってから次のソースを開く。",
  ),
  entry(
    "merge",
    "combination",
    "Interleave multiple sources concurrently with one read slot per source and round-robin selection among ready sources.",
    "複数のソースをソースあたり1スロットで並行して読み取り、準備できたソースからラウンドロビン順に出力する。",
  ),
  entry(
    "zip",
    "combination",
    "Pair corresponding items into tuples, completing and closing remaining sources as soon as the shortest source ends.",
    "各ソースの同じ位置の要素をタプルにまとめ、最短のソースが終了した時点で残りのソースも閉じて完了する。",
  ),
  entry(
    "zipLatest",
    "combination",
    "Emit tuples of the latest values across sources; waits for all sources to emit once unless partial: true is set.",
    "各ソースの最新値をタプルとして出力する。既定では全ソースの初回出力を待ち、partial: true なら未出力スロットを undefined で埋める。",
  ),
  entry(
    "flatten",
    "combination",
    "Flatten a stream of child sources concurrently up to the configured concurrency limit.",
    "ストリームの要素として渡された子ストリームを、指定の concurrency 上限で並行して読み取り、1本にまとめる。",
  ),
  entry(
    "concat",
    "combination",
    "Flatten child sources strictly in child order while bounding active and waiting child slots.",
    "子ストリームの順序を厳密に保って平坦化する。先行する子が完了するまで後続の子のスロット上限も維持される。",
  ),
  entry(
    "flatMap",
    "combination",
    "Map items synchronously to child sources and flatten them concurrently.",
    "各要素を同期コールバックで子ストリームに変換し、並行して平坦化する（非同期セットアップには defer を併用）。",
  ),
  entry(
    "concatMap",
    "combination",
    "Map items synchronously to child sources and flatten them in input order.",
    "各要素を同期コールバックで子ストリームに変換し、入力順を保って平坦化する。",
  ),
  entry(
    "pipe",
    "combination",
    "Standalone helper to wrap a source in a Stream and compose up to eight typed operators from left to right.",
    "任意のソースを Stream に包み、最大8個の型付きオペレータを左から右へ合成するスタンドアロン関数。",
  ),
  entry(
    "rateLimit",
    "timing",
    "Allow at most count upstream read starts per perMs sliding window within a single execution.",
    "1つの実行内で、スライディングウィンドウ（perMs ミリ秒）あたりの上流読み取り開始回数を最大 count 回に制限する。",
  ),
  entry(
    "spaceout",
    "timing",
    "Emit the first item without added delay, then enforce at least intervalMs between subsequent emissions.",
    "最初の要素は遅延なしで通し、2件目以降の出力間隔を最低 intervalMs ミリ秒以上空ける。",
  ),
  entry(
    "delayStart",
    "timing",
    "Wait ms milliseconds on the first downstream read before opening upstream.",
    "最初の読み取り時に ms ミリ秒待機してから上流ソースを開く。",
  ),
  entry(
    "bufferTime",
    "timing",
    "Collect items into non-empty batches, flushing when the first item's windowMs timer expires or maxItems is reached.",
    "バッチの最初の要素から windowMs ミリ秒経過するか maxItems 件に達した時点で配列として出力する（空配列は出力しない）。",
  ),
  entry(
    "timeoutEach",
    "timing",
    "Fail with TimeoutError if a single upstream read takes longer than ms milliseconds (excluding downstream idle time).",
    "上流の1回の読み取りが ms ミリ秒を超えた場合に TimeoutError で失敗させる（下流のアイドル時間は含まない）。",
  ),
  entry(
    "deadline",
    "timing",
    "Fail with DeadlineExceededError once total elapsed stage time reaches durationMs (including downstream idle time).",
    "下流の待機時間も含め、ステージ開始からの総経過時間が durationMs ミリ秒に達した時点で DeadlineExceededError にする。",
  ),
  entry(
    "catchError",
    "resilience",
    "Wait for a failed upstream stage to finish cleanup, then switch once to the fallback source returned by handler.",
    "上流で発生したエラーを捕捉し、上流のクリーンアップ完了を待ってからハンドラが返す代替ソースへ1回だけ切り替える。",
  ),
  entry(
    "tap",
    "lifecycle",
    "Run a sequential sync or async side effect for each item before passing the item downstream.",
    "各要素を下流へそのまま渡す前に、同期・非同期の副作用コールバックを逐次実行して完了を待つ。",
  ),
  entry(
    "finalize",
    "lifecycle",
    "Run a sync or async teardown callback once when a started stage closes, after child stages and in-flight work finish.",
    "開始されたステージが閉じる際、子ステージと実行中タスクの完了後に一度だけクリーンアップ処理を実行し完了を待つ。",
  ),
  entry(
    "iterate",
    "consumers",
    "Acquire a controlled StreamIterator exposing cancel(reason?), return(), throw(error?), and the closed report promise.",
    "即時中断用の cancel() や終了レポート Promise の closed を備えた制御用 StreamIterator を取得する。",
  ),
  entry(
    "toArray",
    "consumers",
    "Consume the entire stream into an array after waiting for cleanup to complete.",
    "ストリームの全要素を配列に収集し、クリーンアップ完了を待ってから解決する。",
  ),
  entry(
    "reduce",
    "consumers",
    "Fold all values sequentially from an awaited initial value into a single final result after cleanup.",
    "初期値から全要素を逐次集約し、クリーンアップ完了後に最終結果を返す。",
  ),
  entry(
    "forEach",
    "consumers",
    "Run a sequential sync or async effect for every item without collecting results in memory.",
    "要素をメモリに溜め込まず、各要素に対して逐次処理を実行し、クリーンアップ完了まで待つ。",
  ),
  entry(
    "find",
    "consumers",
    "Return the first matching item (or undefined) and close upstream immediately; supports synchronous type guards.",
    "条件に一致する最初の要素が見つかり次第、上流を早期クローズしてその値を返す（一致なしは undefined）。型ガードに対応。",
  ),
  entry(
    "some",
    "consumers",
    "Short-circuit to true on the first matching item and close upstream early, or return false for empty/non-matching streams.",
    "条件を満たす要素が1つでも見つかれば即座に上流を閉じて true を返す（空または不一致なら false）。",
  ),
  entry(
    "every",
    "consumers",
    "Short-circuit to false on the first non-matching item and close upstream early, or return true if all items match.",
    "条件を満たさない要素が見つかり次第、即座に上流を閉じて false を返す（全要素が満たすか空なら true）。",
  ),
];
