import {
  from,
  of,
  filter,
  map,
  take,
  skip,
  zip,
  zipLatest,
  pipe,
  flatMap,
  concatMap,
  chunks,
  flatten,
  concat,
  reduce,
  forEach,
  find,
  some,
  every,
  scan,
  fromTimeline,
  mapSettled,
  type SettledResult,
  type MapOptions,
  type MapContext,
  type TimelineEvent,
  type Stream,
  type StreamIterator,
} from "../../dist/index.js";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
    ? true
    : false;
type Expect<T extends true> = T;
type Value<T> = T extends Stream<infer U> ? U : never;

const mapped = of(1).pipe(map(async (value) => `${value}`));
export type AwaitedMap = Expect<Equal<Value<typeof mapped>, string>>;
const iterator = mapped.iterate({ signal: new AbortController().signal });
export type MethodIterator = Expect<
  Equal<typeof iterator, StreamIterator<string>>
>;
const defaultIterator = of(1).iterate();
export type DefaultMethodIterator = Expect<
  Equal<typeof defaultIterator, StreamIterator<number>>
>;
const collected = mapped.toArray({ signal: new AbortController().signal });
export type Collected = Expect<Equal<typeof collected, Promise<string[]>>>;
const narrowed = from<number | string>([1, "a"]).pipe(
  filter((value): value is number => typeof value === "number"),
);
export type NarrowedFilter = Expect<Equal<Value<typeof narrowed>, number>>;
const promised = of(Promise.resolve(1)).pipe(take(1));
const collectedPromises = promised.toArray();
export type CollectedPromises = Expect<
  Equal<typeof collectedPromises, Promise<Promise<number>[]>>
>;
export type PromiseValue = Expect<
  Equal<Value<typeof promised>, Promise<number>>
>;
const zipped = zip(of(1), of("a"), of(true));
export type ZippedTuple = Expect<
  Equal<Value<typeof zipped>, [number, string, boolean]>
>;
const latest = zipLatest([of(1), of("a")]);
export type LatestTuple = Expect<Equal<Value<typeof latest>, [number, string]>>;
const partial = zipLatest([of(1), of("a")], { partial: true });
export type PartialTuple = Expect<
  Equal<Value<typeof partial>, [number | undefined, string | undefined]>
>;
const higher = of(1).pipe(flatMap((value) => [value.toString()]));
export type FlatMapped = Expect<Equal<Value<typeof higher>, string>>;
const concatenated = of(1).pipe(concatMap((value) => [value.toString()]));
export type ConcatMapped = Expect<Equal<Value<typeof concatenated>, string>>;
const flattened = of([1, 2]).pipe(flatten());
const concatValues = of([1, 2]).pipe(concat());
export type Flattened = Expect<Equal<Value<typeof flattened>, number>>;
export type Concatenated = Expect<Equal<Value<typeof concatValues>, number>>;
const chunked = of(1).pipe(chunks(2), skip(0));
export type Chunked = Expect<Equal<Value<typeof chunked>, number[]>>;
const reduced = reduce(of(1), async (sum, value) => sum + value, 0);
export type Reduced = Expect<Equal<typeof reduced, Promise<number>>>;
const methodReduced = of(1).reduce(
  async (text, value, { index, signal }) => {
    const typedValue: number = value;
    const typedIndex: number = index;
    const typedSignal: AbortSignal = signal;
    return text + typedValue + typedIndex + typedSignal.aborted;
  },
  "",
  { signal: new AbortController().signal },
);
export type MethodReduced = Expect<
  Equal<typeof methodReduced, Promise<string>>
>;

// Every advertised composition length is checked with a changing intermediate type.
const a = map((value: number) => String(value));
const b = map((value: string) => value.length);
const chain0 = of(1).pipe();
export type Chain0 = Expect<Equal<Value<typeof chain0>, number>>;
const piped0 = pipe([1]);
export type Piped0 = Expect<Equal<Value<typeof piped0>, number>>;
const chain1 = of(1).pipe(a);
export type Chain1 = Expect<Equal<Value<typeof chain1>, string>>;
const piped1 = pipe([1], a);
export type Piped1 = Expect<Equal<Value<typeof piped1>, string>>;
const chain2 = of(1).pipe(a, b);
export type Chain2 = Expect<Equal<Value<typeof chain2>, number>>;
const piped2 = pipe([1], a, b);
export type Piped2 = Expect<Equal<Value<typeof piped2>, number>>;
const chain3 = of(1).pipe(a, b, a);
export type Chain3 = Expect<Equal<Value<typeof chain3>, string>>;
const piped3 = pipe([1], a, b, a);
export type Piped3 = Expect<Equal<Value<typeof piped3>, string>>;
const chain4 = of(1).pipe(a, b, a, b);
export type Chain4 = Expect<Equal<Value<typeof chain4>, number>>;
const piped4 = pipe([1], a, b, a, b);
export type Piped4 = Expect<Equal<Value<typeof piped4>, number>>;
const chain5 = of(1).pipe(a, b, a, b, a);
export type Chain5 = Expect<Equal<Value<typeof chain5>, string>>;
const piped5 = pipe([1], a, b, a, b, a);
export type Piped5 = Expect<Equal<Value<typeof piped5>, string>>;
const chain6 = of(1).pipe(a, b, a, b, a, b);
export type Chain6 = Expect<Equal<Value<typeof chain6>, number>>;
const piped6 = pipe([1], a, b, a, b, a, b);
export type Piped6 = Expect<Equal<Value<typeof piped6>, number>>;
const chain7 = of(1).pipe(a, b, a, b, a, b, a);
export type Chain7 = Expect<Equal<Value<typeof chain7>, string>>;
const piped7 = pipe([1], a, b, a, b, a, b, a);
export type Piped7 = Expect<Equal<Value<typeof piped7>, string>>;
const chain8 = of(1).pipe(a, b, a, b, a, b, a, b);
export type Chain8 = Expect<Equal<Value<typeof chain8>, number>>;
const piped8 = pipe([1], a, b, a, b, a, b, a, b);
export type Piped8 = Expect<Equal<Value<typeof piped8>, number>>;

// @ts-expect-error Pipeline input/output types must agree.
of(1).pipe(map((value: string) => value.length));
// @ts-expect-error Child factories must synchronously return an iterable.
of(1).pipe(flatMap(async (value) => [value]));
// @ts-expect-error An initial accumulator is required.
reduce(of(1), (a: number, b: number) => a + b);
// @ts-expect-error An initial accumulator is required for the method too.
of(1).reduce((a: number, b: number) => a + b);
// @ts-expect-error Reducer results must match the accumulator type.
of(1).reduce((text: string) => text.length, "");

const visited = forEach([1, 2], async (value, { index, signal }) => {
  const n: number = value + index;
  return signal.aborted ? n : 0;
});
export type Visited = Expect<Equal<typeof visited, Promise<void>>>;
const methodVisited = of(1).forEach(() => Promise.resolve("ignored"));
export type MethodVisited = Expect<Equal<typeof methodVisited, Promise<void>>>;
const searched = find(
  from<number | string>([1, "a"]),
  (value): value is number => typeof value === "number",
);
export type Searched = Expect<
  Equal<typeof searched, Promise<number | undefined>>
>;
const methodSearched = from<number | string>([1, "a"]).find(
  (value): value is string => typeof value === "string",
);
export type MethodSearched = Expect<
  Equal<typeof methodSearched, Promise<string | undefined>>
>;
const asyncSearched = of(1).find(async (value) => value > 0);
export type AsyncSearched = Expect<
  Equal<typeof asyncSearched, Promise<number | undefined>>
>;
const anyMatch = some([1], async (value) => value > 0);
const allMatch = every([1], (value) => value > 0);
const methodAny = of(1).some((value) => value > 0);
const methodAll = of(1).every(async (value) => value > 0);
export type BooleanTerminals = Expect<
  Equal<
    typeof anyMatch | typeof allMatch | typeof methodAny | typeof methodAll,
    Promise<boolean>
  >
>;
const scanned = of(1).pipe(
  scan(
    async (text, value, { index, signal }) =>
      text + value + index + signal.aborted,
    "",
  ),
);
export type Scanned = Expect<Equal<Value<typeof scanned>, string>>;
const schedule: readonly TimelineEvent<number>[] = [
  { at: 0, kind: "value", value: 1 },
  { at: 10, kind: "complete" },
];
const timed = fromTimeline(schedule);
export type Timed = Expect<Equal<Value<typeof timed>, number>>;
const promisedTimeline = fromTimeline([
  { at: 0, kind: "value", value: Promise.resolve(1) },
]);
export type PromisedTimeline = Expect<
  Equal<Value<typeof promisedTimeline>, Promise<number>>
>;
// @ts-expect-error Scan requires an initial accumulator.
scan((sum: number, value: number) => sum + value);
// @ts-expect-error Scan results must match the accumulator type.
of(1).pipe(scan((text: string) => text.length, ""));
// @ts-expect-error A value event requires a value.
fromTimeline([{ at: 0, kind: "value" }]);
// @ts-expect-error Complete events cannot also carry an error.
fromTimeline([{ at: 0, kind: "complete", error: new Error() }]);

const retryOptions: MapOptions = {
  concurrency: 3,
  capacity: 4,
  ordered: false,
  retry: {
    retries: 2,
    shouldRetry: async (
      _error: unknown,
      { attempt, index, signal }: MapContext,
    ) => attempt + index > 0 && !signal.aborted,
    delayMs: (_error, { attempt }) => attempt * 10,
  },
};
const retried = of(1).pipe(
  map(
    async (value, { attempt, index, signal }) =>
      String(value + attempt + index) + signal.aborted,
    retryOptions,
  ),
);
export type Retried = Expect<Equal<Value<typeof retried>, string>>;
const settled = of(1).pipe(mapSettled(async (value) => String(value)));
export type Settled = Expect<
  Equal<Value<typeof settled>, SettledResult<string>>
>;
settled.pipe(
  map((result) => {
    if (result.status === "fulfilled") {
      const value: string = result.value;
      // @ts-expect-error Fulfilled results have no reason.
      result.reason;
      return value;
    }
    const reason: unknown = result.reason;
    // @ts-expect-error Rejected results have no value.
    result.value;
    return reason;
  }),
);
// @ts-expect-error Retry options require an explicit retry budget.
map((value: number) => value, { retry: {} });
map((value: number) => value, {
  // @ts-expect-error Retry policy must return a boolean or PromiseLike<boolean>.
  retry: { retries: 1, shouldRetry: () => "yes" },
});
map((value: number) => value, {
  // @ts-expect-error Delay callbacks return milliseconds synchronously.
  retry: { retries: 1, delayMs: async () => 1 },
});
// @ts-expect-error Retry settings belong inside retry.
map((value: number) => value, { retries: 1 });
const settledRetry = of(1).pipe(
  mapSettled(
    async (value, { attempt }) => String(value + attempt),
    retryOptions,
  ),
);
export type SettledRetry = Expect<
  Equal<Value<typeof settledRetry>, SettledResult<string>>
>;

// @ts-expect-error mapRetry was removed in favor of map's retry option.
import { mapRetry } from "../../dist/index.js";
