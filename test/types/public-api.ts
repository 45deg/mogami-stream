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
  type Stream,
} from "../../dist/index.js";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
    ? true
    : false;
type Expect<T extends true> = T;
type Value<T> = T extends Stream<infer U> ? U : never;

const mapped = of(1).pipe(map(async (value) => `${value}`));
export type AwaitedMap = Expect<Equal<Value<typeof mapped>, string>>;
const narrowed = from<number | string>([1, "a"]).pipe(
  filter((value): value is number => typeof value === "number"),
);
export type NarrowedFilter = Expect<Equal<Value<typeof narrowed>, number>>;
const promised = of(Promise.resolve(1)).pipe(take(1));
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
