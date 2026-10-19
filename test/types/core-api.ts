import {
  from,
  of,
  map,
  take,
  chunks,
  catchError,
  pipe,
  chain,
  merge,
  zip,
  zipLatest,
  type Stream,
  type Operator,
} from "mogami-stream/core";
import {
  from as fluentFrom,
  map as fluentMap,
  type Stream as FluentStream,
} from "mogami-stream";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
    ? true
    : false;
type Expect<T extends true> = T;
type Value<T> = T extends Stream<infer U> ? U : never;

const mapped = from([1]).pipe(
  map(async (value) => String(value)),
  take(1),
);
export type Mapped = Expect<Equal<Value<typeof mapped>, string>>;
const savedTake = take(1);
const savedChunks = chunks(2);
const numbers = of(1).pipe(savedTake, savedChunks);
const strings = of("a").pipe(savedTake, savedChunks);
export type Numbers = Expect<Equal<Value<typeof numbers>, number[]>>;
export type Strings = Expect<Equal<Value<typeof strings>, string[]>>;
const recovered = pipe(
  of(1),
  catchError(() => ["fallback"]),
);
export type Recovered = Expect<Equal<Value<typeof recovered>, number | string>>;
const combined = chain([1], ["a"]);
const merged = merge([1], ["a"]);
const zipped = zip([1], ["a"]);
const partial = zipLatest([[1], ["a"]], { partial: true });
export type Combined = Expect<Equal<Value<typeof combined>, 1 | "a">>;
export type Merged = Expect<Equal<Value<typeof merged>, 1 | "a">>;
export type Zipped = Expect<Equal<Value<typeof zipped>, [1, "a"]>>;
export type Partial = Expect<
  Equal<Value<typeof partial>, [1 | undefined, "a" | undefined]>
>;
const custom: Operator<string, number> = (source) =>
  from(source).pipe(map((value) => value.length));
const mixed = fluentFrom([1])
  .map(String)
  .pipe(custom)
  .map((value) => value > 0);
export type Mixed = Expect<Equal<typeof mixed, FluentStream<boolean>>>;
const functionalFluent = fluentMap((value: number) => String(value))(
  of(1),
).take(1);
export type FunctionalFluent = Expect<
  Equal<typeof functionalFluent, FluentStream<string>>
>;
// @ts-expect-error Core streams intentionally do not have fluent map.
of(1).map(String);
// @ts-expect-error Core streams intentionally do not have fluent take.
of(1).take(1);
// @ts-expect-error Operators must accept the preceding stream's type.
of(1).pipe(map((value: string) => value.length));
