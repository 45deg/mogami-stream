import * as core from "mogami-stream/core";
import * as fluent from "mogami-stream";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
    ? true
    : false;
type Expect<T extends true> = T;
type Value<S> = S extends core.Stream<infer T> ? T : never;

const savedTake = core.take(1);
const savedChunks = core.chunks(2);

const coreMethod = core.from([1]).pipe(
  core.map(async (value) => value.toString()),
  core.map((value) => value.length),
  savedTake,
  savedChunks,
  core.map((values) => values[0]!),
  core.catchError(() => ["fallback"]),
  core.map((value) => value),
  core.map((value) => value),
);
export type CoreMethod = Expect<
  Equal<Value<typeof coreMethod>, number | string>
>;
const coreMethodNumbers = core.from([1]).pipe(savedTake, savedChunks);
const coreMethodStrings = core.from(["a"]).pipe(savedTake, savedChunks);
export type CoreMethodNumbers = Expect<
  Equal<Value<typeof coreMethodNumbers>, number[]>
>;
export type CoreMethodStrings = Expect<
  Equal<Value<typeof coreMethodStrings>, string[]>
>;
core.from([1]).pipe(
  // @ts-expect-error The second operator must accept the first operator's string output.
  core.map((value: number) => String(value)),
  core.map((value: number) => value + 1),
);

const coreFunction = core.pipe(
  [1],
  core.map(async (value) => value.toString()),
  core.map((value) => value.length),
  savedTake,
  savedChunks,
  core.map((values) => values[0]!),
  core.catchError(() => ["fallback"]),
  core.map((value) => value),
  core.map((value) => value),
);
export type CoreFunction = Expect<
  Equal<Value<typeof coreFunction>, number | string>
>;
const coreFunctionNumbers = core.pipe([1], savedTake, savedChunks);
const coreFunctionStrings = core.pipe(["a"], savedTake, savedChunks);
export type CoreFunctionNumbers = Expect<
  Equal<Value<typeof coreFunctionNumbers>, number[]>
>;
export type CoreFunctionStrings = Expect<
  Equal<Value<typeof coreFunctionStrings>, string[]>
>;
core.pipe(
  [1],
  // @ts-expect-error The second operator must accept the first operator's string output.
  core.map((value: number) => String(value)),
  core.map((value: number) => value + 1),
);

const fluentMethod = fluent.from([1]).pipe(
  fluent.map(async (value) => value.toString()),
  fluent.map((value) => value.length),
  savedTake,
  savedChunks,
  fluent.map((values) => values[0]!),
  fluent.catchError(() => ["fallback"]),
  fluent.map((value) => value),
  fluent.map((value) => value),
);
export type FluentMethod = Expect<
  Equal<Value<typeof fluentMethod>, number | string>
>;
const fluentMethodNumbers = fluent.from([1]).pipe(savedTake, savedChunks);
const fluentMethodStrings = fluent.from(["a"]).pipe(savedTake, savedChunks);
export type FluentMethodNumbers = Expect<
  Equal<Value<typeof fluentMethodNumbers>, number[]>
>;
export type FluentMethodStrings = Expect<
  Equal<Value<typeof fluentMethodStrings>, string[]>
>;
fluent.from([1]).pipe(
  // @ts-expect-error The second operator must accept the first operator's string output.
  fluent.map((value: number) => String(value)),
  fluent.map((value: number) => value + 1),
);

const fluentFunction = fluent.pipe(
  [1],
  fluent.map(async (value) => value.toString()),
  fluent.map((value) => value.length),
  savedTake,
  savedChunks,
  fluent.map((values) => values[0]!),
  fluent.catchError(() => ["fallback"]),
  fluent.map((value) => value),
  fluent.map((value) => value),
);
export type FluentFunction = Expect<
  Equal<Value<typeof fluentFunction>, number | string>
>;
const fluentFunctionNumbers = fluent.pipe([1], savedTake, savedChunks);
const fluentFunctionStrings = fluent.pipe(["a"], savedTake, savedChunks);
export type FluentFunctionNumbers = Expect<
  Equal<Value<typeof fluentFunctionNumbers>, number[]>
>;
export type FluentFunctionStrings = Expect<
  Equal<Value<typeof fluentFunctionStrings>, string[]>
>;
fluent.pipe(
  [1],
  // @ts-expect-error The second operator must accept the first operator's string output.
  fluent.map((value: number) => String(value)),
  fluent.map((value: number) => value + 1),
);
