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

const coreFunction9 = core.pipe(
  [1],
  core.map(async (value) => {
    const check: Expect<Equal<typeof value, number>> = true;
    void check;
    return value.toString();
  }),
  core.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  savedTake,
  savedChunks,
  core.map((value) => {
    const check: Expect<Equal<typeof value, number[]>> = true;
    void check;
    return value[0]!;
  }),
  core.catchError(() => ["fallback"]),
  core.map((value) => {
    const check: Expect<Equal<typeof value, number | string>> = true;
    void check;
    return String(value);
  }),
  core.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  core.map((value) => {
    const check: Expect<Equal<typeof value, number>> = true;
    void check;
    return { value };
  }),
);
export type coreFunction9Result = Expect<
  Equal<Value<typeof coreFunction9>, { value: number }>
>;

const coreFunction16 = core.pipe(
  [1],
  core.map(async (value) => {
    const check: Expect<Equal<typeof value, number>> = true;
    void check;
    return value.toString();
  }),
  core.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  savedTake,
  savedChunks,
  core.map((value) => {
    const check: Expect<Equal<typeof value, number[]>> = true;
    void check;
    return value[0]!;
  }),
  core.catchError(() => ["fallback"]),
  core.map((value) => {
    const check: Expect<Equal<typeof value, number | string>> = true;
    void check;
    return String(value);
  }),
  core.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  core.map((value) => {
    const check: Expect<Equal<typeof value, number>> = true;
    void check;
    return { value };
  }),
  core.map((value) => {
    const check: Expect<Equal<typeof value, { value: number }>> = true;
    void check;
    return value.value.toString();
  }),
  core.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  savedTake,
  savedChunks,
  core.map((value) => {
    const check: Expect<Equal<typeof value, number[]>> = true;
    void check;
    return value.join(",");
  }),
  core.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  core.map((value) => {
    const check: Expect<Equal<typeof value, number>> = true;
    void check;
    return String(value);
  }),
);
export type coreFunction16Result = Expect<
  Equal<Value<typeof coreFunction16>, string>
>;

const coreMethod9 = core.from([1]).pipe(
  core.map(async (value) => {
    const check: Expect<Equal<typeof value, number>> = true;
    void check;
    return value.toString();
  }),
  core.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  savedTake,
  savedChunks,
  core.map((value) => {
    const check: Expect<Equal<typeof value, number[]>> = true;
    void check;
    return value[0]!;
  }),
  core.catchError(() => ["fallback"]),
  core.map((value) => {
    const check: Expect<Equal<typeof value, number | string>> = true;
    void check;
    return String(value);
  }),
  core.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  core.map((value) => {
    const check: Expect<Equal<typeof value, number>> = true;
    void check;
    return { value };
  }),
);
export type coreMethod9Result = Expect<
  Equal<Value<typeof coreMethod9>, { value: number }>
>;

const coreMethod16 = core.from([1]).pipe(
  core.map(async (value) => {
    const check: Expect<Equal<typeof value, number>> = true;
    void check;
    return value.toString();
  }),
  core.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  savedTake,
  savedChunks,
  core.map((value) => {
    const check: Expect<Equal<typeof value, number[]>> = true;
    void check;
    return value[0]!;
  }),
  core.catchError(() => ["fallback"]),
  core.map((value) => {
    const check: Expect<Equal<typeof value, number | string>> = true;
    void check;
    return String(value);
  }),
  core.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  core.map((value) => {
    const check: Expect<Equal<typeof value, number>> = true;
    void check;
    return { value };
  }),
  core.map((value) => {
    const check: Expect<Equal<typeof value, { value: number }>> = true;
    void check;
    return value.value.toString();
  }),
  core.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  savedTake,
  savedChunks,
  core.map((value) => {
    const check: Expect<Equal<typeof value, number[]>> = true;
    void check;
    return value.join(",");
  }),
  core.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  core.map((value) => {
    const check: Expect<Equal<typeof value, number>> = true;
    void check;
    return String(value);
  }),
);
export type coreMethod16Result = Expect<
  Equal<Value<typeof coreMethod16>, string>
>;

const fluentFunction9 = fluent.pipe(
  [1],
  fluent.map(async (value) => {
    const check: Expect<Equal<typeof value, number>> = true;
    void check;
    return value.toString();
  }),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  savedTake,
  savedChunks,
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, number[]>> = true;
    void check;
    return value[0]!;
  }),
  fluent.catchError(() => ["fallback"]),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, number | string>> = true;
    void check;
    return String(value);
  }),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, number>> = true;
    void check;
    return { value };
  }),
);
export type fluentFunction9Result = Expect<
  Equal<Value<typeof fluentFunction9>, { value: number }>
>;

fluentFunction9.map((value) => value.value);

const fluentFunction16 = fluent.pipe(
  [1],
  fluent.map(async (value) => {
    const check: Expect<Equal<typeof value, number>> = true;
    void check;
    return value.toString();
  }),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  savedTake,
  savedChunks,
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, number[]>> = true;
    void check;
    return value[0]!;
  }),
  fluent.catchError(() => ["fallback"]),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, number | string>> = true;
    void check;
    return String(value);
  }),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, number>> = true;
    void check;
    return { value };
  }),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, { value: number }>> = true;
    void check;
    return value.value.toString();
  }),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  savedTake,
  savedChunks,
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, number[]>> = true;
    void check;
    return value.join(",");
  }),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, number>> = true;
    void check;
    return String(value);
  }),
);
export type fluentFunction16Result = Expect<
  Equal<Value<typeof fluentFunction16>, string>
>;

fluentFunction16.map((value) => value.length);

const fluentMethod9 = fluent.from([1]).pipe(
  fluent.map(async (value) => {
    const check: Expect<Equal<typeof value, number>> = true;
    void check;
    return value.toString();
  }),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  savedTake,
  savedChunks,
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, number[]>> = true;
    void check;
    return value[0]!;
  }),
  fluent.catchError(() => ["fallback"]),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, number | string>> = true;
    void check;
    return String(value);
  }),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, number>> = true;
    void check;
    return { value };
  }),
);
export type fluentMethod9Result = Expect<
  Equal<Value<typeof fluentMethod9>, { value: number }>
>;

fluentMethod9.map((value) => value.value);

const fluentMethod16 = fluent.from([1]).pipe(
  fluent.map(async (value) => {
    const check: Expect<Equal<typeof value, number>> = true;
    void check;
    return value.toString();
  }),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  savedTake,
  savedChunks,
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, number[]>> = true;
    void check;
    return value[0]!;
  }),
  fluent.catchError(() => ["fallback"]),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, number | string>> = true;
    void check;
    return String(value);
  }),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, number>> = true;
    void check;
    return { value };
  }),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, { value: number }>> = true;
    void check;
    return value.value.toString();
  }),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  savedTake,
  savedChunks,
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, number[]>> = true;
    void check;
    return value.join(",");
  }),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, string>> = true;
    void check;
    return value.length;
  }),
  fluent.map((value) => {
    const check: Expect<Equal<typeof value, number>> = true;
    void check;
    return String(value);
  }),
);
export type fluentMethod16Result = Expect<
  Equal<Value<typeof fluentMethod16>, string>
>;

fluentMethod16.map((value) => value.length);

const stringify = core.map((value: number) => String(value));
const length = core.map((value: string) => value.length);
const valid = [
  stringify,
  length,
  stringify,
  length,
  stringify,
  length,
  stringify,
  length,
  stringify,
  length,
  stringify,
  length,
  stringify,
  length,
  stringify,
  length,
] as const;
const invalid = [
  stringify,
  length,
  stringify,
  length,
  stringify,
  length,
  stringify,
  length,
  stringify,
  stringify,
  length,
  stringify,
  length,
  stringify,
  length,
  stringify,
] as const;

const coreFunctionSpread = core.pipe([1], ...valid);
export type coreFunctionSpreadResult = Expect<
  Equal<Value<typeof coreFunctionSpread>, number>
>;

// @ts-expect-error Stage 10 requires number, but stage 9 emits string.
core.pipe([1], ...invalid);

// @ts-expect-error A single call supports at most sixteen operators.
core.pipe([1], ...valid, stringify);

const coreMethodSpread = core.from([1]).pipe(...valid);
export type coreMethodSpreadResult = Expect<
  Equal<Value<typeof coreMethodSpread>, number>
>;

// @ts-expect-error Stage 10 requires number, but stage 9 emits string.
core.from([1]).pipe(...invalid);

// @ts-expect-error A single call supports at most sixteen operators.
core.from([1]).pipe(...valid, stringify);

const fluentFunctionSpread = fluent.pipe([1], ...valid);
export type fluentFunctionSpreadResult = Expect<
  Equal<Value<typeof fluentFunctionSpread>, number>
>;

// @ts-expect-error Stage 10 requires number, but stage 9 emits string.
fluent.pipe([1], ...invalid);

// @ts-expect-error A single call supports at most sixteen operators.
fluent.pipe([1], ...valid, stringify);

const fluentMethodSpread = fluent.from([1]).pipe(...valid);
export type fluentMethodSpreadResult = Expect<
  Equal<Value<typeof fluentMethodSpread>, number>
>;

// @ts-expect-error Stage 10 requires number, but stage 9 emits string.
fluent.from([1]).pipe(...invalid);

// @ts-expect-error A single call supports at most sixteen operators.
fluent.from([1]).pipe(...valid, stringify);
