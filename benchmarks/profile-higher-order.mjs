import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { pathToFileURL } from "node:url";
import { resolve } from "node:path";
if (process.argv.length !== 4)
  throw new Error(
    "Usage: node --expose-gc benchmarks/profile-higher-order.mjs path/to/dist/index.js case",
  );
const s = await import(pathToFileURL(resolve(process.argv[2])).href);
const count = Number(process.env.BENCH_ITEMS ?? 10000);
assert.ok(
  Number.isSafeInteger(count) && count > 0,
  "BENCH_ITEMS must be a positive safe integer",
);
const values = Array.from({ length: count }, (_, i) => i);
const cases = {
  tap: () =>
    s
      .from(values)
      .pipe(s.tap(() => {}))
      .toArray(),
  reduce: () => s.from(values).reduce((a, v) => a + v, 0),
  flatMap: () =>
    s
      .from(values)
      .pipe(s.flatMap((v) => [v]))
      .toArray(),
  concatMap: () =>
    s
      .from(values)
      .pipe(s.concatMap((v) => [v]))
      .toArray(),
  flatten: () =>
    s
      .from(values.map((v) => [v]))
      .pipe(s.flatten())
      .toArray(),
  concat: () =>
    s
      .from(values.map((v) => [v]))
      .pipe(s.concat())
      .toArray(),
  flatMapEmpty: () =>
    s
      .from(values)
      .pipe(s.flatMap(() => []))
      .toArray(),
  flatMapLarge: () =>
    s
      .of(values)
      .pipe(s.flatMap((v) => v))
      .toArray(),
  flatMapConcurrent: () =>
    s
      .from(values)
      .pipe(s.flatMap((v) => [v], { concurrency: 8 }))
      .toArray(),
  concatMapConcurrent: () =>
    s
      .from(values)
      .pipe(s.concatMap((v) => [v], { concurrency: 8 }))
      .toArray(),
};
const name = process.argv[3];
assert.ok(Object.hasOwn(cases, name), `Unknown case: ${name}`);
const samples = [];
for (let i = 0; i < 29; i++) {
  global.gc?.();
  const start = performance.now();
  const result = await cases[name]();
  const ms = performance.now() - start;
  if (name === "reduce") assert.equal(result, (count * (count - 1)) / 2);
  else if (name === "flatMapEmpty") assert.deepEqual(result, []);
  else if (name === "flatMapConcurrent")
    assert.deepEqual(
      result.toSorted((a, b) => a - b),
      values,
    );
  else assert.deepEqual(result, values);
  if (i >= 8) samples.push(ms);
}
console.log(
  JSON.stringify({
    name,
    items: count,
    warmup: 8,
    samples,
    median: samples.toSorted((a, b) => a - b)[10],
  }),
);
