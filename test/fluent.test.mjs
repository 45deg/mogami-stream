import assert from "node:assert/strict";
import { test } from "node:test";
import * as s from "../dist/index.js";

test("fluent operators preserve standalone operator results", async () => {
  const cases = [
    ["map", [(value) => Promise.resolve(value * 2), { concurrency: 2 }]],
    [
      "mapSettled",
      [
        (value) => {
          if (value === 2) throw new Error("two");
          return value;
        },
      ],
    ],
    ["filter", [async (value) => value % 2 === 0]],
    ["filterMap", [async (value) => (value === 2 ? null : String(value))]],
    ["dedupe", []],
    ["dedupe", [async (value) => value % 2]],
    ["chunks", [2]],
    ["scan", [async (sum, value) => sum + value, Promise.resolve(0)]],
    ["take", [2]],
    ["skip", [2]],
    ["takeWhile", [async (value) => value < 3]],
    ["skipWhile", [async (value) => value < 3]],
    ["flatMap", [(value) => [value, value * 2], { concurrency: 2 }]],
    ["concatMap", [(value) => [value, value * 2], { concurrency: 2 }]],
    ["tap", [async () => {}]],
    ["finalize", [async () => {}]],
    ["catchError", [() => [0]]],
    ["delayStart", [0]],
    ["spaceout", [0]],
    ["rateLimit", [{ count: 10, perMs: 1 }]],
    ["timeoutEach", [10_000]],
    ["deadline", [10_000]],
    ["bufferTime", [10_000, { maxItems: 2 }]],
  ];
  for (const [name, args] of cases) {
    const actual = await s
      .of(1, 2, 2, 3)
      [name](...args)
      .toArray();
    const expected = await s
      .of(1, 2, 2, 3)
      .pipe(s[name](...args))
      .toArray();
    // Error stacks differ between executions; compare settled status and messages.
    const normalize = (values) =>
      name === "mapSettled"
        ? values.map((result) =>
            result.status === "rejected"
              ? { ...result, reason: result.reason.message }
              : result,
          )
        : values;
    assert.deepEqual(normalize(actual), normalize(expected), name);
  }
  for (const name of ["flatten", "concat"]) {
    assert.deepEqual(
      await s.of([1, 2], s.of(3))[name]({ concurrency: 2 }).toArray(),
      await s
        .of([1, 2], s.of(3))
        .pipe(s[name]({ concurrency: 2 }))
        .toArray(),
      name,
    );
  }
});

test("fluent pipelines are lazy, branch independently, and accept custom operators", async () => {
  let opened = 0;
  const base = s
    .defer(() => {
      opened++;
      return [1, 2, 3];
    })
    .map((value) => value * 2);
  const strings = () => (source) => s.from(source).map(String);
  const suffix = () => (source) => s.from(source).map((value) => `${value}!`);
  const first = base.pipe(strings(), suffix()).take(1);
  const rest = base.skip(1);
  assert.equal(opened, 0);
  assert.deepEqual(await first.toArray(), ["2!"]);
  assert.deepEqual(await rest.toArray(), [4, 6]);
  assert.equal(opened, 2);
  assert.equal(base.pipe(), base);
});

test("fluent early termination awaits upstream and finalizer cleanup", async () => {
  const events = [];
  async function* source() {
    try {
      yield 1;
      yield 2;
    } finally {
      await Promise.resolve();
      events.push("upstream closed");
    }
  }
  const result = await s
    .from(source())
    .map((value) => value * 2)
    .finalize(async () => {
      await Promise.resolve();
      events.push("finalized");
    })
    .take(1)
    .toArray();
  assert.deepEqual(result, [2]);
  assert.deepEqual(events, ["upstream closed", "finalized"]);
});

test("fluent mapping passes cancellation and waits for in-flight cleanup", async () => {
  const controller = new AbortController();
  let mapperSignal;
  let cleanup = false;
  const result = s
    .of(1)
    .map(async (_value, { signal }) => {
      mapperSignal = signal;
      controller.abort("stop");
      await Promise.resolve();
      return 1;
    })
    .finalize(async () => {
      cleanup = true;
    })
    .toArray({ signal: controller.signal });
  await assert.rejects(result, (error) => error === "stop");
  assert.equal(mapperSignal.aborted, true);
  assert.equal(cleanup, true);
});

test("fluent recovery can change the output type and validates options immediately", async () => {
  assert.deepEqual(
    await s
      .of(1)
      .map(() => {
        throw new Error("bad");
      })
      .catchError(async () => ["fallback"])
      .toArray(),
    ["fallback"],
  );
  assert.throws(() => s.of(1).map(String, { concurrency: 0 }), RangeError);
  assert.throws(() => s.of(1).take(-1), RangeError);
  assert.throws(() => s.of(1).bufferTime(1, { maxItems: 0 }), RangeError);
});
