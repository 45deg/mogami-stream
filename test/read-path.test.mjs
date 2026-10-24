import assert from "node:assert/strict";
import { test } from "node:test";
import { create } from "../dist/core/stream.js";
import {
  filterMap,
  dedupe,
  tap,
  scan,
  take,
  skip,
  from,
  chunks,
  takeWhile,
  skipWhile,
  chain,
} from "../dist/index.js";
import { deferred, flush } from "./helpers.mjs";

function readOnlySource(values) {
  return create(() => {
    let index = 0;
    return {
      next() {
        throw new Error("intermediate next() fallback");
      },
      read(receive) {
        receive(
          index < values.length
            ? { value: values[index++], done: false }
            : { done: true },
        );
      },
    };
  });
}

for (const [name, operator, expected] of [
  ["filterMap", filterMap((x) => (x % 2 ? x * 2 : null)), [2, 6]],
  ["dedupe", dedupe(), [1, 2, 3]],
  ["tap", tap(() => {}), [1, 2, 2, 3]],
  ["scan", scan((a, x) => a + x, 0), [1, 3, 5, 8]],
  ["take", take(2), [1, 2]],
  ["skip", skip(2), [2, 3]],
]) {
  test(`${name} uses upstream read delivery`, async () => {
    assert.deepEqual(
      await readOnlySource([1, 2, 2, 3]).pipe(operator).toArray(),
      expected,
    );
  });
}

for (const [name, operator] of [
  ["filterMap", (callback) => filterMap(callback)],
  ["dedupe", (callback) => dedupe(callback)],
  ["tap", (callback) => tap(callback)],
  ["scan", (callback) => scan(callback, 0)],
]) {
  test(`${name} drains pending callbacks and retains late failures`, async () => {
    const gate = deferred();
    const late = new Error("late callback");
    const iterator = from([1])
      .pipe(operator(() => gate.promise))
      .iterate();
    const pending = assert.rejects(
      iterator.next(),
      (error) => error === "stop",
    );
    await flush();
    iterator.cancel("stop");
    await pending;
    let closed = false;
    iterator.closed.then(() => {
      closed = true;
    });
    await flush();
    assert.equal(closed, false);
    gate.reject(late);
    assert.deepEqual((await iterator.closed).suppressedErrors, [late]);
  });
}

for (const [name, operator] of [
  ["filterMap", filterMap(() => null)],
  ["dedupe", dedupe()],
  ["skip", skip(50_000)],
]) {
  test(`${name}: discarding synchronous read sources yields to cancellation without stack growth`, async () => {
    const controller = new AbortController();
    let reads = 0;
    const source = create(() => ({
      next() {
        throw new Error("next fallback");
      },
      read(receive) {
        if (++reads === 15_000) queueMicrotask(() => controller.abort("stop"));
        receive({ value: 1, done: false });
      },
    }));
    await assert.rejects(
      source.pipe(operator).toArray({ signal: controller.signal }),
      (error) => error === "stop",
    );
    assert.equal(reads, 15_000);
  });
}

test("owned read routes an outer delivery failure after synchronous reentry to its original reject", async () => {
  const { open } = await import("../dist/core/stream.js");
  const { Scope } = await import("../dist/core/scope.js");
  const { defaultScheduler } = await import("../dist/core/scheduler.js");
  const reported = [];
  const root = new Scope({
    scheduler: defaultScheduler,
    fail: (error) => reported.push(error),
    suppress: (error) => reported.push(error),
  });
  const reader = open(readOnlySource([1, 2]), root);
  const outerError = new Error("outer delivery");
  const values = [];
  const errors = [];
  reader.read(
    (result) => {
      values.push(result.value);
      reader.read(
        (nested) => values.push(nested.value),
        () => assert.fail("nested reject"),
      );
      throw outerError;
    },
    (error) => errors.push(error),
  );
  assert.deepEqual(values, [1, 2]);
  assert.deepEqual(errors, [outerError]);
  assert.deepEqual(reported, [outerError]);
  await root.close();
});

test("owned read drains a late rejection after cancellation before closing", async () => {
  const gate = deferred();
  const late = new Error("late read");
  const source = create(() => ({ next: () => gate.promise }));
  const iterator = source
    .pipe(tap(() => assert.fail("delivered after cancel")))
    .iterate();
  const pending = assert.rejects(iterator.next(), (error) => error === "stop");
  await flush();
  iterator.cancel("stop");
  await pending;
  let closed = false;
  void iterator.closed.then(() => {
    closed = true;
  });
  await flush();
  assert.equal(closed, false);
  gate.reject(late);
  const report = await iterator.closed;
  assert.deepEqual(report.suppressedErrors, [late]);
});

for (const [name, build] of [
  ["chunks", (source) => source.pipe(chunks(2))],
  [
    "takeWhile",
    (source) =>
      source.pipe(takeWhile(() => assert.fail("callback after cancellation"))),
  ],
  [
    "skipWhile",
    (source) =>
      source.pipe(skipWhile(() => assert.fail("callback after cancellation"))),
  ],
  [
    "chain",
    (source) =>
      chain(source, {
        [Symbol.iterator]() {
          assert.fail("next source opened after cancellation");
        },
      }),
  ],
]) {
  for (const rejects of [false, true]) {
    test(`${name}: next fallback drains a late ${rejects ? "failure" : "value"} before cleanup`, async () => {
      const gate = deferred();
      const started = deferred();
      const late = new Error("late read");
      let cleaned = 0;
      const source = create((scope) => {
        scope.finalizers.push(() => {
          cleaned++;
        });
        return {
          next() {
            started.resolve();
            return gate.promise;
          },
        };
      });
      const iterator = build(source).iterate();
      const pending = assert.rejects(
        iterator.next(),
        (error) => error === "stop",
      );
      await started.promise;
      iterator.cancel("stop");
      await pending;
      let closed = false;
      void iterator.closed.then(() => {
        closed = true;
      });
      await flush();
      assert.equal(closed, false);
      assert.equal(cleaned, 0);
      if (rejects) gate.reject(late);
      else gate.resolve({ done: false, value: 1 });
      const report = await iterator.closed;
      assert.equal(report.kind, "cancelled");
      assert.equal(report.reason, "stop");
      assert.deepEqual(report.suppressedErrors, rejects ? [late] : []);
      assert.equal(cleaned, 1);
      assert.deepEqual(await iterator.next(), { done: true, value: undefined });
    });
  }
}
