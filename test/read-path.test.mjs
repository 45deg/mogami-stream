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

for (const operator of [filterMap(() => null), dedupe(), skip(50_000)]) {
  test("discarding synchronous read sources yields to cancellation without stack growth", async () => {
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
