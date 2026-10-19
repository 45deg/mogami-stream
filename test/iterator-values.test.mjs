import assert from "node:assert/strict";
import { test } from "node:test";
import * as stream from "../dist/index.js";
import { deferred, flush } from "./helpers.mjs";

function externalSource(protocol, next, close = () => ({ done: true })) {
  return {
    [protocol]() {
      return {
        next: protocol === Symbol.asyncIterator ? async () => next() : next,
        return: close,
      };
    },
  };
}

for (const protocol of [Symbol.iterator, Symbol.asyncIterator]) {
  const label = protocol === Symbol.iterator ? "sync" : "async";

  for (const [route, wrap] of [
    ["direct", (source) => source],
    ["from", (source) => stream.from(source)],
    ["defer", (source) => stream.defer(() => source)],
  ]) {
    test(`${label} value getter failure closes ${route} terminal input once`, async () => {
      const failure = new Error("value getter");
      const cleanupFailure = new Error("cleanup");
      const cleanup = deferred();
      let returns = 0;
      let settled = false;
      const source = externalSource(
        protocol,
        () => ({
          get value() {
            throw failure;
          },
        }),
        () => {
          returns++;
          return cleanup.promise;
        },
      );
      const rejected = assert.rejects(stream.toArray(wrap(source)), (error) => {
        settled = true;
        return error === failure;
      });
      await flush();
      const returnsBeforeCleanup = returns;
      const settledBeforeCleanup = settled;
      cleanup.reject(cleanupFailure);
      await rejected;
      assert.equal(returnsBeforeCleanup, 1);
      assert.equal(settledBeforeCleanup, false);
      assert.equal(returns, 1);
    });
  }

  test(`${label} filter tests and emits the same value with one getter read per item`, async () => {
    let reads = 0;
    let getters = 0;
    const source = externalSource(protocol, () =>
      reads++ < 3
        ? {
            get value() {
              return ++getters;
            },
          }
        : { done: true },
    );
    const seen = [];
    const output = await stream
      .from(source)
      .pipe(
        stream.filter((value) => {
          seen.push(value);
          return value % 2 === 1;
        }),
      )
      .toArray();
    assert.deepEqual(seen, [1, 2, 3]);
    assert.deepEqual(output, [1, 3]);
    assert.equal(getters, 3);
  });

  for (const [name, operator] of [
    ["filter", stream.filter],
    ["filterMap", stream.filterMap],
    ["dedupe", stream.dedupe],
    ["takeWhile", stream.takeWhile],
    ["skipWhile", stream.skipWhile],
    ["tap", stream.tap],
    ["scan", (callback) => stream.scan(callback, 0)],
    ["flatMap", stream.flatMap],
    ["concatMap", stream.concatMap],
    [
      "concurrent flatMap",
      (callback) => stream.flatMap(callback, { concurrency: 2 }),
    ],
    [
      "concurrent concatMap",
      (callback) => stream.concatMap(callback, { concurrency: 2 }),
    ],
    ["map", stream.map],
    ["concurrent map", (callback) => stream.map(callback, { concurrency: 2 })],
  ]) {
    test(`${label} ${name} never starts a callback after a value getter aborts`, async () => {
      const controller = new AbortController();
      const reason = new Error("abort in value getter");
      let calls = 0;
      let getters = 0;
      let returns = 0;
      const source = externalSource(
        protocol,
        () => ({
          get value() {
            getters++;
            controller.abort(reason);
            return 1;
          },
        }),
        () => {
          returns++;
          return { done: true };
        },
      );
      const iterator = operator(() => {
        calls++;
        return [];
      })(source).iterate({ signal: controller.signal });
      await assert.rejects(iterator.next(), (error) => error === reason);
      assert.deepEqual(await iterator.closed, {
        kind: "cancelled",
        reason,
        suppressedErrors: [],
      });
      assert.equal(calls, 0);
      assert.equal(getters, 1);
      assert.equal(returns, 1);
    });
  }

  test(`${label} values stay lazy and preserve falsy values and Promise identity`, async () => {
    const promise = new Promise(() => {});
    const thenable = {
      get then() {
        throw new Error("must not assimilate an item");
      },
    };
    const values = [promise, thenable, 0, false, "", null, undefined, NaN];
    let reads = 0;
    let getters = 0;
    const source = externalSource(protocol, () => {
      const index = reads++;
      if (index === values.length) {
        return {
          done: true,
          get value() {
            throw new Error("must not read a completed result's value");
          },
        };
      }
      return {
        get value() {
          getters++;
          return values[index];
        },
      };
    });
    const iterator = stream.from(source).iterate();
    assert.equal(reads, 0);
    assert.equal(getters, 0);
    for (const value of values) {
      const result = await iterator.next();
      assert.equal(result.done, false);
      assert.equal(result.value, value);
      assert.equal(result.value, value);
    }
    assert.deepEqual(await iterator.next(), { done: true, value: undefined });
    assert.equal((await iterator.closed).kind, "completed");
    assert.equal(getters, values.length);
    assert.equal(reads, values.length + 1);
  });
}
