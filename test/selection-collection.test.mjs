import assert from "node:assert/strict";
import { test } from "node:test";
import * as api from "../dist/index.js";
import * as core from "../dist/core/index.js";
import { deferred, flush } from "./helpers.mjs";

for (const entry of [api, core]) {
  test(`${entry === api ? "fluent" : "core"} new exports and saved operator inference at runtime`, async () => {
    const source = entry.of(1, 2, 3, 2);
    assert.deepEqual(await entry.slidingWindow(2)(source).toArray(), [
      [1, 2],
      [2, 3],
      [3, 2],
    ]);
    assert.deepEqual(await entry.slice(1, -1)(source).toArray(), [2, 3]);
    assert.equal(await entry.count(source), 4);
    assert.equal(await entry.drain(source), undefined);
    assert.equal(await entry.at(source, -1), 2);
    assert.deepEqual(await entry.toSet(source), new Set([1, 2, 3]));
    assert.deepEqual(
      await entry.toMap(source, (x) => x % 2),
      new Map([
        [1, 3],
        [0, 2],
      ]),
    );
  });
}

test("slice matches Array.slice across positive, negative, empty, and omitted bounds", async () => {
  const max = Number.MAX_SAFE_INTEGER;
  const bounds = [undefined, -max, -12, -6, -2, -1, -0, 0, 1, 2, 6, 12, max];
  for (let length = 0; length < 9; length++) {
    const values = Array.from({ length }, (_, i) => i);
    for (const start of bounds)
      for (const end of bounds) {
        assert.deepEqual(
          await api.from(values).slice(start, end).toArray(),
          values.slice(start, end),
          `${length}: ${start}, ${end}`,
        );
      }
  }
});

test("at matches Array.at and handles large bounds without allocating them upfront", async () => {
  for (const values of [[], [undefined], [0, 1, 2, 3]]) {
    for (const index of [
      -Number.MAX_SAFE_INTEGER,
      -5,
      -4,
      -2,
      -1,
      -0,
      0,
      1,
      3,
      4,
      9,
      Number.MAX_SAFE_INTEGER,
    ]) {
      assert.equal(await api.from(values).at(index), values.at(index));
    }
  }
});

test("range validation does not open sources", async () => {
  const source = {
    [Symbol.iterator]() {
      assert.fail("must not open");
    },
  };
  for (const value of [
    NaN,
    Infinity,
    -Infinity,
    0.5,
    Number.MAX_SAFE_INTEGER + 1,
  ]) {
    assert.throws(() => api.slice(value), RangeError);
    assert.throws(() => api.slice(0, value), RangeError);
    assert.throws(() => api.slidingWindow(value), RangeError);
    await assert.rejects(api.at(source, value), RangeError);
  }
  for (const value of [0, -1])
    assert.throws(() => api.slidingWindow(value), RangeError);
  for (const [start, end] of [
    [0, 0],
    [5, 2],
    [-2, -4],
    [-2, 0],
  ]) {
    assert.deepEqual(await api.from(source).slice(start, end).toArray(), []);
  }
});

test("windows are full, independent snapshots and execution-local", async () => {
  const source = api.of(1, 2, 3, 4).slidingWindow(2);
  const iterator = source.iterate();
  const first = (await iterator.next()).value;
  first[1] = 99;
  assert.deepEqual((await iterator.next()).value, [2, 3]);
  assert.deepEqual((await iterator.next()).value, [3, 4]);
  await iterator.return();
  assert.deepEqual(await source.toArray(), [
    [1, 2],
    [2, 3],
    [3, 4],
  ]);
  assert.deepEqual(await api.of(1, 2).slidingWindow(1).toArray(), [[1], [2]]);
  assert.deepEqual(await api.of(1, 2).slidingWindow(3).toArray(), []);
  assert.deepEqual(
    await api.of(1, 2).slidingWindow(Number.MAX_SAFE_INTEGER).toArray(),
    [],
  );
});

for (const [name, consume, expectedReads] of [
  ["slice", (s) => s.slice(2, 5).toArray(), 5],
  ["at", (s) => s.at(2), 3],
  ["window", (s) => s.slidingWindow(3).take(2).toArray(), 4],
  ["negative end", (s) => s.slice(1, -2).take(2).toArray(), 5],
]) {
  test(`${name} reads only what is needed and waits for cleanup`, async () => {
    let reads = 0;
    let closed = false;
    const gate = deferred();
    let settled = false;
    const source = api.from({
      [Symbol.iterator]() {
        return this;
      },
      next() {
        return { value: reads++, done: false };
      },
      async return() {
        closed = true;
        await gate.promise;
        return { done: true };
      },
    });
    const pending = consume(source).then((v) => {
      settled = true;
      return v;
    });
    await flush();
    assert.equal(closed, true);
    assert.equal(reads, expectedReads);
    assert.equal(settled, false);
    gate.resolve();
    await pending;
  });
}

const terminals = [
  ["drain", (s) => s.drain()],
  ["count", (s) => s.count()],
  ["toSet", (s) => s.toSet()],
  ["toMap", (s) => s.toMap((x) => x)],
  ["at positive", (s) => s.at(0)],
  ["at negative", (s) => s.at(-1)],
  ["slice tail", (s) => s.slice(-3).toArray()],
  ["window", (s) => s.slidingWindow(2).toArray()],
];
for (const [name, consume] of terminals) {
  test(`${name} propagates cleanup-only failures and preserves primary errors`, async () => {
    const primary = new Error("read");
    const cleanup = new Error("cleanup");
    for (const failRead of [false, true]) {
      let returns = 0;
      let finalized = 0;
      const source = api.from({
        [Symbol.asyncIterator]() {
          return this;
        },
        async next() {
          if (failRead) throw primary;
          return { done: true };
        },
        async return() {
          returns++;
          throw cleanup;
        },
      });
      const pipeline = source.finalize(() => {
        finalized++;
        throw cleanup;
      });
      await assert.rejects(
        consume(pipeline),
        (e) => e === (failRead ? primary : cleanup),
      );
      assert.equal(returns, failRead ? 1 : 0);
      assert.equal(finalized, 1);
    }
  });
}

const cancellable = [
  ["drain", (s, options) => s.drain(options)],
  ["count", (s, options) => s.count(options)],
  ["toSet", (s, options) => s.toSet(options)],
  ["toMap", (s, options) => s.toMap((x) => x, options)],
  ["at", (s, options) => s.at(-1, options)],
  ["slice", (s, options) => s.slice(-5).toArray(options)],
  ["window", (s, options) => s.slidingWindow(100000).toArray(options)],
];
for (const [name, consume] of cancellable) {
  test(`${name} accepts pre-abort and queued cancellation during consumption`, async () => {
    const reason = new Error("stop");
    await assert.rejects(
      consume(
        api.defer(() => assert.fail("opened")),
        { signal: AbortSignal.abort(reason) },
      ),
      (e) => e === reason,
    );
    const controller = new AbortController();
    let reads = 0;
    let returns = 0;
    const source = api.from({
      [Symbol.iterator]() {
        return this;
      },
      next() {
        reads++;
        if (reads === 3) queueMicrotask(() => controller.abort(reason));
        return { done: false, value: reads };
      },
      return() {
        returns++;
        return { done: true };
      },
    });
    await assert.rejects(
      consume(source, { signal: controller.signal }),
      (e) => e === reason,
    );
    assert.ok(reads <= 4);
    assert.equal(returns, 1);
  });
}

test("collections preserve item identity and Map uses awaited keys, sequential contexts, and last values", async () => {
  const promise = Promise.resolve(9);
  const values = [promise, promise, NaN, NaN, -0, 0, undefined];
  assert.deepEqual(await api.from(values).toSet(), new Set(values));
  assert.equal(await api.from(values).count(), values.length);
  const seen = [];
  let active = 0;
  const entries = [
    { id: 1, value: "a" },
    { id: 2, value: "b" },
    { id: 1, value: "c" },
  ];
  const result = await api.from(entries).toMap(async (value, context) => {
    assert.equal(active++, 0);
    assert.equal(context.signal.aborted, false);
    seen.push(context.index);
    await Promise.resolve();
    active--;
    return value.id;
  });
  assert.deepEqual(seen, [0, 1, 2]);
  assert.deepEqual(
    [...result],
    [
      [1, entries[2]],
      [2, entries[1]],
    ],
  );
  assert.equal((await api.of(promise).toMap(() => "key")).get("key"), promise);
  assert.equal(await api.of(promise).at(0), 9);
  assert.equal(await api.of(promise).at(-1), 9);
  assert.deepEqual(await api.empty().toMap((x) => x), new Map());
  assert.deepEqual(await api.empty().toSet(), new Set());
  assert.equal(await api.empty().count(), 0);
});

test("toMap owns pending selectors, propagates cancellation, and waits for settlement", async () => {
  const controller = new AbortController();
  const reason = new Error("cancel");
  const gate = deferred();
  const started = deferred();
  let callbackSignal;
  let calls = 0;
  const pending = api.of(1, 2).toMap(
    async (_, { signal }) => {
      calls++;
      callbackSignal = signal;
      started.resolve();
      await gate.promise;
      return 1;
    },
    { signal: controller.signal },
  );
  let settled = false;
  const rejected = assert.rejects(pending, (e) => {
    settled = true;
    return e === reason;
  });
  await started.promise;
  controller.abort(reason);
  await flush();
  assert.equal(callbackSignal.aborted, true);
  assert.equal(settled, false);
  gate.resolve();
  await rejected;
  assert.equal(calls, 1);
});

test("at starts upstream cleanup before awaiting the selected Promise", async () => {
  const gate = deferred();
  let closed = false;
  const source = api.from({
    *[Symbol.iterator]() {
      try {
        yield gate.promise;
      } finally {
        closed = true;
      }
    },
  });
  const pending = source.at(0);
  await flush();
  assert.equal(closed, true);
  gate.resolve(7);
  assert.equal(await pending, 7);
});

test("toMap selector and at selected-value failures close upstream", async () => {
  const failure = new Error("selection");
  for (const consume of [
    (s) =>
      s.toMap(() => {
        throw failure;
      }),
    (s) => s.at(0),
  ]) {
    let closed = false;
    const source = api.from({
      *[Symbol.iterator]() {
        try {
          yield {
            then(_resolve, reject) {
              reject(failure);
            },
          };
        } finally {
          closed = true;
        }
      },
    });
    await assert.rejects(consume(source), (e) => e === failure);
    assert.equal(closed, true);
  }
});

test("new operators and consumers accept asynchronous inputs", async () => {
  function source() {
    return api.from(
      (async function* () {
        for (const value of [0, 1, 2, 3, 4]) {
          await Promise.resolve();
          yield value;
        }
      })(),
    );
  }
  assert.deepEqual(await source().slice(1, 4).toArray(), [1, 2, 3]);
  assert.deepEqual(await source().slice(-4, -1).toArray(), [1, 2, 3]);
  assert.deepEqual(await source().slice(1, -1).toArray(), [1, 2, 3]);
  assert.deepEqual(await source().slidingWindow(3).toArray(), [
    [0, 1, 2],
    [1, 2, 3],
    [2, 3, 4],
  ]);
  assert.equal(await source().at(2), 2);
  assert.equal(await source().at(-2), 3);
  assert.equal(await source().count(), 5);
  assert.equal(await source().drain(), undefined);
  assert.deepEqual(await source().toSet(), new Set([0, 1, 2, 3, 4]));
  assert.deepEqual(
    await source().toMap((x) => x % 2),
    new Map([
      [0, 4],
      [1, 3],
    ]),
  );
});

test("collection keys use SameValueZero and do not unwrap items", async () => {
  const promise = new Promise(() => {});
  const thenable = {
    get then() {
      return assert.fail("unwrapped item");
    },
  };
  assert.deepEqual(
    await api.of(promise, thenable).toSet(),
    new Set([promise, thenable]),
  );
  assert.equal(await api.of(promise, thenable).count(), 2);
  await api.of(promise, thenable).drain();
  assert.deepEqual(
    [...(await api.of(promise, thenable).toMap(() => NaN))],
    [[NaN, thenable]],
  );
  assert.deepEqual(await api.of(promise, thenable).slice(-2).toArray(), [
    promise,
    thenable,
  ]);
  assert.deepEqual(await api.of(promise, thenable).slidingWindow(2).toArray(), [
    [promise, thenable],
  ]);
});

for (const index of [0, -1]) {
  for (const cancel of [false, true]) {
    for (const cleanupFails of [false, true]) {
      test(`at(${index}) waits for cleanup and preserves its outcome (cancel=${cancel}, cleanupFails=${cleanupFails})`, async () => {
        const gate = deferred();
        const controller = new AbortController();
        const cancellation = new Error("cancel during cleanup");
        const cleanupError = new Error("cleanup failed");
        let cleanups = 0;
        let settled = false;
        const outcome = api
          .of(42)
          .finalize(() => {
            cleanups++;
            return gate.promise;
          })
          .at(index, { signal: controller.signal })
          .then(
            (value) => {
              settled = true;
              return { value };
            },
            (error) => {
              settled = true;
              return { error };
            },
          );
        await flush();
        assert.equal(cleanups, 1);
        assert.equal(settled, false);
        if (cancel) {
          controller.abort(cancellation);
          await flush();
          assert.equal(settled, false);
        }
        if (cleanupFails) gate.reject(cleanupError);
        else gate.resolve();
        assert.deepEqual(
          await outcome,
          cancel
            ? { error: cancellation }
            : cleanupFails
              ? { error: cleanupError }
              : { value: 42 },
        );
        assert.equal(cleanups, 1);
      });
    }
  }
}
