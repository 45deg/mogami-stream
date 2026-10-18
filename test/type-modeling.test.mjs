import { test } from "node:test";
import assert from "node:assert/strict";
import * as s from "../dist/index.js";
import { deferred, flush } from "./helpers.mjs";

for (const mode of ["reduce", "scan"]) {
  const run = (source, reducer, initial, options) =>
    mode === "reduce"
      ? s.reduce(source, reducer, initial, options)
      : s.from(source).pipe(s.scan(reducer, initial)).toArray(options);

  test(`${mode} resolves initial and callback promises consistently`, async () => {
    const calls = [];
    const initial = {
      then(resolve) {
        resolve(Promise.resolve(10));
      },
    };
    const result = await run(
      [1, 2],
      (acc, value, { index }) => {
        calls.push([acc, value, index]);
        assert.equal(typeof acc, "number");
        return Promise.resolve(acc + value);
      },
      initial,
    );
    assert.deepEqual(calls, [
      [10, 1, 0],
      [11, 2, 1],
    ]);
    assert.deepEqual(result, mode === "reduce" ? 13 : [11, 13]);
    assert.deepEqual(
      await run([], () => assert.fail(), initial),
      mode === "reduce" ? 10 : [],
    );
  });

  test(`${mode} waits for initialization before opening input`, async () => {
    const gate = deferred();
    let opened = false;
    const source = s.defer(() => {
      opened = true;
      return [1];
    });
    const work = run(source, (acc, value) => acc + value, gate.promise);
    await flush();
    assert.equal(opened, false);
    gate.resolve(10);
    assert.deepEqual(await work, mode === "reduce" ? 11 : [11]);
    assert.equal(opened, true);
  });

  test(`${mode} propagates initializer failure without opening input`, async () => {
    const error = new Error("initial");
    const initial = {
      then(_resolve, reject) {
        reject(error);
      },
    };
    const source = s.defer(() => assert.fail("opened"));
    await assert.rejects(
      run(source, () => assert.fail(), initial),
      (e) => e === error,
    );
  });

  test(`${mode} cancellation drains initialization and prevents input acquisition`, async () => {
    const gate = deferred();
    const controller = new AbortController();
    const reason = new Error("cancel");
    let settled = false;
    const work = run(
      s.defer(() => assert.fail("opened")),
      () => assert.fail(),
      gate.promise,
      { signal: controller.signal },
    );
    const checked = assert
      .rejects(work, (e) => e === reason)
      .then(() => {
        settled = true;
      });
    await flush();
    controller.abort(reason);
    await flush();
    assert.equal(settled, false);
    gate.resolve(0);
    await checked;
  });

  test(`${mode} already aborted executions do not start initialization`, async () => {
    const controller = new AbortController();
    controller.abort();
    const initial = {
      then() {
        assert.fail("initialized");
      },
    };
    await assert.rejects(
      run(
        s.defer(() => assert.fail("opened")),
        () => assert.fail(),
        initial,
        { signal: controller.signal },
      ),
      (e) => e === controller.signal.reason,
    );
  });
}

test("scan initialization is lazy and repeats per execution", async () => {
  let starts = 0;
  const stream = s.of(1).pipe(
    s.scan((acc, value) => acc + value, {
      then(resolve) {
        starts++;
        resolve(0);
      },
    }),
  );
  const iterator = stream.iterate();
  assert.equal(starts, 0);
  await iterator.return();
  assert.equal(starts, 0);
  assert.deepEqual(await stream.toArray(), [1]);
  assert.deepEqual(await stream.toArray(), [1]);
  assert.equal(starts, 2);
});

test("find resolves a matched promise but passes the original value to the predicate", async () => {
  const promise = Promise.resolve(7);
  for (const find of [
    (p) => s.find([p], (value) => value === p),
    (p) => s.of(p).find((value) => value === p),
  ]) {
    assert.equal(await find(promise), 7);
  }
});

test("manual iteration retains the existing completion behavior for external generators", async () => {
  function* source() {
    yield 1;
    return "finished";
  }
  const iterator = s.from(source()).iterate();
  assert.deepEqual(await iterator.next(), { done: false, value: 1 });
  assert.deepEqual(await iterator.next(), { done: true, value: undefined });
  assert.deepEqual(await iterator.closed, {
    kind: "completed",
    suppressedErrors: [],
  });
});

test("failed and cancelled close reports carry even an undefined reason", async () => {
  const failed = s
    .defer(() => {
      throw undefined;
    })
    .iterate();
  await assert.rejects(failed.next(), (error) => error === undefined);
  assert.deepEqual(await failed.closed, {
    kind: "failed",
    reason: undefined,
    suppressedErrors: [],
  });
  const cancelled = s.of(1).iterate();
  cancelled.cancel("stop");
  assert.deepEqual(await cancelled.closed, {
    kind: "cancelled",
    reason: "stop",
    suppressedErrors: [],
  });
});

test("mixed combinations retain promise-valued items and reusable operators retain values", async () => {
  const promise = Promise.resolve(3);
  for (const combine of [s.chain, s.merge]) {
    const result = await combine([1], ["a"], [promise]).toArray();
    assert.equal(result.length, 3);
    assert.ok(result.includes(1));
    assert.ok(result.includes("a"));
    assert.ok(result.includes(promise));
    assert.deepEqual(await combine().toArray(), []);
  }
  const first = s.take(1);
  assert.deepEqual(await s.of(1, 2).pipe(first).toArray(), [1]);
  assert.deepEqual(await s.of("a", "b").pipe(first).toArray(), ["a"]);
  assert.deepEqual(
    await s
      .from([1, 2, "a", 3])
      .pipe(s.takeWhile((value) => typeof value === "number"))
      .toArray(),
    [1, 2],
  );
});

test("scan cancellation rejects a pending read promptly and reports a late initializer failure", async () => {
  const gate = deferred();
  const reason = new Error("cancel");
  const late = new Error("late initialization failure");
  const iterator = s
    .defer(() => assert.fail("opened"))
    .pipe(s.scan(() => assert.fail("reduced"), gate.promise))
    .iterate();
  const pending = iterator.next();
  const rejected = assert.rejects(pending, (error) => error === reason);
  await flush();
  iterator.cancel(reason);
  await rejected;
  let closed = false;
  void iterator.closed.then(() => {
    closed = true;
  });
  await flush();
  assert.equal(closed, false);
  gate.reject(late);
  const report = await iterator.closed;
  assert.equal(report.kind, "cancelled");
  assert.equal(report.reason, reason);
  assert.ok(report.suppressedErrors.includes(late));
});

test("method reduce resolves a promised initializer on empty and nonempty input", async () => {
  assert.equal(
    await s.of(1, 2).reduce((acc, value) => acc + value, Promise.resolve(10)),
    13,
  );
  assert.equal(
    await s.empty().reduce(() => assert.fail(), Promise.resolve(10)),
    10,
  );
});
