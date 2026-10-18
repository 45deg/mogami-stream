import { test } from "node:test";
import assert from "node:assert/strict";
import * as s from "../dist/index.js";
import { iterateWithScheduler } from "../dist/terminal/iterate.js";
import { createTestClock } from "../dist/internal/test-clock.js";
import { deferred, flush } from "./helpers.mjs";

test("forEach awaits effects sequentially with context and waits for cleanup", async () => {
  const gate = deferred(),
    cleanup = deferred();
  const calls = [];
  let callbackSignal;
  const source = s.of(10, 20).pipe(s.finalize(() => cleanup.promise));
  let settled = false;
  const work = source
    .forEach(async (value, { index, signal }) => {
      calls.push([value, index]);
      callbackSignal = signal;
      if (index === 0) await gate.promise;
    })
    .then(() => {
      settled = true;
    });
  await flush();
  assert.deepEqual(calls, [[10, 0]]);
  assert.equal(callbackSignal.aborted, false);
  gate.resolve();
  await flush();
  assert.deepEqual(calls, [
    [10, 0],
    [20, 1],
  ]);
  assert.equal(settled, false);
  cleanup.resolve();
  await work;
  assert.equal(await s.forEach([], () => assert.fail()), undefined);
});

for (const name of ["forEach", "find", "some", "every"]) {
  test(`${name} handles empty input and standalone/method equivalence`, async () => {
    const expected = {
      forEach: undefined,
      find: undefined,
      some: false,
      every: true,
    }[name];
    assert.equal(await s[name]([], () => assert.fail()), expected);
    assert.equal(await s.empty()[name](() => assert.fail()), expected);
    const effect = async (x) => x > 1;
    assert.equal(
      await s[name]([1, 2, 3], effect),
      await s.of(1, 2, 3)[name](effect),
    );
  });

  test(`${name} preserves a callback error when cleanup fails`, async () => {
    const primary = new Error("callback"),
      secondary = new Error("cleanup");
    await assert.rejects(
      s
        .of(1)
        .pipe(
          s.finalize(() => {
            throw secondary;
          }),
        )
        [name](() => {
          throw primary;
        }),
      (error) => error === primary,
    );
  });

  test(`${name} does not open an already cancelled source`, async () => {
    const reason = new Error("cancel");
    const controller = new AbortController();
    controller.abort(reason);
    const source = s.defer(() => {
      assert.fail("opened");
    });
    await assert.rejects(
      source[name](() => true, { signal: controller.signal }),
      (error) => error === reason,
    );
  });

  test(`${name} cancels callbacks and waits for their outstanding work`, async () => {
    const controller = new AbortController(),
      gate = deferred();
    const reason = new Error("cancel");
    let signal,
      settled = false;
    const work = s.of(1)[name](
      async (_value, context) => {
        signal = context.signal;
        await gate.promise;
        return true;
      },
      { signal: controller.signal },
    );
    const checked = assert
      .rejects(work, (error) => error === reason)
      .then(() => {
        settled = true;
      });
    await flush();
    controller.abort(reason);
    await flush();
    assert.equal(signal.aborted, true);
    assert.equal(settled, false);
    gate.resolve();
    await checked;
  });
}

for (const [name, predicate, expected, reads] of [
  ["find", (x) => x === 2, 2, 2],
  ["some", (x) => x === 2, true, 2],
  ["every", (x) => x < 2, false, 2],
]) {
  test(`${name} short circuits, closes upstream, and waits for cleanup`, async () => {
    let count = 0,
      returned = false,
      settled = false;
    const gate = deferred(),
      indexes = [];
    const source = {
      *[Symbol.iterator]() {
        try {
          for (const value of [1, 2, 3]) {
            count++;
            yield value;
          }
        } finally {
          returned = true;
        }
      },
    };
    const work = s[name](
      s.from(source).pipe(s.finalize(() => gate.promise)),
      async (value, { index }) => {
        indexes.push(index);
        return predicate(value);
      },
    ).then((result) => {
      settled = true;
      return result;
    });
    await flush();
    assert.equal(count, reads);
    assert.equal(returned, true);
    assert.equal(settled, false);
    assert.deepEqual(indexes, [0, 1]);
    gate.resolve();
    assert.equal(await work, expected);
  });
}

test("matching undefined counts as a successful some result", async () => {
  assert.equal(await s.some([undefined], () => true), true);
  assert.equal(await s.every([undefined], () => true), true);
});

test("early successful terminals reject cleanup failures", async () => {
  const error = new Error("cleanup");
  for (const name of ["find", "some", "every"]) {
    await assert.rejects(
      s
        .of(1, 2)
        .pipe(
          s.finalize(() => {
            throw error;
          }),
        )
        [name](() => name !== "every"),
      (reason) => reason === error,
    );
  }
});

test("scan emits updated state only and resets execution state", async () => {
  const contexts = [];
  const source = s.of(1, 2, 3).pipe(
    s.scan(async (sum, value, { index, signal }) => {
      contexts.push(index);
      assert.equal(signal.aborted, false);
      return sum + value;
    }, 0),
  );
  assert.deepEqual(await source.toArray(), [1, 3, 6]);
  assert.deepEqual(await source.toArray(), [1, 3, 6]);
  assert.deepEqual(contexts, [0, 1, 2, 0, 1, 2]);
  assert.deepEqual(
    await s
      .empty()
      .pipe(s.scan(() => assert.fail(), 0))
      .toArray(),
    [],
  );
});

test("scan propagates failure and cancellation through reducer work", async () => {
  const error = new Error("reducer");
  await assert.rejects(
    s
      .of(1)
      .pipe(
        s.scan(() => {
          throw error;
        }, 0),
      )
      .toArray(),
    (e) => e === error,
  );
  const controller = new AbortController(),
    gate = deferred();
  let signal,
    settled = false;
  const work = s
    .of(1)
    .pipe(
      s.scan(async (sum, _value, ctx) => {
        signal = ctx.signal;
        await gate.promise;
        return sum + 1;
      }, 0),
    )
    .toArray({ signal: controller.signal });
  const checked = assert
    .rejects(work, (e) => e === error)
    .then(() => {
      settled = true;
    });
  await flush();
  controller.abort(error);
  await flush();
  assert.equal(signal.aborted, true);
  assert.equal(settled, false);
  gate.resolve();
  await checked;
});

test("fromTimeline validates and snapshots its schedule", async () => {
  for (const at of [-1, NaN, Infinity])
    assert.throws(() => s.fromTimeline([{ at, kind: "complete" }]), RangeError);
  assert.throws(
    () =>
      s.fromTimeline([
        { at: 2, kind: "value", value: 1 },
        { at: 1, kind: "complete" },
      ]),
    RangeError,
  );
  assert.throws(
    () =>
      s.fromTimeline([
        { at: 0, kind: "complete" },
        { at: 1, kind: "value", value: 1 },
      ]),
    RangeError,
  );
  assert.throws(() => s.fromTimeline([{ at: 0, kind: "unknown" }]), TypeError);
  const events = [{ at: 0, kind: "value", value: 1 }];
  const source = s.fromTimeline(events);
  events[0].value = 9;
  events.push({ at: 0, kind: "value", value: 2 });
  assert.deepEqual(await source.toArray(), [1]);
  assert.deepEqual(await source.toArray(), [1]);
  assert.deepEqual(await s.fromTimeline([]).toArray(), []);
});

test("fromTimeline is lazy, preserves equal-time order and promise identity, and counts idle time", async () => {
  const clock = createTestClock(),
    promise = Promise.resolve(3);
  const iterator = iterateWithScheduler(
    s.fromTimeline([
      { at: 10, kind: "value", value: promise },
      { at: 10, kind: "value", value: "B" },
      { at: 20, kind: "complete" },
    ]),
    {},
    clock,
  );
  await clock.advanceTo(100);
  assert.equal(clock.pendingTimers, 0);
  let ready = false;
  const first = iterator.next().then((value) => {
    ready = true;
    return value;
  });
  await clock.advanceBy(9);
  assert.equal(ready, false);
  await clock.advanceBy(1);
  assert.equal((await first).value, promise);
  assert.equal(clock.pendingTimers, 0);
  await clock.advanceBy(100);
  assert.equal((await iterator.next()).value, "B");
  assert.equal((await iterator.next()).done, true);
  assert.equal((await iterator.closed).kind, "completed");
});

test("fromTimeline shares the execution clock with timing operators", async () => {
  const clock = createTestClock();
  const iterator = iterateWithScheduler(
    s
      .fromTimeline([
        { at: 10, kind: "value", value: 1 },
        { at: 20, kind: "value", value: 2 },
      ])
      .pipe(s.timeoutEach(15)),
    {},
    clock,
  );
  const first = iterator.next();
  await clock.advanceBy(10);
  assert.equal((await first).value, 1);
  const second = iterator.next();
  await clock.advanceBy(10);
  assert.equal((await second).value, 2);
  await iterator.return();
  assert.equal(clock.pendingTimers, 0);
});

test("fromTimeline delivers errors and releases timers on cancellation or timeout", async () => {
  const error = new Error("timeline");
  const clock = createTestClock();
  const iterator = iterateWithScheduler(
    s.fromTimeline([{ at: 10, kind: "error", error }]),
    {},
    clock,
  );
  const failed = assert.rejects(iterator.next(), (e) => e === error);
  await clock.advanceBy(10);
  await failed;
  assert.equal((await iterator.closed).reason, error);
  for (const timeout of [false, true]) {
    const source = s.fromTimeline([{ at: 100, kind: "value", value: 1 }]);
    const pending = iterateWithScheduler(
      timeout ? source.pipe(s.timeoutEach(10)) : source,
      {},
      clock,
    );
    const checked = assert.rejects(
      pending.next(),
      timeout ? s.TimeoutError : (e) => e === error,
    );
    await flush();
    if (timeout) await clock.advanceBy(10);
    else pending.cancel(error);
    await checked;
    await pending.closed;
    assert.equal(clock.pendingTimers, 0);
  }
});

test("test clock advances chained timers in order and supports cancellation", async () => {
  const clock = createTestClock(),
    controller = new AbortController(),
    seen = [];
  const work = (async () => {
    await clock.sleep(10, controller.signal);
    seen.push(clock.now());
    await clock.sleep(10, controller.signal);
    seen.push(clock.now());
  })();
  await clock.advanceTo(30);
  await work;
  assert.deepEqual(seen, [10, 20]);
  assert.equal(clock.now(), 30);
  const reason = new Error("cancel");
  const cancelled = assert.rejects(
    clock.sleep(100, controller.signal),
    (e) => e === reason,
  );
  assert.equal(clock.pendingTimers, 1);
  controller.abort(reason);
  await cancelled;
  assert.equal(clock.pendingTimers, 0);
  await assert.rejects(clock.advanceTo(29), RangeError);
  await assert.rejects(clock.advanceTo(Infinity), RangeError);
  const advancing = clock.advanceBy(1);
  await assert.rejects(clock.advanceBy(1), TypeError);
  await advancing;
});
