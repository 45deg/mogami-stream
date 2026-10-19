import { test } from "node:test";
import assert from "node:assert/strict";
import { defer, from, map, scan } from "../dist/index.js";

test("immutable scan updates preserve snapshots across repeated and concurrent executions", async () => {
  const initial = { total: 0 };
  const totals = from([1, 2]).pipe(
    scan((state, value) => ({ total: state.total + value }), initial),
  );
  const expected = [{ total: 1 }, { total: 3 }];
  assert.deepEqual(await totals.toArray(), expected);
  assert.deepEqual(await totals.toArray(), expected);
  const [first, second] = await Promise.all([
    totals.toArray(),
    totals.toArray(),
  ]);
  assert.deepEqual(first, expected);
  assert.deepEqual(second, expected);
  assert.notEqual(first[0], second[0]);
  assert.deepEqual(initial, { total: 0 });
});

test("deferred scan factories create lazy, independent mutable accumulators", async () => {
  const states = [];
  const createState = () => {
    const state = { total: 0 };
    states.push(state);
    return state;
  };
  const totals = defer(() =>
    from([1, 2]).pipe(
      scan((state, value) => {
        state.total += value;
        return state;
      }, createState()),
      map((state) => state.total),
    ),
  );
  const unopened = totals.iterate();
  await unopened.return();
  assert.equal(states.length, 0);
  assert.deepEqual(await totals.toArray(), [1, 3]);
  assert.deepEqual(await totals.toArray(), [1, 3]);

  // Keep both executions open and advance them together to expose shared state.
  const first = totals.iterate();
  const second = totals.iterate();
  try {
    for (const value of [1, 3]) {
      assert.deepEqual(await Promise.all([first.next(), second.next()]), [
        { done: false, value },
        { done: false, value },
      ]);
    }
    assert.deepEqual(await Promise.all([first.next(), second.next()]), [
      { done: true, value: undefined },
      { done: true, value: undefined },
    ]);
  } finally {
    await Promise.all([first.return(), second.return()]);
  }
  assert.equal(states.length, 4);
  assert.equal(new Set(states).size, 4);
  assert.deepEqual(
    states,
    Array.from({ length: 4 }, () => ({ total: 3 })),
  );
});

test("scan preserves function-valued initial accumulators without invoking them", async () => {
  const initial = () => assert.fail("initial is a value, not a factory");
  const functions = from([1, 2]).pipe(
    scan((state) => {
      assert.equal(state, initial);
      return state;
    }, initial),
  );
  assert.deepEqual(await functions.toArray(), [initial, initial]);
  assert.deepEqual(await functions.toArray(), [initial, initial]);
});
