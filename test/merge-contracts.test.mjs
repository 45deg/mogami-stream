import { test } from "node:test";
import assert from "node:assert/strict";
import { empty, finalize, iterate, merge, toArray } from "../dist/index.js";
import { controlled, deferred, flush } from "./helpers.mjs";

test("merge preserves rotation as empty and shorter inputs finish", async () => {
  const source = merge(
    [],
    ["a0"],
    [],
    ["b0", "b1"],
    [],
    ["c0", "c1", "c2"],
    [],
  );
  const expected = ["a0", "b0", "c0", "b1", "c1", "c2"];
  assert.deepEqual(await toArray(source), expected);
  assert.deepEqual(await toArray(source), expected);
  assert.deepEqual(await toArray(merge([], [], [])), []);
  assert.deepEqual(await toArray(merge()), []);
});

test("merge waits for cleanup of an input removed from rotation", async () => {
  const cleanup = deferred();
  let finalized = false;
  const iterator = iterate(
    merge(
      empty().pipe(
        finalize(() => {
          finalized = true;
          return cleanup.promise;
        }),
      ),
      [1, 2],
    ),
  );
  assert.equal((await iterator.next()).value, 1);
  assert.equal((await iterator.next()).value, 2);
  let closed = false;
  const closing = iterator.return().then(() => {
    closed = true;
  });
  await flush();
  assert.equal(finalized, true);
  assert.equal(closed, false);
  cleanup.resolve();
  await closing;
  assert.equal((await iterator.closed).kind, "completed");
});

test("merge selects ready positions in rotation, not notification order", async () => {
  const inputs = Array.from({ length: 65 }, () => controlled());
  const iterator = iterate(merge(...inputs.map((input) => input.source)));
  try {
    const first = iterator.next();
    await flush();
    for (const index of [64, 32, 0])
      inputs[index].reads[0].resolve({ value: index });
    assert.equal((await first).value, 0);
    await flush();
    assert.ok(inputs.every((input) => input.reads.length === 1));
    assert.equal((await iterator.next()).value, 32);
    assert.equal((await iterator.next()).value, 64);
    const wrapped = iterator.next();
    await flush();
    inputs[32].reads[1].resolve({ value: 132 });
    inputs[0].reads[1].resolve({ done: true });
    inputs[1].reads[0].resolve({ value: 1 });
    assert.equal((await wrapped).value, 1);
    assert.equal((await iterator.next()).value, 132);
  } finally {
    const closing = iterator.return();
    for (const input of inputs)
      for (const read of input.reads) read.resolve({ done: true });
    await closing;
  }
  assert.equal(inputs[0].returns, 0);
  assert.ok(inputs.slice(1).every((input) => input.returns === 1));
});

test("merge reports an idle sibling failure and waits for pending reads on close", async () => {
  const inputs = Array.from({ length: 65 }, () => controlled());
  const iterator = iterate(
    merge([1, 2], ...inputs.map((input) => input.source)),
  );
  assert.equal((await iterator.next()).value, 1);
  const error = new Error("idle sibling failed");
  inputs[32].reads[0].reject(error);
  await flush();
  await assert.rejects(iterator.next(), (caught) => caught === error);
  let closed = false;
  void iterator.closed.then(() => {
    closed = true;
  });
  await flush();
  assert.equal(closed, false);
  for (const input of inputs)
    for (const read of input.reads) read.resolve({ done: true });
  assert.equal((await iterator.closed).reason, error);
  assert.ok(inputs.every((input) => input.returns === 1));
});

test("merge preserves round-robin order across many uneven input lifetimes", async () => {
  const inputs = Array.from({ length: 1024 }, (_, index) =>
    Array.from({ length: (index * 17) % 7 }, (_, turn) => index * 10 + turn),
  );
  const expected = [];
  for (let turn = 0; turn < 7; turn++)
    for (const input of inputs)
      if (turn < input.length) expected.push(input[turn]);
  assert.deepEqual(await toArray(merge(...inputs)), expected);
});
