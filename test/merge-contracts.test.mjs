import { test } from "node:test";
import assert from "node:assert/strict";
import { empty, finalize, iterate, merge, toArray } from "../dist/index.js";
import { deferred, flush } from "./helpers.mjs";

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
