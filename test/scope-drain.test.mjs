import { test } from "node:test";
import assert from "node:assert/strict";
import { Scope } from "../dist/core/scope.js";
import { deferred, flush, VirtualScheduler } from "./helpers.mjs";

test("scope close waits for work registered while the last rejection is handled", async () => {
  const first = deferred();
  const cleanup = deferred();
  const failure = new Error("operation failed");
  const errors = [];
  const scope = new Scope({
    scheduler: new VirtualScheduler(),
    fail(error) {
      errors.push(error);
      scope.observe(cleanup.promise, true);
    },
    suppress(error) {
      errors.push(error);
    },
  });
  scope.observe(first.promise);
  let closed = false;
  const closing = scope.close().then(() => {
    closed = true;
  });
  await flush();
  first.reject(failure);
  await flush();
  assert.deepEqual(errors, [failure]);
  assert.equal(closed, false);
  cleanup.resolve();
  await closing;
  assert.equal(closed, true);
});

test("scope drains overlapping operations before running finalizers", async () => {
  const first = deferred();
  const second = deferred();
  const errors = [];
  const scope = new Scope({
    scheduler: new VirtualScheduler(),
    fail: (error) => errors.push(error),
    suppress: (error) => errors.push(error),
  });
  let finalized = false;
  scope.finalizers.push(() => {
    finalized = true;
  });
  scope.observe(first.promise);
  scope.observe(second.promise);
  scope.stop("stop");
  const closing = scope.close();
  second.resolve();
  await flush();
  assert.equal(finalized, false);
  const failure = new Error("late failure");
  first.reject(failure);
  await closing;
  assert.equal(finalized, true);
  assert.deepEqual(errors, [failure]);
});

test("scope close shares one drain waiter across reentrant and repeated calls", async () => {
  const operation = deferred();
  let finalized = 0;
  const scope = new Scope({
    scheduler: new VirtualScheduler(),
    fail(error) {
      throw error;
    },
    suppress(error) {
      throw error;
    },
  });
  scope.observe(operation.promise);
  scope.setCleanup(() => {
    assert.equal(scope.close(), closing);
  });
  scope.finalizers.push(() => {
    finalized++;
  });
  const closing = scope.close();
  assert.equal(scope.close(), closing);
  scope.stop("stop");
  await flush();
  assert.equal(finalized, 0);
  operation.resolve();
  await closing;
  assert.equal(scope.close(), closing);
  assert.equal(finalized, 1);
});
