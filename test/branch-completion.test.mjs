import assert from "node:assert/strict";
import { test } from "node:test";
import { from, finalize } from "../dist/index.js";
import {
  broadcast as coreBroadcast,
  route as coreRoute,
} from "../dist/core/index.js";
import { deferred, flush } from "./helpers.mjs";

const done = { done: true, value: undefined };
const factories = {
  "core broadcast": (source) =>
    coreBroadcast(source, { branches: 1, capacity: 1 }),
  "fluent broadcast": (source) =>
    from(source).broadcast({ branches: 1, capacity: 1 }),
  "core route": (source) =>
    coreRoute(source, { branches: 1, capacity: 1, select: () => 0 }),
  "fluent route": (source) =>
    from(source).route({ branches: 1, capacity: 1, select: () => 0 }),
};

for (const [name, createGroup] of Object.entries(factories)) {
  test(`${name}: endpoint return and closed do not wait for shared cleanup`, async () => {
    const cleanup = deferred();
    const entered = deferred();
    const source = from([1, 2]).pipe(
      finalize(() => {
        entered.resolve();
        return cleanup.promise;
      }),
    );
    const group = createGroup(source);
    const branch = group.branches[0];
    const iterator = branch.iterate();
    assert.equal((await iterator.next()).value, 1);
    assert.equal(iterator.closed, branch.closed);
    assert.deepEqual(await iterator.return(), done);
    assert.equal((await iterator.closed).kind, "completed");
    await entered.promise;
    let closed = false;
    const closing = group.close().then((report) => {
      closed = true;
      return report;
    });
    await flush();
    assert.equal(closed, false);
    cleanup.resolve();
    assert.equal((await closing).source.kind, "completed");
  });

  test(`${name}: successful consumers can precede shared cleanup failure`, async () => {
    const cleanup = deferred();
    const failure = new Error("shared cleanup");
    const group = createGroup(from([1]).pipe(finalize(() => cleanup.promise)));
    assert.deepEqual(await from(group.branches[0]).toArray(), [1]);
    const endpoint = await group.branches[0].closed;
    assert.equal(endpoint.kind, "completed");
    let closed = false;
    void group.closed.then(() => {
      closed = true;
    });
    await flush();
    assert.equal(closed, false);
    cleanup.reject(failure);
    const report = await group.closed;
    assert.equal(report.source.kind, "failed");
    assert.equal(report.source.reason, failure);
    assert.equal(report.branches[0], endpoint);
  });

  test(`${name}: group completion does not wait for downstream finalizers`, async () => {
    const cleanup = deferred();
    const entered = deferred();
    const group = createGroup([1]);
    let finished = false;
    const job = from(group.branches[0])
      .pipe(
        finalize(() => {
          entered.resolve();
          return cleanup.promise;
        }),
      )
      .toArray()
      .then((values) => {
        finished = true;
        return values;
      });
    await entered.promise;
    assert.equal((await group.closed).source.kind, "completed");
    assert.equal(finished, false);
    cleanup.resolve();
    assert.deepEqual(await job, [1]);
  });
}

test("ordinary iterator return and closed wait for owned cleanup and report its failure", async () => {
  const cleanup = deferred();
  const entered = deferred();
  const failure = new Error("owned cleanup");
  const iterator = from([1, 2])
    .pipe(
      finalize(() => {
        entered.resolve();
        return cleanup.promise;
      }),
    )
    .iterate();
  await iterator.next();
  let returned = false;
  let closed = false;
  const returning = iterator.return().finally(() => {
    returned = true;
  });
  const rejected = assert.rejects(returning, (error) => error === failure);
  void iterator.closed.then(() => {
    closed = true;
  });
  await entered.promise;
  await flush();
  assert.equal(returned, false);
  assert.equal(closed, false);
  cleanup.reject(failure);
  await rejected;
  assert.equal((await iterator.closed).reason, failure);
});
