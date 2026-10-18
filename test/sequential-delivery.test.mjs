import assert from "node:assert/strict";
import { test } from "node:test";
import { filter, from, iterate, map, toArray } from "../dist/index.js";
import { deferred, flush } from "./helpers.mjs";

test("queued cancellation interrupts synchronous filtering without another source read", async () => {
  const controller = new AbortController();
  const reason = new Error("queued filter cancellation");
  let reads = 0;
  let closed = false;
  const input = {
    *[Symbol.iterator]() {
      try {
        while (reads < 20_000) yield reads++;
      } finally {
        closed = true;
      }
    },
  };
  const iterator = iterate(
    from(input).pipe(
      filter((value) => {
        if (value === 15_000) queueMicrotask(() => controller.abort(reason));
        return false;
      }),
    ),
    { signal: controller.signal },
  );
  await assert.rejects(iterator.next(), (error) => error === reason);
  const report = await iterator.closed;
  assert.equal(reads, 15_001);
  assert.equal(closed, true);
  assert.equal(report.reason, reason);
  assert.deepEqual(report.suppressedErrors, []);
});

test("sequential map retains the cancellation checkpoint for synchronous callback results", async () => {
  const controller = new AbortController();
  const reason = new Error("queued mapper cancellation");
  let downstreamCalls = 0;
  const pipeline = from([1, 2]).pipe(
    map((value) => {
      queueMicrotask(() => controller.abort(reason));
      return value;
    }),
    map((value) => {
      downstreamCalls++;
      return value;
    }),
  );
  await assert.rejects(
    toArray(pipeline, { signal: controller.signal }),
    (error) => error === reason,
  );
  assert.equal(downstreamCalls, 0);
});

test("cancelled sequential chains drain a pending predicate and preserve its late failure", async () => {
  const predicate = deferred();
  const cleanup = deferred();
  const late = new Error("late predicate failure");
  let reads = 0;
  let returns = 0;
  let downstreamCalls = 0;
  const input = {
    [Symbol.iterator]() {
      return this;
    },
    next() {
      return { value: reads++ };
    },
    return() {
      returns++;
      return cleanup.promise;
    },
  };
  const iterator = iterate(
    from(input).pipe(
      map((value) => value + 1),
      filter(() => predicate.promise),
      map((value) => {
        downstreamCalls++;
        return value;
      }),
    ),
  );
  const pending = iterator.next();
  await flush();
  iterator.cancel("stop");
  await assert.rejects(pending, (error) => error === "stop");
  let closed = false;
  iterator.closed.then(() => {
    closed = true;
  });
  cleanup.resolve({ done: true });
  await flush();
  assert.equal(closed, false);
  assert.equal(returns, 1);
  predicate.reject(late);
  const report = await iterator.closed;
  assert.equal(reads, 1);
  assert.equal(downstreamCalls, 0);
  assert.equal(report.kind, "cancelled");
  assert.deepEqual(report.suppressedErrors, [late]);
});

test("filter preserves Promise-valued items while sequential map awaits its callback result", async () => {
  const promise = Promise.resolve(7);
  const kept = from([promise]).pipe(filter(() => true));
  assert.deepEqual(await kept.toArray(), [promise]);
  assert.deepEqual(await kept.pipe(map((value) => value)).toArray(), [7]);
});

test("sequential stages preserve ordered concurrent delivery and its read-ahead bound", async () => {
  const gates = Array.from({ length: 5 }, deferred);
  let reads = 0;
  let closed = false;
  const input = {
    *[Symbol.iterator]() {
      try {
        while (reads < 100) yield reads++;
      } finally {
        closed = true;
      }
    },
  };
  const iterator = iterate(
    from(input).pipe(
      map((value) => gates[value].promise, {
        concurrency: 3,
        capacity: 5,
        ordered: true,
      }),
      filter((value) => value % 2 === 0),
      map((value) => value + 1),
    ),
  );
  const first = iterator.next();
  let delivered = false;
  first.then(() => {
    delivered = true;
  });
  await flush();
  assert.equal(reads, 3);
  gates[1].resolve(1);
  await flush();
  assert.equal(reads, 4);
  gates[2].resolve(2);
  await flush();
  assert.equal(reads, 5);
  gates[3].resolve(3);
  gates[4].resolve(4);
  await flush();
  assert.equal(delivered, false);
  assert.equal(reads, 5);
  gates[0].resolve(0);
  assert.equal((await first).value, 1);
  await flush();
  assert.equal(reads, 5, "no further demand means no background refill");
  await iterator.return();
  assert.equal(closed, true);
  assert.deepEqual((await iterator.closed).suppressedErrors, []);
});
