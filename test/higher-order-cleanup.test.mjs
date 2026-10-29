import { test } from "node:test";
import assert from "node:assert/strict";
import * as s from "../dist/index.js";
import { controlled, deferred, flush } from "./helpers.mjs";

function combine(name, concurrency = 2) {
  return name.endsWith("Map")
    ? s[name]((source) => source, { concurrency })
    : s[name]({ concurrency });
}

for (const name of ["flatten", "flatMap"]) {
  test(`${name} reserves cleanup slots until each closes independently`, async () => {
    const cleanups = [deferred(), deferred()];
    let accepted = 0;
    function* outer() {
      for (const cleanup of cleanups) {
        accepted++;
        yield s.empty().pipe(s.finalize(() => cleanup.promise));
      }
      accepted++;
      yield [42];
    }
    const iterator = s.from(outer()).pipe(combine(name)).iterate();
    try {
      let result;
      const reading = iterator.next().then((value) => (result = value));
      await flush();
      assert.equal(accepted, 2);
      assert.equal(result, undefined);
      // The later child frees its own slot without waiting for the first child.
      cleanups[1].resolve();
      await flush();
      assert.deepEqual(result, { done: false, value: 42 });
      await reading;
      assert.equal(accepted, 3);
    } finally {
      for (const cleanup of cleanups) cleanup.resolve();
      await iterator.return();
    }
  });

  test(`${name} progresses siblings while cleanup retains its concurrency slot`, async () => {
    const cleanup = deferred();
    let accepted = 0;
    let reads = 0;
    let finalizers = 0;
    const empty = s.empty().pipe(
      s.finalize(() => {
        finalizers++;
        return cleanup.promise;
      }),
    );
    const sibling = {
      *[Symbol.iterator]() {
        for (const value of [42, 43]) {
          reads++;
          yield value;
        }
      },
    };
    function* outer() {
      for (const child of [empty, sibling, [99]]) {
        accepted++;
        yield child;
      }
    }
    const iterator = s.from(outer()).pipe(combine(name)).iterate();
    try {
      let first;
      const reading = iterator.next().then((result) => (first = result));
      await flush();
      assert.deepEqual(first, { done: false, value: 42 });
      await reading;
      assert.equal(finalizers, 1);
      assert.equal(accepted, 2);
      assert.equal(reads, 1);
      assert.equal((await iterator.next()).value, 43);
      await flush();
      assert.equal(accepted, 2);
      assert.equal(reads, 2);
      // Releasing cleanup without downstream demand must not pull a new child.
      cleanup.resolve();
      await flush();
      assert.equal(accepted, 2);
      assert.equal((await iterator.next()).value, 99);
      assert.equal((await iterator.next()).done, true);
      assert.equal(accepted, 3);
      assert.equal(finalizers, 1);
    } finally {
      cleanup.resolve();
      await iterator.return();
    }
  });

  test(`${name} waits for all child cleanup before normal completion`, async () => {
    const cleanup = deferred();
    const iterator = s
      .of(s.empty().pipe(s.finalize(() => cleanup.promise)), [42])
      .pipe(combine(name))
      .iterate();
    try {
      let first;
      iterator.next().then((result) => (first = result));
      await flush();
      assert.deepEqual(first, { done: false, value: 42 });
      let ended = false;
      let closed = false;
      const ending = iterator.next().then((result) => {
        ended = true;
        return result;
      });
      iterator.closed.then(() => (closed = true));
      await flush();
      assert.equal(ended, false);
      assert.equal(closed, false);
      cleanup.resolve();
      assert.equal((await ending).done, true);
      assert.deepEqual(await iterator.closed, {
        kind: "completed",
        suppressedErrors: [],
      });
    } finally {
      cleanup.resolve();
      await iterator.return();
    }
  });
}

for (const [name, concurrency] of [
  ["concat", 2],
  ["concatMap", 2],
  ["flatten", 1],
  ["flatMap", 1],
]) {
  test(`${name} with concurrency ${concurrency} waits for leading cleanup`, async () => {
    const cleanup = deferred();
    let accepted = 0;
    function* outer() {
      for (const child of [
        s.empty().pipe(s.finalize(() => cleanup.promise)),
        [42, 43],
        [99],
      ]) {
        accepted++;
        yield child;
      }
    }
    const iterator = s.from(outer()).pipe(combine(name, concurrency)).iterate();
    try {
      let received = false;
      const first = iterator.next().then((result) => {
        received = true;
        return result;
      });
      await flush();
      assert.equal(received, false);
      assert.equal(accepted, concurrency);
      cleanup.resolve();
      assert.equal((await first).value, 42);
      assert.equal((await iterator.next()).value, 43);
      assert.equal((await iterator.next()).value, 99);
      assert.equal((await iterator.next()).done, true);
    } finally {
      cleanup.resolve();
      await iterator.return();
    }
  });
}

for (const termination of ["cancel", "child failure", "cleanup failure"]) {
  test(`flatten drains cleanup once and reports errors after ${termination}`, async () => {
    const cleanup = deferred();
    const sibling = controlled();
    const primary = new Error(termination);
    const secondary = new Error("cleanup failed");
    let finalizers = 0;
    const iterator = s
      .of(
        s.empty().pipe(
          s.finalize(() => {
            finalizers++;
            return cleanup.promise;
          }),
        ),
        sibling.source,
      )
      .pipe(s.flatten({ concurrency: 2 }))
      .iterate();
    const reading = iterator.next();
    const rejected = assert.rejects(reading, (error) => error === primary);
    await flush();
    assert.equal(finalizers, 1);
    assert.equal(sibling.reads.length, 1);
    let closed = false;
    iterator.closed.then(() => (closed = true));
    if (termination === "cancel") iterator.cancel(primary);
    else if (termination === "child failure") sibling.reads[0].reject(primary);
    else cleanup.reject(primary);
    await rejected;
    await flush();
    assert.equal(closed, false);
    assert.equal(sibling.returns, 1);
    if (termination !== "cleanup failure") cleanup.reject(secondary);
    sibling.reads[0].resolve({ done: true });
    const report = await iterator.closed;
    assert.equal(
      report.kind,
      termination === "cancel" ? "cancelled" : "failed",
    );
    assert.equal(report.reason, primary);
    assert.deepEqual(
      report.suppressedErrors,
      termination === "cleanup failure" ? [] : [secondary],
    );
    assert.equal(finalizers, 1);
    assert.equal(sibling.returns, 1);
    assert.equal((await iterator.next()).done, true);
  });
}

for (const name of ["concat", "flatten"]) {
  test(`${name} retires and refills uneven children without losing per-child order`, async () => {
    const children = Array.from({ length: 2048 }, (_, id) =>
      Array.from({ length: (id * 17) % 7 }, (_, sequence) => ({
        id,
        sequence,
      })),
    );
    const result = await s
      .from(children)
      .pipe(s[name]({ concurrency: 128 }))
      .toArray();
    if (name === "concat") assert.deepEqual(result, children.flat());
    else {
      const next = new Uint8Array(children.length);
      for (const { id, sequence } of result) assert.equal(sequence, next[id]++);
      assert.deepEqual(
        Array.from(next),
        children.map((child) => child.length),
      );
    }
  });
}
