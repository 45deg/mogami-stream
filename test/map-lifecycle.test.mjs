import { test } from "node:test";
import assert from "node:assert/strict";
import * as stream from "../dist/index.js";
import { deferred, flush } from "./helpers.mjs";

test("concurrent map observes synchronous and thenable failures behind a pending head", async () => {
  for (const mode of ["throw", "then-getter", "then-reject"]) {
    const head = deferred();
    const failure = new Error(mode);
    let finalized = false;
    const iterator = stream.iterate(
      stream.of(0, 1).pipe(
        stream.map(
          (value) => {
            if (value === 0) return head.promise;
            if (mode === "throw") throw failure;
            if (mode === "then-getter") {
              return {
                get then() {
                  throw failure;
                },
              };
            }
            return { then: (_resolve, reject) => reject(failure) };
          },
          { concurrency: 2 },
        ),
        stream.finalize(() => {
          finalized = true;
        }),
      ),
    );
    await assert.rejects(iterator.next(), (error) => error === failure);
    await flush();
    assert.equal(finalized, false);
    head.resolve(0);
    const report = await iterator.closed;
    assert.equal(finalized, true);
    assert.equal(report.kind, "failed");
    assert.equal(report.reason, failure);
    assert.deepEqual(report.suppressedErrors, []);
  }
});

test("a mapper can cancel synchronously without refilling or abandoning its returned work", async () => {
  const controller = new AbortController();
  const work = deferred();
  const cleanup = deferred();
  const reason = new Error("cancel in mapper");
  const late = new Error("late mapper failure");
  let reads = 0;
  let returns = 0;
  let finalized = false;
  const source = {
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
  const iterator = stream.iterate(
    stream.from(source).pipe(
      stream.map(
        () => {
          controller.abort(reason);
          return work.promise;
        },
        { concurrency: 3 },
      ),
      stream.finalize(() => {
        finalized = true;
      }),
    ),
    { signal: controller.signal },
  );
  await assert.rejects(iterator.next(), (error) => error === reason);
  assert.equal((await iterator.next()).done, true);
  await flush();
  assert.equal(reads, 1);
  assert.equal(returns, 1);
  assert.equal(finalized, false);
  cleanup.resolve({ done: true });
  await flush();
  assert.equal(finalized, false);
  work.reject(late);
  const report = await iterator.closed;
  assert.equal(finalized, true);
  assert.equal(report.kind, "cancelled");
  assert.equal(report.reason, reason);
  assert.deepEqual(report.suppressedErrors, [late]);
});

test("stacked concurrent maps drain a large ready backlog without recursive refilling", async () => {
  const count = 12_000;
  const head = deferred();
  const started = deferred();
  const iterator = stream.iterate(
    stream.range(0, count).pipe(
      stream.map(
        (value) => {
          if (value === count - 1) started.resolve();
          return value === 0 ? head.promise : value;
        },
        { concurrency: count },
      ),
      stream.map((value) => value + 1, { concurrency: count }),
    ),
  );
  const first = iterator.next();
  await started.promise;
  await flush();
  head.resolve(0);
  assert.equal((await first).value, 1);
  for (let value = 2; value <= count; value++) {
    assert.equal((await iterator.next()).value, value);
  }
  assert.equal((await iterator.next()).done, true);
  assert.equal((await iterator.closed).kind, "completed");
});

test("cancellation across directly connected maps waits for both stages and retains their late errors", async () => {
  const controller = new AbortController();
  const upstreamWork = deferred();
  const downstreamWork = deferred();
  const reason = new Error("downstream cancellation");
  const upstreamError = new Error("late upstream mapper");
  const downstreamError = new Error("late downstream mapper");
  let finalized = false;
  let upstreamStarted = 0;
  const iterator = stream.iterate(
    stream.of(0, 1).pipe(
      stream.map(
        (value) => {
          upstreamStarted++;
          return value === 0 ? value : upstreamWork.promise;
        },
        { concurrency: 2 },
      ),
      stream.map(
        () => {
          controller.abort(reason);
          return downstreamWork.promise;
        },
        { concurrency: 2 },
      ),
      stream.finalize(() => {
        finalized = true;
      }),
    ),
    { signal: controller.signal },
  );
  await assert.rejects(iterator.next(), (error) => error === reason);
  assert.equal(upstreamStarted, 2);
  downstreamWork.reject(downstreamError);
  await flush();
  assert.equal(finalized, false);
  upstreamWork.reject(upstreamError);
  const report = await iterator.closed;
  assert.equal(finalized, true);
  assert.equal(report.reason, reason);
  assert.deepEqual(
    new Set(report.suppressedErrors),
    new Set([upstreamError, downstreamError]),
  );
});

for (const options of [
  { concurrency: 1, capacity: 1 },
  { concurrency: 1, capacity: 3 },
  { concurrency: 2, capacity: 2 },
]) {
  test(`map does not invoke its callback after a result getter aborts (${options.concurrency}/${options.capacity})`, async () => {
    const controller = new AbortController();
    const reason = new Error("cancel in value getter");
    const cleanup = deferred();
    let reads = 0;
    let returns = 0;
    const source = {
      [Symbol.iterator]() {
        return this;
      },
      next() {
        reads++;
        return {
          done: false,
          get value() {
            controller.abort(reason);
            return 7;
          },
        };
      },
      return() {
        returns++;
        return cleanup.promise;
      },
    };
    const iterator = stream
      .from(source)
      .pipe(
        stream.map(() => assert.fail("mapper started after abort"), options),
      )
      .iterate({ signal: controller.signal });
    await assert.rejects(iterator.next(), (error) => error === reason);
    let closed = false;
    iterator.closed.then(() => {
      closed = true;
    });
    await flush();
    assert.equal(reads, 1);
    assert.equal(returns, 1);
    assert.equal(closed, false);
    cleanup.resolve({ done: true });
    const report = await iterator.closed;
    assert.equal(report.kind, "cancelled");
    assert.equal(report.reason, reason);
    assert.deepEqual(report.suppressedErrors, []);
    assert.equal((await iterator.next()).done, true);
  });

  test(`map retries the same value without rereading its result getter (${options.concurrency}/${options.capacity})`, async () => {
    const values = [];
    let getterReads = 0;
    let pulled = false;
    const source = {
      [Symbol.iterator]() {
        return this;
      },
      next() {
        if (pulled) return { done: true };
        pulled = true;
        return {
          done: false,
          get value() {
            return ++getterReads;
          },
        };
      },
    };
    const output = await stream
      .from(source)
      .pipe(
        stream.map(
          (value, { attempt }) => {
            values.push(value);
            if (attempt === 1) throw new Error("retry");
            return value;
          },
          { ...options, retry: { retries: 1 } },
        ),
      )
      .toArray();
    assert.equal(getterReads, 1);
    assert.deepEqual(values, [1, 1]);
    assert.deepEqual(output, [1]);
  });
}
