import test from "node:test";
import assert from "node:assert/strict";
import { finalize, from, iterate } from "../dist/index.js";
import { deferred, flush } from "./helpers.mjs";

const rejectsWith = (promise, reason) =>
  assert.rejects(promise, (error) => Object.is(error, reason));

for (const pendingRead of [false, true]) {
  test(`throw rejects before gated cleanup finishes (pending read: ${pendingRead})`, async () => {
    const cleanup = deferred();
    const read = deferred();
    const failure = new Error("throw");
    let returns = 0;
    let throws = 0;
    const iterator = iterate({
      [Symbol.asyncIterator]() {
        return this;
      },
      next() {
        return pendingRead ? read.promise : { value: 1, done: false };
      },
      return() {
        returns++;
        return cleanup.promise;
      },
      throw() {
        throws++;
        throw new Error("must not forward");
      },
    });
    const next = iterator.next();
    if (!pendingRead) await next;
    const rejectedRead = pendingRead ? rejectsWith(next, failure) : undefined;
    let closed = false;
    void iterator.closed.then(() => {
      closed = true;
    });
    await rejectsWith(iterator.throw(failure), failure);
    await rejectedRead;
    assert.equal(closed, false);
    assert.equal(returns, 1);
    assert.equal(throws, 0);
    cleanup.resolve({ done: true });
    if (pendingRead) {
      await flush();
      assert.equal(closed, false);
      read.resolve({ done: true });
    }
    assert.deepEqual(await iterator.closed, {
      kind: "failed",
      reason: failure,
      suppressedErrors: [],
    });
    assert.deepEqual(await iterator.next(), { done: true, value: undefined });
  });
}

for (const cancel of [false, true]) {
  test(`return waits for cleanup and preserves ${cancel ? "cancellation" : "cleanup failure"}`, async () => {
    const cleanup = deferred();
    const started = deferred();
    const failure = new Error("cleanup");
    const iterator = iterate(
      from([1]).pipe(
        finalize(() => {
          started.resolve();
          return cleanup.promise;
        }),
      ),
    );
    await iterator.next();
    if (cancel) iterator.cancel("cancelled");
    const returning = iterator.return();
    const result = cancel ? returning : rejectsWith(returning, failure);
    let settled = false;
    void result.then(() => {
      settled = true;
    });
    await started.promise;
    assert.equal(settled, false);
    cleanup.reject(failure);
    await result;
    if (cancel)
      assert.deepEqual(await returning, { done: true, value: undefined });
    assert.deepEqual(
      await iterator.closed,
      cancel
        ? {
            kind: "cancelled",
            reason: "cancelled",
            suppressedErrors: [failure],
          }
        : { kind: "failed", reason: failure, suppressedErrors: [] },
    );
  });
}

for (const primary of [new Error("primary"), undefined, NaN]) {
  for (const mode of ["exhaust", "return", "throw", "cancel"]) {
    test(`${mode} preserves ${String(primary)} and deduplicates cleanup errors`, async () => {
      const secondary = new Error("secondary");
      const errors = [
        primary,
        primary,
        secondary,
        secondary,
        NaN,
        NaN,
        undefined,
        undefined,
      ];
      const iterator = iterate(
        from([1]).pipe(
          ...errors.map((error) =>
            finalize(() => {
              throw error;
            }),
          ),
        ),
      );
      await iterator.next();
      let reason = primary;
      if (mode === "cancel") {
        // cancel(undefined) intentionally selects the default AbortError.
        iterator.cancel(primary);
        await iterator.return();
        reason = (await iterator.closed).reason;
      } else if (mode === "throw") {
        await rejectsWith(iterator.throw(primary), primary);
      } else if (mode === "return") {
        await rejectsWith(iterator.return(), primary);
      } else {
        await rejectsWith(iterator.next(), primary);
      }
      const expected = [];
      for (const error of errors) {
        if (!Object.is(error, reason) && !expected.includes(error))
          expected.push(error);
      }
      assert.deepEqual(await iterator.closed, {
        kind: mode === "cancel" ? "cancelled" : "failed",
        reason,
        suppressedErrors: expected,
      });
    });
  }
}

test("throw after cancellation preserves its reason while cleanup is pending", async () => {
  const cleanup = deferred();
  const secondary = new Error("later throw");
  const iterator = iterate(from([1]).pipe(finalize(() => cleanup.promise)));
  await iterator.next();
  iterator.cancel(NaN);
  await rejectsWith(iterator.throw(secondary), NaN);
  cleanup.resolve();
  assert.deepEqual(await iterator.closed, {
    kind: "cancelled",
    reason: NaN,
    suppressedErrors: [secondary],
  });
});
