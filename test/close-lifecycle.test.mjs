import test from "node:test";
import assert from "node:assert/strict";
import {
  finalize,
  from,
  iterate,
  defer,
  take,
  takeWhile,
  broadcast,
} from "../dist/index.js";
import { deferred, flush } from "./helpers.mjs";

const rejectsWith = (promise, reason) =>
  assert.rejects(promise, (error) => Object.is(error, reason));

for (const reason of [undefined, NaN, new Error("throw during return")]) {
  test(`throw(${String(reason)}) during return promotes completion to failure`, async () => {
    const cleanup = deferred();
    const started = deferred();
    const iterator = iterate(
      from([1]).pipe(
        finalize(() => {
          started.resolve();
          return cleanup.promise;
        }),
      ),
    );
    await iterator.next();
    let returned = false;
    const returning = iterator.return().then(
      () => {
        returned = true;
        return { rejected: false };
      },
      (error) => {
        returned = true;
        return { rejected: true, reason: error };
      },
    );
    await started.promise;
    await rejectsWith(iterator.throw(reason), reason);
    assert.equal(returned, false);
    cleanup.resolve();
    assert.deepEqual(await returning, { rejected: true, reason });
    assert.deepEqual(await iterator.closed, {
      kind: "failed",
      reason,
      suppressedErrors: [],
    });
  });
}

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

for (const mode of [
  "take",
  "takeWhile",
  "return",
  "detach",
  "close",
  "cancel",
  "external",
]) {
  for (const sameReason of [true, false]) {
    test(`${mode}: upstream abort preserves completion and suppresses only the identical reason (${sameReason})`, async () => {
      const controller = new AbortController();
      const externalReason = new Error("external stop");
      const distinct = new Error("distinct abort failure");
      distinct.name = "AbortError";
      let upstreamSignal;
      let cleanup = 0;
      const source = defer(({ signal }) => {
        upstreamSignal = signal;
        return {
          [Symbol.iterator]() {
            return this;
          },
          next() {
            return { done: false, value: 1 };
          },
          return() {
            cleanup++;
            assert.equal(signal.aborted, true);
            throw sameReason ? signal.reason : distinct;
          },
        };
      });
      const grouped = mode === "detach" || mode === "close";
      const group = grouped
        ? broadcast(source, { branches: 1, capacity: 1 })
        : undefined;
      const pipeline =
        mode === "take"
          ? source.pipe(take(1))
          : mode === "takeWhile"
            ? source.pipe(takeWhile(() => false))
            : source;
      const iterator = group
        ? group.branches[0].iterate()
        : pipeline.iterate({ signal: controller.signal });
      if (mode === "take" || mode === "takeWhile") {
        const consuming = (async () => {
          for await (const value of iterator) assert.equal(value, 1);
        })();
        if (sameReason) await consuming;
        else await rejectsWith(consuming, distinct);
      } else {
        await iterator.next();
        if (mode === "return") {
          const returning = iterator.return();
          if (sameReason) await returning;
          else await rejectsWith(returning, distinct);
        } else if (mode === "detach") group.branches[0].detach();
        else if (mode === "close") await group.close();
        else if (mode === "cancel") iterator.cancel();
        else controller.abort(externalReason);
      }
      const report = group
        ? (await group.closed).source
        : await iterator.closed;
      assert.equal(upstreamSignal.aborted, true);
      if (mode === "external")
        assert.equal(upstreamSignal.reason, externalReason);
      else assert.equal(upstreamSignal.reason.name, "AbortError");
      const cancelled = mode === "cancel" || mode === "external";
      assert.equal(
        report.kind,
        cancelled ? "cancelled" : sameReason ? "completed" : "failed",
      );
      if (cancelled) assert.equal(report.reason, upstreamSignal.reason);
      else if (!sameReason) assert.equal(report.reason, distinct);
      assert.deepEqual(
        report.suppressedErrors,
        cancelled && !sameReason ? [distinct] : [],
      );
      assert.equal(cleanup, 1);
      if (group)
        assert.equal((await group.branches[0].closed).kind, "completed");
    });
  }
}

test("an upstream signal passed directly to fetch aborts I/O without failing normal return", async () => {
  let signal;
  const iterator = defer((context) => {
    signal = context.signal;
    return [1, 2];
  }).iterate();
  await iterator.next();
  await iterator.return();
  // A pre-aborted signal exercises fetch's rejection contract without network I/O.
  await rejectsWith(fetch("http://127.0.0.1:1/", { signal }), signal.reason);
  assert.deepEqual(await iterator.closed, {
    kind: "completed",
    suppressedErrors: [],
  });
});
