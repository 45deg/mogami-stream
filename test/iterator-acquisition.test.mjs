import assert from "node:assert/strict";
import { test } from "node:test";
import * as stream from "../dist/index.js";
import { deferred, flush } from "./helpers.mjs";

for (const protocol of [Symbol.iterator, Symbol.asyncIterator]) {
  for (const [route, iterate] of [
    ["direct", (source, options) => stream.iterate(source, options)],
    ["from", (source, options) => stream.from(source).iterate(options)],
    ["chain", (source, options) => stream.chain(source).iterate(options)],
  ]) {
    test(`${String(protocol)} next getter cancellation prevents ${route} read invocation`, async () => {
      const controller = new AbortController();
      const cleanup = deferred();
      const reason = new Error("cancel during next lookup");
      const calls = [];
      const iterator = iterate(
        {
          [protocol]() {
            return {
              get next() {
                calls.push("lookup");
                controller.abort(reason);
                return () => {
                  calls.push("next");
                  return { done: false, value: 42 };
                };
              },
              return() {
                calls.push("return");
                return cleanup.promise;
              },
            };
          },
        },
        { signal: controller.signal },
      );
      await assert.rejects(iterator.next(), (error) => error === reason);
      let closed = false;
      void iterator.closed.then(() => {
        closed = true;
      });
      await flush();
      const beforeCleanup = [...calls];
      const closedBeforeCleanup = closed;
      cleanup.resolve({ done: true });
      assert.deepEqual(await iterator.closed, {
        kind: "cancelled",
        reason,
        suppressedErrors: [],
      });
      assert.equal(closedBeforeCleanup, false);
      assert.deepEqual(beforeCleanup, ["lookup", "return"]);
      assert.deepEqual(calls, ["lookup", "return"]);
    });
  }
}

for (const protocol of [Symbol.asyncIterator, Symbol.iterator]) {
  const label = protocol === Symbol.asyncIterator ? "async" : "sync";

  for (const [route, iterate] of [
    ["direct", (source, options) => stream.iterate(source, options)],
    ["from", (source, options) => stream.from(source).iterate(options)],
    ["defer", (source, options) => stream.defer(() => source).iterate(options)],
  ]) {
    test(`${label} method getter cancellation prevents ${route} factory acquisition`, async () => {
      const controller = new AbortController();
      const reason = new Error("cancel during lookup");
      const calls = [];
      const source = {
        get [protocol]() {
          calls.push("lookup");
          controller.abort(reason);
          return () => {
            calls.push("factory");
            return {
              next: () => assert.fail("must not read after cancellation"),
              return() {
                calls.push("return");
                return { done: true };
              },
            };
          };
        },
      };
      const iterator = iterate(source, { signal: controller.signal });
      assert.deepEqual(calls, []);
      await assert.rejects(iterator.next(), (error) => error === reason);
      assert.deepEqual(await iterator.closed, {
        kind: "cancelled",
        reason,
        suppressedErrors: [],
      });
      await iterator.return();
      assert.deepEqual(calls, ["lookup"]);
    });
  }

  test(`${label} factory cancellation still owns and awaits one iterator cleanup`, async () => {
    const controller = new AbortController();
    const reason = new Error("cancel during factory");
    const cleanup = deferred();
    const cleanupFailure = new Error("cleanup failed");
    const calls = [];
    const iterator = stream
      .from({
        [protocol]() {
          calls.push("factory");
          controller.abort(reason);
          return {
            next: () => assert.fail("must not read after cancellation"),
            return() {
              calls.push("return");
              return cleanup.promise;
            },
          };
        },
      })
      .iterate({ signal: controller.signal });
    await assert.rejects(iterator.next(), (error) => error === reason);
    let closed = false;
    void iterator.closed.then(() => {
      closed = true;
    });
    await flush();
    assert.equal(closed, false);
    assert.deepEqual(calls, ["factory", "return"]);
    cleanup.reject(cleanupFailure);
    assert.deepEqual(await iterator.closed, {
      kind: "cancelled",
      reason,
      suppressedErrors: [cleanupFailure],
    });
    await iterator.return();
    assert.deepEqual(calls, ["factory", "return"]);
  });

  for (const boundary of ["getter", "factory"]) {
    for (const abort of [false, true]) {
      test(`${label} ${boundary} failure ${abort ? "after cancellation" : "during acquisition"} preserves the primary reason`, async () => {
        const controller = new AbortController();
        const reason = new Error("cancel acquisition");
        const failure = new Error(`${boundary} failed`);
        const fail = () => {
          if (abort) controller.abort(reason);
          throw failure;
        };
        const source = {
          get [protocol]() {
            if (boundary === "getter") fail();
            return fail;
          },
          get return() {
            assert.fail("a failed acquisition owns no iterator");
          },
        };
        const iterator = stream
          .from(source)
          .iterate({ signal: controller.signal });
        await assert.rejects(
          iterator.next(),
          (error) => error === (abort ? reason : failure),
        );
        assert.deepEqual(await iterator.closed, {
          kind: abort ? "cancelled" : "failed",
          reason: abort ? reason : failure,
          suppressedErrors: abort ? [failure] : [],
        });
      });
    }
  }

  test(`${label} acquisition is lazy, keeps its receiver, and skips pre-aborted sources`, async () => {
    const calls = [];
    const source = {
      get [protocol]() {
        calls.push("lookup");
        return function () {
          assert.equal(this, source);
          calls.push("factory");
          return [42][Symbol.iterator]();
        };
      },
    };
    const iterator = stream.from(source).iterate();
    assert.deepEqual(calls, []);
    assert.deepEqual(await iterator.next(), { done: false, value: 42 });
    assert.deepEqual(calls, ["lookup", "factory"]);
    assert.equal((await iterator.next()).done, true);
    assert.equal((await iterator.closed).kind, "completed");
    const controller = new AbortController();
    const reason = new Error("already cancelled");
    controller.abort(reason);
    const stopped = stream.from(source).iterate({ signal: controller.signal });
    await assert.rejects(stopped.next(), (error) => error === reason);
    assert.equal((await stopped.closed).reason, reason);
    assert.deepEqual(calls, ["lookup", "factory"]);
  });
}

for (const missing of [undefined, null]) {
  test(`an async getter returning ${missing} after cancellation prevents sync fallback lookup`, async () => {
    const controller = new AbortController();
    const reason = "cancel before fallback";
    const source = {
      get [Symbol.asyncIterator]() {
        controller.abort(reason);
        return missing;
      },
      get [Symbol.iterator]() {
        assert.fail("must not inspect the fallback after cancellation");
      },
    };
    const iterator = stream.from(source).iterate({ signal: controller.signal });
    await assert.rejects(iterator.next(), (error) => error === reason);
    assert.deepEqual(await iterator.closed, {
      kind: "cancelled",
      reason,
      suppressedErrors: [],
    });
  });

  test(`an async getter returning ${missing} normally uses the sync fallback`, async () => {
    const calls = [];
    const source = {
      get [Symbol.asyncIterator]() {
        calls.push("async lookup");
        return missing;
      },
      get [Symbol.iterator]() {
        calls.push("sync lookup");
        return function () {
          assert.equal(this, source);
          calls.push("sync factory");
          return [42][Symbol.iterator]();
        };
      },
    };
    assert.deepEqual(await stream.from(source).toArray(), [42]);
    assert.deepEqual(calls, ["async lookup", "sync lookup", "sync factory"]);
  });
}

test("an async factory takes precedence over the sync protocol", async () => {
  assert.deepEqual(
    await stream
      .from({
        [Symbol.asyncIterator]: () => [42][Symbol.iterator](),
        get [Symbol.iterator]() {
          assert.fail("must not inspect the sync protocol");
        },
      })
      .toArray(),
    [42],
  );
});

for (const protocol of [Symbol.asyncIterator, Symbol.iterator]) {
  test(`a non-callable ${String(protocol)} method fails without fallback`, async () => {
    const source = { [protocol]: 42 };
    if (protocol === Symbol.asyncIterator) {
      Object.defineProperty(source, Symbol.iterator, {
        get() {
          assert.fail("an invalid async method must not fall back");
        },
      });
    }
    const iterator = stream.from(source).iterate();
    await assert.rejects(iterator.next(), TypeError);
    assert.equal((await iterator.closed).kind, "failed");
  });
}
