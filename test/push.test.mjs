import assert from "node:assert/strict";
import { test } from "node:test";
import { EventEmitter } from "node:events";
import { fromCallback, fromEvent, take } from "../dist/index.js";
import { deferred, flush } from "./helpers.mjs";

for (const overflow of ["drop-oldest", "drop-newest"]) {
  test(`push queue ${overflow} bounds synchronous bursts and drains on completion`, async () => {
    let cleaned = 0;
    const accepted = [];
    const source = fromCallback(
      (observer) => {
        for (let i = 0; i < 100; i++) accepted.push(observer.next(i));
        observer.complete();
        assert.equal(observer.next(101), false);
        return () => {
          cleaned++;
        };
      },
      { capacity: 3, overflow },
    );
    assert.equal(cleaned, 0);
    assert.deepEqual(
      await source.toArray(),
      overflow === "drop-oldest" ? [97, 98, 99] : [0, 1, 2],
    );
    assert.equal(cleaned, 1);
    assert.equal(
      accepted.filter(Boolean).length,
      overflow === "drop-oldest" ? 100 : 3,
    );
  });
}

test("overflow fails and unsubscribes even during synchronous setup", async () => {
  let cleaned = 0;
  await assert.rejects(
    fromCallback(
      (observer) => {
        observer.next(1);
        assert.equal(observer.next(2), false);
        return () => {
          cleaned++;
        };
      },
      { capacity: 1 },
    ).toArray(),
    /capacity exceeded/,
  );
  assert.equal(cleaned, 1);
});

test("pending push read cancels and closed waits for asynchronous cleanup", async () => {
  const cleanup = deferred();
  let observer;
  let cleaned = 0;
  const iterator = fromCallback(
    (sink) => {
      observer = sink;
      return () => {
        cleaned++;
        return cleanup.promise;
      };
    },
    { capacity: 1 },
  ).iterate();
  const pending = assert.rejects(iterator.next(), (error) => error === "stop");
  await flush();
  iterator.cancel("stop");
  await pending;
  let closed = false;
  iterator.closed.then(() => {
    closed = true;
  });
  await flush();
  assert.equal(closed, false);
  assert.equal(cleaned, 1);
  assert.equal(observer.signal.aborted, true);
  assert.equal(observer.next(1), false);
  cleanup.resolve();
  await iterator.closed;
  assert.equal(cleaned, 1);
});

test("push failures discard queued values and preserve cleanup failures", async () => {
  let observer;
  const failure = new Error("producer failed");
  const cleanupFailure = new Error("cleanup failed");
  const iterator = fromCallback(
    (sink) => {
      observer = sink;
      return () => {
        throw cleanupFailure;
      };
    },
    { capacity: 2 },
  ).iterate();
  const pending = iterator.next();
  await flush();
  const value = Promise.resolve(1);
  observer.next(value);
  assert.equal((await pending).value, value);
  observer.next(2);
  observer.error(failure);
  await assert.rejects(iterator.next(), (error) => error === failure);
  const report = await iterator.closed;
  assert.ok(report.suppressedErrors.includes(cleanupFailure));
});

test("fromEvent subscribes lazily, isolates executions, and removes listeners", async () => {
  class Target extends EventTarget {
    listeners = 0;
    addEventListener(...args) {
      this.listeners++;
      super.addEventListener(...args);
    }
    removeEventListener(...args) {
      this.listeners--;
      super.removeEventListener(...args);
    }
  }
  const target = new Target();
  const source = fromEvent(target, "value", { capacity: 2 }).pipe(take(1));
  assert.equal(target.listeners, 0);
  const first = source.toArray();
  const second = source.toArray();
  await flush();
  assert.equal(target.listeners, 2);
  const event = new Event("value");
  target.dispatchEvent(event);
  assert.deepEqual(await first, [event]);
  assert.deepEqual(await second, [event]);
  assert.equal(target.listeners, 0);
});

test("fromCallback adapts EventEmitter with explicit cleanup", async () => {
  const emitter = new EventEmitter();
  const source = fromCallback(
    (observer) => {
      const listener = (value) => observer.next(value);
      emitter.on("data", listener);
      return () => {
        emitter.off("data", listener);
      };
    },
    { capacity: 2 },
  ).pipe(take(1));
  const result = source.toArray();
  await flush();
  emitter.emit("data", undefined);
  assert.deepEqual(await result, [undefined]);
  assert.equal(emitter.listenerCount("data"), 0);
});

test("push completion wakes a pending read and setup errors propagate", async () => {
  let observer;
  const result = fromCallback(
    (sink) => {
      observer = sink;
    },
    { capacity: 1 },
  ).toArray();
  await flush();
  observer.complete();
  assert.deepEqual(await result, []);
  const failure = new Error("setup failed");
  await assert.rejects(
    fromCallback(
      () => {
        throw failure;
      },
      { capacity: 1 },
    ).toArray(),
    (error) => error === failure,
  );
});

test("push options reject invalid capacity and policy", () => {
  for (const capacity of [0, -1, 1.5, Infinity, NaN])
    assert.throws(() => fromCallback(() => {}, { capacity }), RangeError);
  assert.throws(
    () => fromCallback(() => {}, { capacity: 1, overflow: "block" }),
    TypeError,
  );
});

test("push errors recover after unsubscribe, including an idle producer failure", async () => {
  const { catchError } = await import("../dist/index.js");
  let observer;
  let cleaned = false;
  const source = fromCallback(
    (sink) => {
      observer = sink;
      return () => {
        cleaned = true;
      };
    },
    { capacity: 2 },
  ).pipe(
    catchError((error) => {
      assert.equal(error, "failed");
      assert.equal(cleaned, true);
      return [9];
    }),
  );
  const iterator = source.iterate();
  const first = iterator.next();
  await flush();
  observer.next(1);
  assert.equal((await first).value, 1);
  observer.error("failed");
  assert.equal((await iterator.next()).value, 9);
  assert.equal((await iterator.next()).done, true);
  assert.deepEqual((await iterator.closed).suppressedErrors, []);
});

test("push ring reuses slots across interleaved reads and preserves falsy values", async () => {
  let observer;
  const iterator = fromCallback(
    (sink) => {
      observer = sink;
    },
    {
      capacity: 2,
      overflow: "drop-oldest",
    },
  ).iterate();
  const first = iterator.next();
  await flush();
  observer.next(null);
  assert.equal((await first).value, null);
  observer.next(undefined);
  observer.next(false);
  assert.equal((await iterator.next()).value, undefined);
  observer.next(0);
  observer.next("");
  observer.complete();
  assert.equal((await iterator.next()).value, 0);
  assert.equal((await iterator.next()).value, "");
  assert.equal((await iterator.next()).done, true);
  await iterator.closed;
});
