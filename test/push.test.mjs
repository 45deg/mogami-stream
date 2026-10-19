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

for (const overflow of ["drop-oldest", "drop-newest"]) {
  test(`${overflow} reports live, per-execution counts without retaining dropped values`, async () => {
    const infos = [];
    const counts = [];
    const source = fromCallback(
      (observer) => {
        for (let value = 0; value < 5; value++) observer.next({ value });
        observer.complete();
        assert.equal(observer.next({ value: 99 }), false);
      },
      {
        capacity: 2,
        overflow,
        onDrop(info) {
          infos.push(info);
          counts.push(info.count);
          assert.equal(info.overflow, overflow);
          assert.deepEqual(Object.keys(info).sort(), [
            "count",
            "overflow",
            "signal",
          ]);
          assert.equal(Object.isFrozen(info), true);
        },
      },
    );
    const expected = overflow === "drop-oldest" ? [3, 4] : [0, 1];
    assert.deepEqual(
      (await source.toArray()).map(({ value }) => value),
      expected,
    );
    assert.deepEqual(counts, [1, 2, 3]);
    assert.ok(infos.every((info) => info === infos[0]));
    assert.equal(infos[0].count, 3);
    await source.toArray();
    assert.deepEqual(counts, [1, 2, 3, 1, 2, 3]);
    assert.notEqual(infos[0], infos[3]);
    assert.notEqual(infos[0].signal, infos[3].signal);
    assert.equal(infos[0].count, 3);
  });

  test(`${overflow} bounds reentrant notification depth while counting every drop`, async () => {
    let observer;
    let info;
    let calls = 0;
    const accepted = [];
    const iterator = fromCallback(
      (sink) => {
        observer = sink;
      },
      {
        capacity: 1,
        overflow,
        onDrop(current) {
          calls++;
          info = current;
          if (calls === 1) {
            for (let value = 3; value < 20003; value++) {
              accepted.push(observer.next(value));
            }
          }
        },
      },
    ).iterate();
    const first = iterator.next();
    observer.next(0);
    assert.equal((await first).value, 0);
    assert.equal(observer.next(1), true);
    assert.equal(observer.next(2), overflow === "drop-oldest");
    assert.equal(calls, 1);
    assert.equal(info.count, 20001);
    assert.ok(
      accepted.every((value) => value === (overflow === "drop-oldest")),
    );
    assert.equal(
      (await iterator.next()).value,
      overflow === "drop-oldest" ? 20002 : 1,
    );
    observer.next(4);
    observer.next(5);
    assert.equal(calls, 2);
    assert.equal(info.count, 20002);
    observer.complete();
    await iterator.return();
    assert.equal(observer.next(6), false);
    assert.equal(info.count, 20002);
    assert.equal(calls, 2);
  });

  test(`${overflow} hook throws fail setup without escaping next or losing cleanup`, async () => {
    const failure = new Error("drop hook failed");
    let cleaned = 0;
    let info;
    const acceptance = [];
    await assert.rejects(
      fromCallback(
        (observer) => {
          acceptance.push(observer.next(1));
          acceptance.push(observer.next(2));
          acceptance.push(observer.next(3));
          observer.complete();
          return () => {
            cleaned++;
          };
        },
        {
          capacity: 1,
          overflow,
          onDrop(current) {
            info = current;
            throw failure;
          },
        },
      ).toArray(),
      (error) => error === failure,
    );
    assert.deepEqual(acceptance, [true, overflow === "drop-oldest", false]);
    assert.equal(cleaned, 1);
    assert.equal(info.count, 1);
    assert.equal(info.signal.aborted, true);
  });

  test(`${overflow} cancellation inside the hook stops later emissions and preserves later throws`, async () => {
    const controller = new AbortController();
    const failure = new Error("after cancellation");
    let observer;
    let info;
    let calls = 0;
    let cleaned = 0;
    const iterator = fromCallback(
      (sink) => {
        observer = sink;
        return () => {
          cleaned++;
        };
      },
      {
        capacity: 1,
        overflow,
        onDrop(current) {
          calls++;
          info = current;
          controller.abort("stop");
          assert.equal(observer.next(3), false);
          throw failure;
        },
      },
    ).iterate({ signal: controller.signal });
    const first = iterator.next();
    observer.next(0);
    await first;
    observer.next(1);
    assert.equal(observer.next(2), overflow === "drop-oldest");
    assert.equal(observer.next(4), false);
    await assert.rejects(iterator.next(), (error) => error === "stop");
    const report = await iterator.closed;
    assert.equal(report.kind, "cancelled");
    assert.equal(report.reason, "stop");
    assert.deepEqual(report.suppressedErrors, [failure]);
    assert.equal(cleaned, 1);
    assert.equal(info.count, 1);
    assert.equal(calls, 1);
  });
}

test("drop-oldest commits the replacement before a hook reads or completes the source", async () => {
  let observer;
  let fromHook;
  let info;
  const iterator = fromCallback(
    (sink) => {
      observer = sink;
    },
    {
      capacity: 1,
      overflow: "drop-oldest",
      onDrop(current) {
        info = current;
        fromHook = iterator.next();
        observer.complete();
        assert.equal(observer.next(99), false);
      },
    },
  ).iterate();
  const first = iterator.next();
  observer.next(0);
  await first;
  observer.next(1);
  assert.equal(observer.next(2), true);
  assert.equal((await fromHook).value, 2);
  assert.equal((await iterator.next()).done, true);
  assert.equal(info.count, 1);
  await iterator.closed;
});

test("overflow errors and lifecycle discards never notify onDrop", async () => {
  let calls = 0;
  await assert.rejects(
    fromCallback(
      (observer) => {
        observer.next(1);
        observer.next(2);
      },
      {
        capacity: 1,
        onDrop() {
          calls++;
        },
      },
    ).toArray(),
    /capacity exceeded/,
  );
  for (const ending of ["complete", "error", "cancel"]) {
    let observer;
    const iterator = fromCallback(
      (sink) => {
        observer = sink;
      },
      {
        capacity: 1,
        overflow: "drop-oldest",
        onDrop() {
          calls++;
        },
      },
    ).iterate();
    const first = iterator.next();
    observer.next(0);
    await first;
    observer.next(1);
    if (ending === "cancel") iterator.cancel("stop");
    else if (ending === "error") observer.error("failed");
    else {
      observer.complete();
      await iterator.next();
      await iterator.next();
    }
    await iterator.closed;
    assert.equal(observer.next(2), false);
  }
  assert.equal(calls, 0);
});

test("fromEvent exposes separate live drop statistics for concurrent executions", async () => {
  const target = new EventTarget();
  const infos = [];
  const source = fromEvent(target, "value", {
    capacity: 1,
    overflow: "drop-oldest",
    onDrop(info) {
      infos.push(info);
    },
  });
  const first = source.iterate();
  const second = source.iterate();
  const pending = [first.next(), second.next()];
  target.dispatchEvent(new Event("value"));
  await Promise.all(pending);
  target.dispatchEvent(new Event("value"));
  target.dispatchEvent(new Event("value"));
  assert.equal(infos.length, 2);
  assert.notEqual(infos[0], infos[1]);
  assert.equal(infos[0].count, 1);
  assert.equal(infos[1].count, 1);
  await first.return();
  target.dispatchEvent(new Event("value"));
  assert.equal(infos.length, 3);
  assert.equal(infos[0].count, 1);
  assert.equal(infos[1].count, 2);
  assert.equal(infos[1], infos[2]);
  await second.return();
  target.dispatchEvent(new Event("value"));
  assert.equal(infos.length, 3);
});

test("push options reject non-function onDrop hooks", () => {
  for (const onDrop of [null, false, 1, {}]) {
    assert.throws(
      () => fromCallback(() => {}, { capacity: 1, onDrop }),
      TypeError,
    );
  }
});

test("drop hook preserves a reentrant producer failure and records its own later throw", async () => {
  let observer;
  const failure = new Error("producer failed");
  const hookFailure = new Error("hook failed later");
  const iterator = fromCallback(
    (sink) => {
      observer = sink;
    },
    {
      capacity: 1,
      overflow: "drop-oldest",
      onDrop() {
        observer.error(failure);
        assert.equal(observer.next(3), false);
        throw hookFailure;
      },
    },
  ).iterate();
  const first = iterator.next();
  observer.next(0);
  await first;
  observer.next(1);
  assert.equal(observer.next(2), true);
  await assert.rejects(iterator.next(), (error) => error === failure);
  const report = await iterator.closed;
  assert.equal(report.kind, "failed");
  assert.equal(report.reason, failure);
  assert.deepEqual(report.suppressedErrors, [hookFailure]);
});

test("drop hook throws remain failures after reentrant completion", async () => {
  let observer;
  const failure = new Error("hook failed after complete");
  await assert.rejects(
    fromCallback(
      (sink) => {
        observer = sink;
        observer.next(1);
        assert.equal(observer.next(2), true);
        assert.equal(observer.next(3), false);
      },
      {
        capacity: 1,
        overflow: "drop-oldest",
        onDrop() {
          observer.complete();
          throw failure;
        },
      },
    ).toArray(),
    (error) => error === failure,
  );
});

test("drop notification ignores returned thenables without retaining or awaiting them", async () => {
  const result = await fromCallback(
    (observer) => {
      observer.next(1);
      observer.next(2);
      observer.complete();
    },
    {
      capacity: 1,
      overflow: "drop-oldest",
      onDrop() {
        return {
          get then() {
            assert.fail("hook return must not be inspected");
          },
        };
      },
    },
  ).toArray();
  assert.deepEqual(result, [2]);
});
