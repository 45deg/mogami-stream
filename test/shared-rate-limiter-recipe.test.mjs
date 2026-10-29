import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { getEventListeners } from "node:events";
import ts from "typescript";
import { flush } from "./helpers.mjs";

// Execute the documented TypeScript recipe on the supported Node 22 baseline.
const source = await readFile(
  new URL("../docs/examples/shared-rate-limiter.ts", import.meta.url),
  "utf8",
);
const { outputText } = ts.transpileModule(source, {
  compilerOptions: {
    target: ts.ScriptTarget.ES2022,
    module: ts.ModuleKind.ESNext,
  },
});
const executable = outputText.replaceAll(
  '"mogami-stream"',
  JSON.stringify(new URL("../dist/index.js", import.meta.url).href),
);
const { createSharedLimiter, LimiterQueueFullError, collectWithSharedRetries } =
  await import(
    `data:text/javascript;base64,${Buffer.from(executable).toString("base64")}`
  );

function clock(t) {
  let now = 0;
  t.mock.method(performance, "now", () => now);
  t.mock.timers.enable({ apis: ["setTimeout"] });
  return {
    async advance(ms) {
      now += ms;
      t.mock.timers.tick(ms);
      await flush();
    },
  };
}

const freshSignal = () => new AbortController().signal;

test("shared recipe bounds waiting callers, preserves FIFO, and does not wait for I/O", async (t) => {
  const time = clock(t);
  const limiter = createSharedLimiter(10, 2);
  const starts = [];
  let finishFirst;
  const first = limiter.run(freshSignal(), () => {
    starts.push(["first", performance.now()]);
    return new Promise((resolve) => {
      finishFirst = resolve;
    });
  });
  const second = limiter.run(freshSignal(), async () =>
    starts.push(["second", performance.now()]),
  );
  const third = limiter.run(freshSignal(), async () =>
    starts.push(["third", performance.now()]),
  );
  await assert.rejects(
    limiter.run(freshSignal(), async () => assert.fail("overflow")),
    LimiterQueueFullError,
  );
  await flush();
  assert.deepEqual(starts, [["first", 0]]);
  await time.advance(9);
  assert.equal(starts.length, 1);
  await time.advance(1);
  await second;
  await time.advance(10);
  await third;
  assert.deepEqual(starts, [
    ["first", 0],
    ["second", 10],
    ["third", 20],
  ]);
  finishFirst();
  await first;
});

test("waiting abort frees capacity without consuming a permit or disturbing FIFO", async (t) => {
  const time = clock(t);
  const limiter = createSharedLimiter(10, 2);
  await limiter.run(freshSignal(), async () => {});
  const cancelled = new AbortController();
  const reason = new Error("cancel queued caller");
  const waiting = limiter.run(cancelled.signal, async () =>
    assert.fail("cancelled I/O"),
  );
  const rejected = assert.rejects(waiting, (error) => error === reason);
  const order = [];
  const next = limiter.run(freshSignal(), async () => order.push("next"));
  assert.equal(getEventListeners(cancelled.signal, "abort").length, 1);
  cancelled.abort(reason);
  await rejected;
  assert.equal(getEventListeners(cancelled.signal, "abort").length, 0);
  const last = limiter.run(freshSignal(), async () => order.push("last"));
  await time.advance(10);
  await next;
  assert.deepEqual(order, ["next"]);
  await time.advance(10);
  await last;
  assert.deepEqual(order, ["next", "last"]);
});

test("abort immediately after a grant prevents I/O and keeps that permit consumed", async (t) => {
  const time = clock(t);
  const limiter = createSharedLimiter(10, 1);
  const controller = new AbortController();
  const reason = new Error("cancel after grant");
  const granted = limiter.run(controller.signal, async () =>
    assert.fail("cancelled I/O"),
  );
  // acquire resolves synchronously, but run has not resumed its await yet.
  assert.equal(getEventListeners(controller.signal, "abort").length, 0);
  controller.abort(reason);
  await assert.rejects(granted, (error) => error === reason);
  let calls = 0;
  const next = limiter.run(freshSignal(), async () => calls++);
  await time.advance(9);
  assert.equal(calls, 0);
  await time.advance(1);
  await next;
  assert.equal(calls, 1);
});

test("already aborted callers do not consume a permit; cancelling the last waiter clears its timer", async (t) => {
  const time = clock(t);
  const clear = t.mock.method(globalThis, "clearTimeout");
  const limiter = createSharedLimiter(10, 1);
  const cancelled = new AbortController();
  cancelled.abort(new Error("already stopped"));
  await assert.rejects(
    limiter.run(cancelled.signal, async () => assert.fail("I/O")),
    (error) => error === cancelled.signal.reason,
  );
  await limiter.run(freshSignal(), async () => {});
  const waiter = new AbortController();
  const pending = limiter.run(waiter.signal, async () => assert.fail("I/O"));
  waiter.abort();
  await assert.rejects(pending, (error) => error === waiter.signal.reason);
  assert.ok(clear.mock.callCount() > 0);
  assert.equal(getEventListeners(waiter.signal, "abort").length, 0);
  await time.advance(100);
  await limiter.run(freshSignal(), async () => {});
});

test("idle time and late timers do not accumulate burst credit", async (t) => {
  const time = clock(t);
  const limiter = createSharedLimiter(10, 2);
  await limiter.run(freshSignal(), async () => {});
  await time.advance(100);
  const starts = [];
  const operations = [1, 2, 3].map(() =>
    limiter.run(freshSignal(), async () => starts.push(performance.now())),
  );
  await flush();
  assert.deepEqual(starts, [100]);
  await time.advance(100);
  assert.deepEqual(starts, [100, 200]);
  await time.advance(10);
  await Promise.all(operations);
  assert.deepEqual(starts, [100, 200, 210]);
});

test("synchronous throws and rejected I/O release admission without refunding permits", async (t) => {
  const time = clock(t);
  const limiter = createSharedLimiter(10, 1);
  const failure = new Error("I/O failed");
  await assert.rejects(
    limiter.run(freshSignal(), () => {
      throw failure;
    }),
    (error) => error === failure,
  );
  const rejection = assert.rejects(
    limiter.run(freshSignal(), async () => {
      throw failure;
    }),
    (error) => error === failure,
  );
  await time.advance(10);
  await rejection;
  const success = limiter.run(freshSignal(), async () => "ok");
  await time.advance(10);
  assert.equal(await success, "ok");
});

test("published usage shares one budget across two executions and all retries", async (t) => {
  const time = clock(t);
  const starts = [];
  const attempts = new Map();
  const transient = new Error("retry this request");
  const result = collectWithSharedRetries(
    ["a", "b"],
    ["c", "d"],
    async (job, signal) => {
      signal.throwIfAborted();
      const attempt = (attempts.get(job) ?? 0) + 1;
      attempts.set(job, attempt);
      starts.push({ job, at: performance.now() });
      if (job === "a" && attempt === 1) throw transient;
      return job.toUpperCase();
    },
    (error) => error === transient,
  );
  await flush();
  assert.equal(starts.length, 1);
  for (let i = 0; i < 4; i++) await time.advance(100);
  assert.deepEqual(await result, [
    ["A", "B"],
    ["C", "D"],
  ]);
  assert.deepEqual(
    starts.map(({ at }) => at),
    [0, 100, 200, 300, 400],
  );
  assert.equal(attempts.get("a"), 2);
  assert.equal(starts.filter(({ job }) => job === "c").length, 1);
});

test("configuration is validated and zero waiting capacity permits only immediate admission", async (t) => {
  const time = clock(t);
  for (const interval of [0, -1, Infinity, NaN])
    assert.throws(() => createSharedLimiter(interval, 1), RangeError);
  for (const capacity of [-1, 0.5, Infinity, NaN, Number.MAX_SAFE_INTEGER + 1])
    assert.throws(() => createSharedLimiter(10, capacity), RangeError);
  const limiter = createSharedLimiter(10, 0);
  await limiter.run(freshSignal(), async () => {});
  await assert.rejects(
    limiter.run(freshSignal(), async () => assert.fail("queue")),
    LimiterQueueFullError,
  );
  await time.advance(10);
  await limiter.run(freshSignal(), async () => {});
});

test("bulk cancellation preserves FIFO for survivors and newly queued callers", async (t) => {
  const time = clock(t);
  const limiter = createSharedLimiter(1, 128);
  await limiter.run(freshSignal(), async () => {});
  const controllers = Array.from({ length: 128 }, () => new AbortController());
  const reason = new Error("remove queued callers");
  const starts = [];
  const pending = controllers.map((controller, index) =>
    limiter
      .run(controller.signal, async () => starts.push(index))
      .catch((error) => {
        assert.equal(error, reason);
      }),
  );
  for (let index = 126; index >= 0; index -= 2)
    controllers[index].abort(reason);
  pending.push(limiter.run(freshSignal(), async () => starts.push(128)));
  for (let index = 0; index < 65; index++) await time.advance(1);
  await Promise.all(pending);
  assert.deepEqual(starts, [
    ...Array.from({ length: 64 }, (_, index) => index * 2 + 1),
    128,
  ]);
  for (const controller of controllers)
    assert.equal(getEventListeners(controller.signal, "abort").length, 0);
});
