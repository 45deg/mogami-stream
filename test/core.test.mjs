import { test } from "node:test";
import assert from "node:assert/strict";
import * as s from "../dist/index.js";
import { controlled, deferred, flush, turn } from "./helpers.mjs";

test("construction, iterator acquisition, take(0), and pre-abort are lazy", async () => {
  let opened = 0,
    finalized = 0;
  const source = s
    .defer(() => {
      opened++;
      return [1, 2];
    })
    .pipe(s.finalize(() => finalized++));
  source[Symbol.asyncIterator]();
  assert.equal(opened, 0);
  assert.deepEqual(await s.toArray(source.pipe(s.take(0))), []);
  const controller = new AbortController();
  controller.abort("pre-aborted");
  await assert.rejects(
    s.toArray(source, { signal: controller.signal }),
    (error) => error === "pre-aborted",
  );
  assert.equal(opened, 0);
  assert.equal(finalized, 0);
});

test("sequential operators, index reset, reducers, and reuse", async () => {
  const indexes = [];
  const pipeline = s.range(0, 10).pipe(
    s.skip(1),
    s.filter((x) => x % 2 === 1),
    s.takeWhile((x) => x < 8),
    s.tap((x, { index }) => indexes.push([x, index])),
    s.chunks(3),
  );
  assert.deepEqual(await s.toArray(pipeline), [[1, 3, 5], [7]]);
  assert.deepEqual(await s.toArray(pipeline), [[1, 3, 5], [7]]);
  assert.deepEqual(indexes.slice(0, 4), indexes.slice(4));
  assert.equal(await s.reduce(s.range(1, 5), async (sum, x) => sum + x, 0), 10);
  assert.equal(
    await s.reduce(
      s.empty(),
      () => {
        throw Error();
      },
      8,
    ),
    8,
  );
  assert.deepEqual(
    await s.toArray(
      s.pipe(
        [1, 2],
        s.map((x) => x + 1),
      ),
    ),
    [2, 3],
  );
});

test("Promise values are preserved by sources and selection; mapper results are awaited", async () => {
  const value = Promise.resolve(12);
  const source = s.from([value]).pipe(s.take(1));
  assert.equal((await s.iterate(source).next()).value, value);
  assert.equal((await s.toArray(s.from([value])))[0], value);
  assert.deepEqual(
    await s.toArray(s.of(2).pipe(s.map((x) => Promise.resolve(x * 2)))),
    [4],
  );
});

test("safe integer ranges stop before overflow and accept either direction", async () => {
  assert.deepEqual(await s.toArray(s.range(5, -2, -2)), [5, 3, 1, -1]);
  assert.deepEqual(await s.toArray(s.range(0, 3, -1)), []);
  const max = Number.MAX_SAFE_INTEGER;
  assert.deepEqual(await s.toArray(s.range(max - 1, max, 2)), [max - 1]);
  assert.deepEqual(await s.toArray(s.range(-max + 1, -max, -2)), [-max + 1]);
});

test("invalid options fail during construction", () => {
  for (const value of [
    0,
    -1,
    1.2,
    NaN,
    Infinity,
    Number.MAX_SAFE_INTEGER + 1,
  ]) {
    assert.throws(() => s.map((x) => x, { concurrency: value }), RangeError);
    assert.throws(() => s.chunks(value), RangeError);
    assert.throws(() => s.flatten({ concurrency: value }), RangeError);
  }
  for (const value of [-1, 1.2, NaN, Infinity]) {
    assert.throws(() => s.take(value), RangeError);
    assert.throws(() => s.skip(value), RangeError);
  }
  for (const value of [-1, NaN, Infinity]) {
    for (const operator of [
      s.spaceout,
      s.timeoutEach,
      s.deadline,
      s.delayStart,
    ])
      assert.throws(() => operator(value), RangeError);
  }
  assert.throws(
    () => s.map((x) => x, { concurrency: 2, capacity: 1 }),
    RangeError,
  );
  assert.throws(() => s.rateLimit({ count: 1, perMs: 0 }), RangeError);
  assert.throws(() => s.bufferTime(0, { maxItems: 1 }), RangeError);
  assert.throws(() => s.bufferTime(1, { maxItems: 0 }), RangeError);
  assert.throws(() => s.range(0, 1, 0), RangeError);
  assert.throws(() => s.range(0, Infinity), RangeError);
});

test("next rejects overlapping demand without disturbing the first request", async () => {
  const input = controlled();
  const iterator = s.iterate(input.source);
  const pending = iterator.next();
  await assert.rejects(iterator.next(), TypeError);
  await flush();
  input.reads[0].resolve({ value: 1, done: false });
  assert.deepEqual(await pending, { value: 1, done: false });
  await iterator.return();
  assert.equal(input.returns, 1);
});

test("cancellation rejects next promptly but closed waits for a non-cooperative read", async () => {
  const input = controlled();
  const iterator = s.iterate(input.source);
  const reading = iterator.next();
  const rejected = assert.rejects(reading, (error) => error === "cancel");
  await flush();
  iterator.cancel("cancel");
  await rejected;
  assert.equal(input.returns, 1);
  let closed = false;
  void iterator.closed.then(() => {
    closed = true;
  });
  await flush();
  assert.equal(closed, false);
  assert.deepEqual(await iterator.next(), { done: true, value: undefined });
  input.reads[0].resolve({ done: true });
  assert.equal((await iterator.closed).kind, "cancelled");
});

test("return broadcasts abort before awaiting pending work and does not reject the pending next", async () => {
  let signal;
  const gate = deferred();
  const iterator = s.iterate(
    s.defer((context) => {
      signal = context.signal;
      return {
        [Symbol.asyncIterator]() {
          return this;
        },
        next: () => gate.promise,
        return: () => ({ done: true }),
      };
    }),
  );
  const read = iterator.next();
  await flush();
  let returned = false;
  const closing = iterator.return().then(() => {
    returned = true;
  });
  assert.equal(signal.aborted, true);
  assert.deepEqual(await read, { done: true, value: undefined });
  await flush();
  assert.equal(returned, false);
  gate.resolve({ done: true });
  await closing;
  assert.equal((await iterator.closed).kind, "completed");
});

test("throw stops an execution and reports its primary failure", async () => {
  const iterator = s.iterate(s.range(0, 3));
  await iterator.next();
  const error = new Error("consumer");
  await assert.rejects(iterator.throw(error), (e) => e === error);
  assert.equal((await iterator.closed).reason, error);
  assert.equal((await iterator.next()).done, true);
});

test("normal exhaustion does not call return; incomplete cleanup is reported", async () => {
  let returns = 0;
  const exhausted = {
    [Symbol.asyncIterator]() {
      return this;
    },
    next: () => ({ done: true }),
    return: () => {
      returns++;
      return { done: true };
    },
  };
  await s.toArray(exhausted);
  assert.equal(returns, 0);
  const broken = {
    [Symbol.asyncIterator]() {
      return this;
    },
    next: () => ({ value: 1, done: false }),
    return: () => ({ done: false, value: 2 }),
  };
  await assert.rejects(
    s.toArray(s.from(broken).pipe(s.take(1))),
    s.IteratorCleanupError,
  );
});

test("primary errors survive cleanup errors; finalizers run once and inside out", async () => {
  const order = [],
    failure = new Error("mapper"),
    cleanup = new Error("cleanup");
  const iterator = s.iterate(
    s.of(1).pipe(
      s.finalize(async () => {
        order.push("inner");
        throw cleanup;
      }),
      s.map(() => {
        throw failure;
      }),
      s.finalize(() => order.push("outer")),
    ),
  );
  await assert.rejects(iterator.next(), (e) => e === failure);
  const report = await iterator.closed;
  assert.equal(report.reason, failure);
  assert.ok(report.suppressedErrors.includes(cleanup));
  assert.deepEqual(order, ["inner", "outer"]);
  await iterator.return().catch(() => {});
  assert.equal(order.length, 2);
});

test("a cleanup-only failure rejects normal terminal consumption", async () => {
  const error = new Error("finalizer");
  await assert.rejects(
    s.toArray(
      s.of(1).pipe(
        s.finalize(() => {
          throw error;
        }),
      ),
    ),
    (e) => e === error,
  );
});

test("take stops upstream before delivering its last value and leaves downstream signal alive", async () => {
  let upstreamSignal, downstreamSignal;
  const iterator = s.iterate(
    s
      .defer(({ signal }) => {
        upstreamSignal = signal;
        return [1, 2];
      })
      .pipe(
        s.take(1),
        s.map((value, { signal }) => {
          downstreamSignal = signal;
          return value;
        }),
      ),
  );
  assert.equal((await iterator.next()).value, 1);
  assert.equal(upstreamSignal.aborted, true);
  assert.equal(downstreamSignal.aborted, false);
  await iterator.return();
});

test("break waits for finalization; reducer failure closes its source", async () => {
  let finalized = 0;
  for await (const value of s.range(0, 9).pipe(
    s.finalize(() => {
      finalized++;
    }),
  )) {
    assert.equal(value, 0);
    break;
  }
  assert.equal(finalized, 1);
  const failure = new Error("reducer");
  await assert.rejects(
    s.reduce(
      s.range(0, 9).pipe(
        s.finalize(() => {
          finalized++;
        }),
      ),
      () => {
        throw failure;
      },
      0,
    ),
    (e) => e === failure,
  );
  assert.equal(finalized, 2);
});

test("the same iterator cannot be owned twice in one execution", async () => {
  const iterator = [1, 2][Symbol.iterator]();
  await assert.rejects(s.toArray(s.merge(iterator, iterator)), TypeError);
});

test("independent executions do not share mapper indexes or signals", async () => {
  const runs = [];
  const source = s.of(1, 2).pipe(
    s.map((value, { index, signal }) => {
      runs.push({ index, signal });
      return value;
    }),
  );
  assert.deepEqual(await Promise.all([s.toArray(source), s.toArray(source)]), [
    [1, 2],
    [1, 2],
  ]);
  assert.equal(new Set(runs.map((x) => x.signal)).size, 2);
  assert.deepEqual(runs.map((x) => x.index).sort(), [0, 0, 1, 1]);
});

test("late failures are observed and retained after cancellation", async () => {
  const gate = deferred(),
    late = new Error("late");
  const iterator = s.iterate(s.of(1).pipe(s.map(() => gate.promise)));
  const read = iterator.next();
  const rejection = assert.rejects(read, (e) => e === "stop");
  await flush();
  iterator.cancel("stop");
  await rejection;
  gate.reject(late);
  assert.ok((await iterator.closed).suppressedErrors.includes(late));
  await turn();
});

test("throw rejects an already pending read and later reads finish", async () => {
  const input = controlled();
  const iterator = s.iterate(input.source);
  const error = new Error("injected stop");
  const read = iterator.next();
  const rejected = assert.rejects(read, (value) => value === error);
  await flush();
  await assert.rejects(iterator.throw(error), (value) => value === error);
  await rejected;
  assert.equal((await iterator.next()).done, true);
  input.reads[0].resolve({ done: true });
  await iterator.closed;
});

test("external cancellation preserves a null reason", async () => {
  const controller = new AbortController();
  controller.abort(null);
  const iterator = s.iterate(s.of(1), { signal: controller.signal });
  await assert.rejects(iterator.next(), (reason) => reason === null);
  assert.equal((await iterator.closed).reason, null);
});

test("terminals wait for asynchronous cleanup without replacing a primary error", async () => {
  const cleanup = deferred();
  const failure = new Error("work");
  let settled = false;
  const result = s.toArray(
    s.of(1).pipe(
      s.map(() => {
        throw failure;
      }),
      s.finalize(() => cleanup.promise),
    ),
  );
  const rejected = assert.rejects(result, (error) => error === failure);
  void result.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    },
  );
  await flush();
  assert.equal(settled, false);
  cleanup.reject(new Error("cleanup"));
  await rejected;
});

test("all owned input signals are aborted before a non-cooperative sibling can delay cleanup", async () => {
  const inputs = [controlled(), controlled()];
  const signals = [];
  const iterator = s.iterate(
    s.merge(
      ...inputs.map((input, index) =>
        s.defer(({ signal }) => {
          signals[index] = signal;
          return input.source;
        }),
      ),
    ),
  );
  const read = iterator.next();
  const rejected = assert.rejects(read, (error) => error === "stop");
  await flush();
  iterator.cancel("stop");
  assert.ok(signals.every((signal) => signal.aborted));
  assert.ok(inputs.every((input) => input.returns === 1));
  await rejected;
  for (const input of inputs) input.reads[0].resolve({ done: true });
  await iterator.closed;
});

test("reducer callbacks use the managed execution signal and preserve the first stop reason", async () => {
  const controller = new AbortController();
  let reducerSignal;
  const result = s.reduce(
    s.of(1),
    (sum, value, { signal }) => {
      reducerSignal = signal;
      return new Promise((resolve, reject) => {
        signal.addEventListener("abort", () => reject(signal.reason), {
          once: true,
        });
      });
    },
    0,
    { signal: controller.signal },
  );
  const rejected = assert.rejects(
    result,
    (reason) => reason === "stop reducer",
  );
  await flush();
  controller.abort("stop reducer");
  assert.equal(reducerSignal.aborted, true);
  await rejected;
});
