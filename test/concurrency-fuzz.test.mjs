import { test } from "node:test";
import assert from "node:assert/strict";
import * as stream from "../dist/index.js";
import { iterateWithScheduler } from "../dist/terminal/iterate.js";
import { deferred, flush, turn, VirtualScheduler } from "./helpers.mjs";
import { forSeeds } from "./fuzz-helpers.mjs";

function task() {
  const gate = deferred();
  return {
    pending: true,
    promise: gate.promise,
    resolve(value) {
      assert.equal(this.pending, true);
      this.pending = false;
      gate.resolve(value);
    },
    reject(error) {
      assert.equal(this.pending, true);
      this.pending = false;
      gate.reject(error);
    },
  };
}

function input() {
  const reads = [];
  const cleanup = task();
  let returns = 0;
  return {
    reads,
    cleanup,
    get pending() {
      return reads.find((read) => read.pending);
    },
    get returns() {
      return returns;
    },
    source: {
      [Symbol.asyncIterator]() {
        return this;
      },
      next() {
        assert.equal(
          reads.some((read) => read.pending),
          false,
          "overlapping upstream reads",
        );
        const read = task();
        reads.push(read);
        return read.promise;
      },
      return() {
        assert.equal(++returns, 1);
        return cleanup.promise;
      },
    },
  };
}

// Drain microtasks and cooperative scheduler yields without advancing time.
async function settle() {
  await flush();
  await turn();
  await flush();
}

function observe(promise) {
  const state = { settled: false, result: undefined, error: undefined };
  state.promise = promise.then(
    (result) => {
      state.settled = true;
      state.result = result;
    },
    (error) => {
      state.settled = true;
      state.error = error;
    },
  );
  return state;
}

function assertErrors(actual, expected) {
  assert.equal(
    actual.length,
    expected.size,
    `Unexpected suppressed errors: ${actual
      .filter((error) => !expected.has(error))
      .map(String)
      .join(", ")}`,
  );
  for (const error of expected) assert.ok(actual.includes(error));
}

test("seeded fuzz: map bounds and output order survive shuffled pulls and completions", async () => {
  await forSeeds(async (pick) => {
    const concurrency = pick(4) + 1;
    const capacity = concurrency + pick(5);
    const ordered = Boolean(pick(2));
    const count = pick(13);
    const source = input();
    const work = [];
    const completed = [];
    const output = [];
    let accepted = 0;
    const iterator = iterateWithScheduler(
      stream.from(source.source).pipe(
        stream.map(
          (value, context) => {
            assert.equal(value, context.index);
            const gate = task();
            work.push(gate);
            return gate.promise;
          },
          { concurrency, capacity, ordered },
        ),
      ),
      {},
      new VirtualScheduler(),
    );

    function bounds() {
      const reserved = Number(Boolean(source.pending));
      const running = work.filter((gate) => gate.pending).length;
      assert.ok(reserved + running <= concurrency);
      assert.ok(accepted - output.length + reserved <= capacity);
    }
    function advance() {
      const actions = [];
      if (source.pending)
        actions.push(() => {
          const read = source.pending;
          if (accepted < count) read.resolve({ value: accepted++ });
          else read.resolve({ done: true });
        });
      work.forEach((gate, index) => {
        if (gate.pending)
          actions.push(() => {
            completed.push(index);
            gate.resolve(index);
          });
      });
      assert.ok(
        actions.length,
        "pending demand has no remaining operation to settle",
      );
      actions[pick(actions.length)]();
    }
    for (;;) {
      const reading = observe(iterator.next());
      for (let step = 0; !reading.settled; step++) {
        assert.ok(step < count * 3 + 10, "map made no progress");
        await settle();
        bounds();
        if (!reading.settled) advance();
      }
      assert.equal(reading.error, undefined);
      if (reading.result.done) break;
      output.push(reading.result.value);
      if (!ordered) assert.equal(output.at(-1), completed[output.length - 1]);
      await settle();
      bounds();
      const pausedReads = source.reads.length;
      if (pick(2)) {
        // Already-reserved input may finish while demand is paused.
        if (source.pending || work.some((gate) => gate.pending)) advance();
        await settle();
      }
      assert.equal(
        source.reads.length,
        pausedReads,
        "input refilled without demand",
      );
    }
    assert.deepEqual(
      ordered ? output : [...output].sort((a, b) => a - b),
      Array.from({ length: count }, (_, index) => index),
    );
    assert.equal(source.returns, 0);
    assert.equal((await iterator.closed).kind, "completed");
  });
});

test("seeded fuzz: map cancellation races retain the first cause and await every operation", async () => {
  await forSeeds(async (pick) => {
    const concurrency = pick(3) + 2;
    const source = input();
    const work = [];
    const signals = [];
    const finalize = task();
    let finalized = 0;
    const controller = new AbortController();
    const iterator = iterateWithScheduler(
      stream.from(source.source).pipe(
        stream.map(
          (value, { signal }) => {
            signals.push(signal);
            const gate = task();
            work.push(gate);
            return gate.promise;
          },
          {
            concurrency,
            capacity: concurrency + pick(4),
            ordered: Boolean(pick(2)),
          },
        ),
        stream.finalize(() => {
          finalized++;
          return finalize.promise;
        }),
      ),
      { signal: controller.signal },
      new VirtualScheduler(),
    );
    const reading = observe(iterator.next());
    for (let index = 0; index < concurrency - 1; index++) {
      await settle();
      source.pending.resolve({ value: index });
    }
    await settle();
    assert.equal(work.length, concurrency - 1);
    assert.ok(source.pending);
    const failure = new Error("racing operation");
    const reason = pick(2) ? NaN : new Error("stop request");
    const mode = pick(3);
    const failInput = Boolean(pick(2));
    const failing = failInput ? source.pending : work[pick(work.length)];
    const failureFirst = Boolean(pick(2));
    const separated = Boolean(pick(2));
    let thrown;
    const stop = () => {
      if (mode === 0) iterator.cancel(reason);
      else if (mode === 1) controller.abort(reason);
      else thrown = observe(iterator.throw(reason));
    };
    if (failureFirst) failing.reject(failure);
    else stop();
    if (separated) await settle();
    if (failureFirst) stop();
    else failing.reject(failure);
    await settle();
    const primary = failureFirst && separated ? failure : reason;
    const kind =
      (failureFirst && separated) || mode === 2 ? "failed" : "cancelled";
    assert.equal(reading.settled, true, "stop notification waited for cleanup");
    assert.ok(Object.is(reading.error, primary));
    if (thrown) {
      await thrown.promise;
      assert.ok(Object.is(thrown.error, primary));
    }
    assert.ok(
      signals.every(
        (signal) => signal.aborted && Object.is(signal.reason, primary),
      ),
    );
    assert.equal(source.returns, 1);
    let closed = false;
    iterator.closed.then(() => {
      closed = true;
    });
    const returning = observe(iterator.return());
    const expected = new Set(primary === failure ? [] : [failure]);
    if (mode === 2 && !Object.is(primary, reason)) expected.add(reason);
    const readsBeforeStop = source.reads.length;
    for (const gate of [...source.reads, ...work]) {
      if (!gate.pending) continue;
      const outcome = pick(3);
      if (outcome === 0) gate.resolve({ done: true });
      else if (outcome === 1) gate.reject(primary);
      else {
        const error = new Error("late operation");
        expected.add(error);
        gate.reject(error);
      }
    }
    await settle();
    assert.equal(closed, false);
    assert.equal(finalized, 0, "finalizer ran before upstream cleanup");
    if (pick(2)) {
      const error = new Error("return cleanup");
      expected.add(error);
      source.cleanup.reject(error);
    } else source.cleanup.resolve({ done: true });
    await settle();
    assert.equal(finalized, 1);
    assert.equal(closed, false);
    if (pick(2)) {
      const error = new Error("finalizer");
      expected.add(error);
      finalize.reject(error);
    } else finalize.resolve();
    const report = await iterator.closed;
    await returning.promise;
    assert.equal(report.kind, kind);
    assert.ok(Object.is(report.reason, primary));
    assertErrors(report.suppressedErrors, expected);
    assert.equal(source.reads.length, readsBeforeStop);
    assert.equal((await iterator.next()).done, true);
    if (kind === "failed") assert.ok(Object.is(returning.error, primary));
    else assert.equal(returning.result.done, true);
  });
});

test("seeded fuzz: concurrent children keep their slots through cleanup and preserve order", async () => {
  await forSeeds(async (pick) => {
    const concurrency = pick(4) + 1;
    const name = ["flatten", "concat", "flatMap", "concatMap"][pick(4)];
    const ordered = name.startsWith("concat");
    const outer = input();
    const children = Array.from({ length: pick(9) }, (_, id) => {
      const source = input();
      const finalize = task();
      return {
        source,
        finalize,
        read: 0,
        done: false,
        finalizing: false,
        values: Array.from({ length: pick(5) }, (_, index) => id * 100 + index),
      };
    });
    const sources = children.map((child) =>
      stream.from(child.source.source).pipe(
        stream.finalize(() => {
          child.finalizing = true;
          return child.finalize.promise;
        }),
      ),
    );
    const operator = name.endsWith("Map")
      ? stream[name]((source) => source, { concurrency })
      : stream[name]({ concurrency });
    const iterator = iterateWithScheduler(
      stream.from(outer.source).pipe(operator),
      {},
      new VirtualScheduler(),
    );
    const output = [];
    let accepted = 0;
    function bounds() {
      let retired = 0;
      for (const child of children.slice(0, accepted)) {
        if (child.done && !child.finalize.pending) retired++;
        else if (ordered) break;
      }
      assert.ok(
        accepted - retired + Number(Boolean(outer.pending)) <= concurrency,
        "child slots exceeded concurrency",
      );
    }
    function advance(idle = false) {
      const actions = [];
      if (outer.pending)
        actions.push(() => {
          if (accepted < sources.length)
            outer.pending.resolve({ value: sources[accepted++] });
          else outer.pending.resolve({ done: true });
        });
      children.forEach((child) => {
        if (child.source.pending)
          actions.push(() => {
            if (child.read < child.values.length)
              child.source.pending.resolve({
                value: child.values[child.read++],
              });
            else {
              child.done = true;
              child.source.pending.resolve({ done: true });
            }
          });
        if (child.finalizing && child.finalize.pending)
          actions.push(() => child.finalize.resolve());
      });
      if (idle && !actions.length) return;
      assert.ok(
        actions.length,
        "pending child demand has no remaining operation to settle",
      );
      actions[pick(actions.length)]();
    }
    for (;;) {
      const reading = observe(iterator.next());
      for (let step = 0; !reading.settled; step++) {
        assert.ok(step < 200, "children made no progress");
        await settle();
        bounds();
        if (!reading.settled) advance();
      }
      assert.equal(reading.error, undefined);
      if (reading.result.done) break;
      output.push(reading.result.value);
      await settle();
      bounds();
      const pausedReads = outer.reads.length;
      if (pick(2)) {
        advance(true);
        await settle();
      }
      assert.equal(
        outer.reads.length,
        pausedReads,
        "outer input refilled without demand",
      );
    }
    const expected = children.flatMap((child) => child.values);
    assert.deepEqual(
      ordered ? output : [...output].sort((a, b) => a - b),
      expected,
    );
    children.forEach((child, id) => {
      assert.deepEqual(
        output.filter((value) => Math.floor(value / 100) === id),
        child.values,
      );
      assert.equal(child.source.returns, 0);
    });
    assert.equal(outer.returns, 0);
    assert.equal((await iterator.closed).kind, "completed");
  });
});

test("seeded fuzz: multi-input completion and cancellation races await late reads and finalizers", async () => {
  await forSeeds(async (pick) => {
    const count = pick(4) + 2;
    const children = Array.from({ length: count }, () => ({
      source: input(),
      finalize: task(),
      signal: undefined,
      finalized: 0,
    }));
    const sources = children.map((child) =>
      stream
        .defer(({ signal }) => {
          child.signal = signal;
          return child.source.source;
        })
        .pipe(
          stream.finalize(() => {
            child.finalized++;
            return child.finalize.promise;
          }),
        ),
    );
    const name = ["merge", "zip", "zipLatest", "flatten", "concat"][pick(5)];
    const combined =
      name === "zipLatest"
        ? stream.zipLatest(sources)
        : name === "flatten" || name === "concat"
          ? stream.from(sources).pipe(stream[name]({ concurrency: count }))
          : stream[name](...sources);
    const controller = new AbortController();
    const iterator = iterateWithScheduler(
      combined,
      { signal: controller.signal },
      new VirtualScheduler(),
    );
    const reading = observe(iterator.next());
    await settle();
    assert.ok(children.every((child) => child.source.pending));
    const finished = pick(count);
    // Leave cleanup gated, so even zip's shortest-input completion is still pending.
    children[finished].source.pending.resolve({ done: true });
    if (pick(2)) await settle();
    const reason = pick(2) ? NaN : new Error("multi-input cancellation");
    const mode = pick(3);
    let thrown;
    if (mode === 0) iterator.cancel(reason);
    else if (mode === 1) controller.abort(reason);
    else thrown = observe(iterator.throw(reason));
    await settle();
    assert.equal(reading.settled, true);
    assert.ok(Object.is(reading.error, reason));
    if (thrown) {
      await thrown.promise;
      assert.ok(Object.is(thrown.error, reason));
    }
    assert.ok(
      children.every(
        (child, index) => index === finished || child.signal.aborted,
      ),
    );
    const expected = new Set();
    let closed = false;
    iterator.closed.then(() => {
      closed = true;
    });
    const returning = observe(iterator.return());
    for (const child of children) {
      if (child.source.pending) {
        const mode = pick(3);
        if (mode === 0) child.source.pending.resolve({ value: "late" });
        else if (mode === 1) child.source.pending.reject(child.signal.reason);
        else {
          const error = new Error("late input failure");
          expected.add(error);
          child.source.pending.reject(error);
        }
      }
      // A result settled just before cancellation may not yet have been observed as done.
      assert.ok(child.source.returns <= 1);
      if (pick(2) && child.source.returns) {
        const error = new Error("input return failure");
        expected.add(error);
        child.source.cleanup.reject(error);
      } else child.source.cleanup.resolve({ done: true });
    }
    await settle();
    assert.equal(closed, false);
    assert.ok(children.every((child) => child.finalized === 1));
    for (const child of children) {
      if (pick(2)) {
        const error = new Error("child finalizer failure");
        expected.add(error);
        child.finalize.reject(error);
      } else child.finalize.resolve();
    }
    const report = await iterator.closed;
    await returning.promise;
    assert.equal(report.kind, mode === 2 ? "failed" : "cancelled");
    assert.ok(Object.is(report.reason, reason));
    assertErrors(report.suppressedErrors, expected);
    assert.equal((await iterator.next()).done, true);
    assert.ok(
      children.every(
        (child) =>
          child.source.reads.length === 1 &&
          child.source.returns <= 1 &&
          child.finalized === 1,
      ),
    );
  });
});
