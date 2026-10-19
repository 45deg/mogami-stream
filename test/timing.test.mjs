import { test } from "node:test";
import assert from "node:assert/strict";
import * as s from "../dist/index.js";
import { iterateWithScheduler } from "../dist/terminal/iterate.js";
import { VirtualScheduler, controlled, flush } from "./helpers.mjs";
const virtual = (source, scheduler) =>
  iterateWithScheduler(source, {}, scheduler);

test("spaceout emits immediately, enforces minimum spacing, and accounts for idle time", async () => {
  const clock = new VirtualScheduler(),
    iterator = virtual(s.of(1, 2, 3).pipe(s.spaceout(10)), clock);
  assert.equal((await iterator.next()).value, 1);
  assert.equal(clock.timers.size, 0);
  const second = iterator.next();
  await flush();
  assert.equal(clock.timers.size, 1);
  await clock.advance(9);
  let settled = false;
  void second.then(() => (settled = true));
  await flush();
  assert.equal(settled, false);
  await clock.advance(1);
  assert.equal((await second).value, 2);
  await clock.advance(100);
  assert.equal((await iterator.next()).value, 3);
  await iterator.return();
  assert.equal(clock.timers.size, 0);
});

test("slow upstream reads do not incur an additional spaceout interval", async () => {
  const clock = new VirtualScheduler(),
    input = controlled();
  const iterator = virtual(s.from(input.source).pipe(s.spaceout(10)), clock);
  const first = iterator.next();
  await flush();
  input.reads[0].resolve({ value: 1 });
  await first;
  const second = iterator.next();
  await flush();
  await clock.advance(20);
  input.reads[1].resolve({ value: 2 });
  assert.equal((await second).value, 2);
  assert.equal(clock.timers.size, 0);
  await iterator.return();
});

test("rateLimit counts starts in a sliding window including the done read", async () => {
  const clock = new VirtualScheduler(),
    starts = [];
  const input = {
    *[Symbol.iterator]() {
      for (let i = 0; i < 4; i++) {
        starts.push(clock.now());
        yield i;
      }
      starts.push(clock.now());
    },
  };
  const iterator = virtual(
    s.from(input).pipe(s.rateLimit({ count: 2, perMs: 10 })),
    clock,
  );
  await iterator.next();
  await iterator.next();
  const third = iterator.next();
  await flush();
  assert.deepEqual(starts, [0, 0]);
  await clock.advance(10);
  await third;
  await iterator.next();
  assert.deepEqual(starts, [0, 0, 10, 10]);
  const end = iterator.next();
  await flush();
  await clock.advance(10);
  assert.equal((await end).done, true);
  assert.deepEqual(starts, [0, 0, 10, 10, 20]);
  assert.equal(clock.timers.size, 0);
});

test("rateLimit forgets expired starts after long idle and cancellation clears its wait", async () => {
  const clock = new VirtualScheduler();
  const iterator = virtual(
    s.range(0, 5).pipe(s.rateLimit({ count: 1, perMs: 10 })),
    clock,
  );
  await iterator.next();
  await clock.advance(100);
  await iterator.next();
  const read = iterator.next();
  const rejected = assert.rejects(read, (e) => e === "stop");
  await flush();
  iterator.cancel("stop");
  await rejected;
  await iterator.closed;
  assert.equal(clock.timers.size, 0);
});

test("delayStart does not open upstream until its one initial wait finishes", async () => {
  const clock = new VirtualScheduler();
  let opens = 0;
  const iterator = virtual(
    s
      .defer(() => {
        opens++;
        return [1, 2];
      })
      .pipe(s.delayStart(10)),
    clock,
  );
  const first = iterator.next();
  await flush();
  assert.equal(opens, 0);
  await clock.advance(10);
  assert.equal((await first).value, 1);
  assert.equal(opens, 1);
  assert.equal((await iterator.next()).value, 2);
  assert.equal(clock.timers.size, 0);
  await iterator.return();
});

test("timeoutEach notifies promptly while closed still tracks an uncooperative input", async () => {
  const clock = new VirtualScheduler(),
    input = controlled();
  const iterator = virtual(s.from(input.source).pipe(s.timeoutEach(10)), clock);
  const read = iterator.next();
  const rejected = assert.rejects(read, s.TimeoutError);
  await flush();
  await clock.advance(10);
  await rejected;
  assert.equal(input.returns, 1);
  assert.equal(clock.timers.size, 0);
  input.reads[0].resolve({ value: 99 });
  const report = await iterator.closed;
  assert.equal(report.kind, "failed");
  assert.ok(report.reason instanceof s.TimeoutError);
});

test("timeoutEach cancels successful read timers and does not time downstream idle", async () => {
  const clock = new VirtualScheduler(),
    iterator = virtual(s.of(1, 2).pipe(s.timeoutEach(10)), clock);
  assert.equal((await iterator.next()).value, 1);
  await flush();
  assert.equal(clock.timers.size, 0);
  await clock.advance(100);
  assert.equal((await iterator.next()).value, 2);
  await iterator.return();
  assert.equal(clock.timers.size, 0);
});

test("timeoutEach checks elapsed time for values and completion when timer callbacks are delayed", async () => {
  for (const elapsed of [9, 10, 11]) {
    for (const done of [false, true]) {
      const clock = new VirtualScheduler(),
        input = controlled();
      clock.time = 100;
      const iterator = virtual(
        s.from(input.source).pipe(s.timeoutEach(10)),
        clock,
      );
      const read = iterator.next();
      const rejected = elapsed >= 10 && assert.rejects(read, s.TimeoutError);
      await flush();
      assert.equal(clock.timers.size, 1);
      // Advance only the monotonic clock, leaving the timer callback pending.
      clock.time += elapsed;
      input.reads[0].resolve({ done, value: 7 });
      if (rejected) {
        await rejected;
        assert.equal((await iterator.closed).kind, "failed");
        assert.equal((await iterator.next()).done, true);
      } else {
        assert.deepEqual(await read, { done, value: done ? undefined : 7 });
        await iterator.return();
      }
      assert.equal(input.returns, done ? 0 : 1);
      assert.equal(clock.timers.size, 0);
    }
  }
});

test("timeoutEach includes synchronous work performed by an upstream read", async () => {
  const clock = new VirtualScheduler();
  let finalized = 0;
  const iterator = virtual(
    s
      .from({
        *[Symbol.iterator]() {
          try {
            clock.time += 25;
            yield 1;
          } finally {
            finalized++;
          }
        },
      })
      .pipe(s.timeoutEach(5)),
    clock,
  );
  await assert.rejects(iterator.next(), s.TimeoutError);
  const report = await iterator.closed;
  assert.equal(report.kind, "failed");
  assert.equal(report.reason.durationMs, 5);
  assert.equal(finalized, 1);
  assert.equal(clock.timers.size, 0);
});

test("timeoutEach(0) fails without opening input or scheduling a timer", async () => {
  const clock = new VirtualScheduler();
  let opens = 0;
  const source = {
    [Symbol.asyncIterator]() {
      opens++;
      return { next: async () => ({ done: false, value: 1 }) };
    },
  };
  // Apply the operator directly to also cover arbitrary AsyncIterable input.
  const zero = virtual(s.timeoutEach(0)(source), clock);
  await assert.rejects(zero.next(), s.TimeoutError);
  await zero.closed;
  assert.equal(opens, 0);
  assert.equal(clock.timers.size, 0);
});

test("value/timeout races choose one result and observe late failures", async () => {
  for (const timeoutFirst of [false, true]) {
    const clock = new VirtualScheduler(),
      input = controlled();
    const iterator = virtual(
      s.from(input.source).pipe(s.timeoutEach(10)),
      clock,
    );
    const read = iterator.next();
    const result = read.then(
      (value) => ({ value }),
      (error) => ({ error }),
    );
    await flush();
    const late = new Error("late");
    if (timeoutFirst) {
      await clock.advance(10);
      input.reads[0].reject(late);
      assert.ok((await result).error instanceof s.TimeoutError);
    } else {
      input.reads[0].resolve({ value: 7 });
      await flush();
      await clock.advance(10);
      assert.equal((await result).value.value, 7);
      await iterator.return();
    }
    const report = await iterator.closed;
    assert.deepEqual(report.suppressedErrors, timeoutFirst ? [late] : []);
    assert.equal(clock.timers.size, 0);
  }
});

test("deadline includes downstream idle and reports the failure exactly once", async () => {
  const clock = new VirtualScheduler();
  let signal;
  const iterator = virtual(
    s
      .defer((context) => {
        signal = context.signal;
        return [1, 2];
      })
      .pipe(s.deadline(10)),
    clock,
  );
  assert.equal((await iterator.next()).value, 1);
  await clock.advance(10);
  assert.equal(signal.aborted, true);
  await assert.rejects(iterator.next(), s.DeadlineExceededError);
  assert.equal((await iterator.next()).done, true);
  await iterator.closed;
  assert.equal(clock.timers.size, 0);
});

test("deadline(0) never opens input; normal completion and return clear deadline timers", async () => {
  const clock = new VirtualScheduler();
  let opens = 0;
  const zero = virtual(
    s
      .defer(() => {
        opens++;
        return [1];
      })
      .pipe(s.deadline(0)),
    clock,
  );
  await assert.rejects(zero.next(), s.DeadlineExceededError);
  await zero.closed;
  assert.equal(opens, 0);
  const normal = virtual(s.of(1).pipe(s.deadline(10)), clock);
  await normal.next();
  await normal.next();
  await normal.closed;
  assert.equal(clock.timers.size, 0);
  const returned = virtual(s.of(1, 2).pipe(s.deadline(10)), clock);
  await returned.next();
  await returned.return();
  assert.equal(clock.timers.size, 0);
});

test("bufferTime flushes on item limits, preserves a final partial array, and emits no empty arrays", async () => {
  const clock = new VirtualScheduler();
  const iterator = virtual(
    s.range(0, 5).pipe(s.bufferTime(10, { maxItems: 2 })),
    clock,
  );
  const values = [];
  for (;;) {
    const result = await iterator.next();
    if (result.done) break;
    values.push(result.value);
  }
  assert.deepEqual(values, [[0, 1], [2, 3], [4]]);
  assert.equal(clock.timers.size, 0);
  assert.deepEqual(
    await s.toArray(s.empty().pipe(s.bufferTime(1, { maxItems: 2 }))),
    [],
  );
});

test("bufferTime flushes during a pending read and keeps only that reservation while idle", async () => {
  const clock = new VirtualScheduler(),
    input = controlled();
  const iterator = virtual(
    s.from(input.source).pipe(s.bufferTime(10, { maxItems: 3 })),
    clock,
  );
  const first = iterator.next();
  await flush();
  assert.equal(clock.timers.size, 0);
  input.reads[0].resolve({ value: 1 });
  await flush();
  assert.equal(input.reads.length, 2);
  await clock.advance(10);
  assert.deepEqual((await first).value, [1]);
  await clock.advance(100);
  assert.equal(input.reads.length, 2);
  input.reads[1].resolve({ value: 2 });
  await flush();
  await clock.advance(11);
  assert.equal(input.reads.length, 2);
  assert.deepEqual((await iterator.next()).value, [2]);
  assert.equal(input.reads.length, 2);
  const last = iterator.next();
  await flush();
  input.reads[2].resolve({ done: true });
  assert.equal((await last).done, true);
  await iterator.closed;
  assert.equal(clock.timers.size, 0);
});

test("bufferTime discards partial arrays on failure and cancellation", async () => {
  const clock = new VirtualScheduler(),
    input = controlled();
  const iterator = virtual(
    s.from(input.source).pipe(s.bufferTime(10, { maxItems: 3 })),
    clock,
  );
  const read = iterator.next();
  const failure = new Error("input");
  const rejected = assert.rejects(read, (e) => e === failure);
  await flush();
  input.reads[0].resolve({ value: 1 });
  await flush();
  input.reads[1].reject(failure);
  await rejected;
  await iterator.closed;
  assert.equal(clock.timers.size, 0);
  const input2 = controlled(),
    cancelled = virtual(
      s.from(input2.source).pipe(s.bufferTime(10, { maxItems: 3 })),
      clock,
    );
  const pending = cancelled.next();
  const rejection = assert.rejects(pending, (e) => e === "stop");
  await flush();
  input2.reads[0].resolve({ value: 1 });
  await flush();
  cancelled.cancel("stop");
  await rejection;
  input2.reads[1].resolve({ done: true });
  await cancelled.closed;
  assert.equal(clock.timers.size, 0);
});

test("default scheduler connects to real monotonic clocks and abortable host timers", async () => {
  assert.deepEqual(
    await s.toArray(s.of(1, 2).pipe(s.spaceout(1), s.delayStart(1))),
    [1, 2],
  );
  const controller = new AbortController();
  const consumption = s.toArray(s.of(1).pipe(s.delayStart(60_000)), {
    signal: controller.signal,
  });
  const rejected = assert.rejects(consumption, (e) => e === "cancel");
  controller.abort("cancel");
  await rejected;
});
