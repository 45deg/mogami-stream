import assert from "node:assert/strict";
import { getEventListeners } from "node:events";
import { test } from "node:test";
import { from, range, tap } from "../dist/index.js";
import {
  yieldAfter,
  yieldEvery,
  yieldToHost,
} from "../docs/examples/event-loop.mjs";
import { flush } from "./helpers.mjs";

test("yield recipes reject invalid intervals", () => {
  for (const count of [0, -1, 1.5, Infinity, NaN, Number.MAX_SAFE_INTEGER + 1])
    assert.throws(() => yieldEvery(count), RangeError);
  for (const budget of [0, -1, Infinity, NaN])
    assert.throws(() => yieldAfter(budget), RangeError);
});

test("count recipe preserves order, Promise identity and execution independence", async () => {
  const promise = Promise.resolve(7);
  const values = [1, promise, 2, 3, 4];
  const stream = from(values).pipe(yieldEvery(2));
  const [first, second] = await Promise.all([
    stream.toArray(),
    stream.toArray(),
  ]);
  assert.deepEqual(first, values);
  assert.deepEqual(second, values);
  assert.equal(first[1], promise);
});

test("count recipe reads only on demand and cancellation interrupts its boundary wait", async () => {
  let reads = 0;
  let returns = 0;
  const source = {
    [Symbol.iterator]() {
      return this;
    },
    next() {
      return { value: reads++, done: false };
    },
    return() {
      returns++;
      return { done: true };
    },
  };
  const iterator = from(source).pipe(yieldEvery(2)).iterate();
  await flush();
  assert.equal(reads, 0);
  assert.deepEqual(await iterator.next(), { value: 0, done: false });
  await flush();
  assert.equal(reads, 1);
  let delivered = false;
  const pending = iterator.next().then((result) => {
    delivered = true;
    return result;
  });
  const rejected = assert.rejects(pending, (error) => error === "stop");
  await flush();
  assert.equal(reads, 2);
  assert.equal(delivered, false);
  iterator.cancel("stop");
  await rejected;
  await iterator.closed;
  assert.equal(reads, 2);
  assert.equal(returns, 1);
});

test("time recipe owns its clock per execution and its wait is cancellable", async (t) => {
  let now = 0;
  const original = Object.getOwnPropertyDescriptor(performance, "now");
  Object.defineProperty(performance, "now", {
    configurable: true,
    value: () => now,
  });
  t.after(() => {
    if (original) Object.defineProperty(performance, "now", original);
    else delete performance.now;
  });
  const stream = range(0, 3).pipe(yieldAfter(4));
  const first = stream.iterate();
  const second = stream.iterate();
  t.after(() => {
    first.cancel();
    second.cancel();
  });
  assert.deepEqual(await first.next(), { value: 0, done: false });
  now = 5;
  // The second execution starts a fresh clock at 5, rather than sharing 0.
  let secondDelivered = false;
  const secondRead = second.next().then((result) => {
    secondDelivered = true;
    return result;
  });
  await flush();
  assert.equal(secondDelivered, true);
  assert.deepEqual(await secondRead, { value: 0, done: false });
  let firstDelivered = false;
  const pending = first.next().then((result) => {
    firstDelivered = true;
    return result;
  });
  const rejected = assert.rejects(pending, (error) => error === "stop");
  await flush();
  assert.equal(firstDelivered, false);
  first.cancel("stop");
  await rejected;
  await first.closed;
  assert.deepEqual(await second.next(), { value: 1, done: false });
});

test("host wait removes abort listeners after completion, cancellation and pre-abort", async () => {
  const controller = new AbortController();
  await yieldToHost(controller.signal);
  assert.equal(getEventListeners(controller.signal, "abort").length, 0);
  const pending = yieldToHost(controller.signal);
  assert.equal(getEventListeners(controller.signal, "abort").length, 1);
  controller.abort("stop");
  await assert.rejects(pending, (error) => error === "stop");
  assert.equal(getEventListeners(controller.signal, "abort").length, 0);
  await assert.rejects(
    yieldToHost(controller.signal),
    (error) => error === "stop",
  );
  assert.equal(getEventListeners(controller.signal, "abort").length, 0);
});

test("a timer can cancel the count recipe before the remaining work runs", async () => {
  const controller = new AbortController();
  let seen = 0;
  let closed = false;
  function* source() {
    try {
      yield* Array.from({ length: 1_000 }, (_, index) => index);
    } finally {
      closed = true;
    }
  }
  const timer = setTimeout(() => controller.abort("timer stop"), 0);
  try {
    await assert.rejects(
      from(source())
        .pipe(
          yieldEvery(8),
          tap(() => {
            seen++;
          }),
        )
        .forEach(() => {}, { signal: controller.signal }),
      (error) => error === "timer stop",
    );
    assert.equal(seen, 7);
    assert.equal(closed, true);
  } finally {
    clearTimeout(timer);
  }
});
