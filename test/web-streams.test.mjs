import assert from "node:assert/strict";
import { test } from "node:test";
import {
  fromReadableStream,
  toReadableStream,
  of,
  defer,
} from "../dist/index.js";

const tick = () => new Promise((resolve) => setTimeout(resolve, 0));

test("input acquires lazily, reads on demand and releases on completion", async () => {
  let pulls = 0;
  let cancels = 0;
  const source = new ReadableStream(
    {
      pull(c) {
        if (++pulls <= 2) c.enqueue(pulls);
        else c.close();
      },
      cancel() {
        cancels++;
      },
    },
    { highWaterMark: 0 },
  );
  const it = fromReadableStream(source).iterate();
  await tick();
  assert.equal(source.locked, false);
  assert.equal(pulls, 0);
  assert.equal((await it.next()).value, 1);
  await tick();
  assert.equal(pulls, 1);
  assert.equal(source.locked, true);
  assert.equal((await it.next()).value, 2);
  assert.equal((await it.next()).done, true);
  assert.equal(source.locked, false);
  assert.equal(cancels, 0);
});

test("pending input cancellation forwards reason and waits for cleanup", async () => {
  let finish;
  const cleanup = new Promise((resolve) => {
    finish = resolve;
  });
  let received;
  const source = new ReadableStream({
    cancel(reason) {
      received = reason;
      return cleanup;
    },
  });
  const it = fromReadableStream(source).iterate();
  const pending = it.next();
  const reason = new Error("stop");
  const rejected = assert.rejects(pending, (e) => e === reason);
  it.cancel(reason);
  await rejected;
  assert.equal(received, reason);
  assert.equal(source.locked, true);
  finish();
  assert.equal((await it.closed).kind, "cancelled");
  assert.equal(source.locked, false);
});

test("take cancels input and releases its lock", async () => {
  let cancelled = 0;
  const source = new ReadableStream({
    pull(c) {
      c.enqueue(1);
    },
    cancel() {
      cancelled++;
    },
  });
  assert.deepEqual(await fromReadableStream(source).take(1).toArray(), [1]);
  assert.equal(cancelled, 1);
  assert.equal(source.locked, false);
});

test("input errors release locks; occupied locks remain owned by their reader", async () => {
  const reason = new Error("read failed");
  const source = new ReadableStream({
    start(c) {
      c.error(reason);
    },
  });
  await assert.rejects(
    fromReadableStream(source).toArray(),
    (e) => e === reason,
  );
  assert.equal(source.locked, false);
  const other = new ReadableStream();
  const reader = other.getReader();
  await assert.rejects(fromReadableStream(other).toArray(), TypeError);
  assert.equal(other.locked, true);
  reader.releaseLock();
});

test("output has no prefetch and supports a round trip", async () => {
  let count = 0;
  const output = of(1, 2)
    .tap(() => {
      count++;
    })
    .toReadableStream();
  await tick();
  assert.equal(count, 0);
  const reader = output.getReader();
  assert.equal((await reader.read()).value, 1);
  await tick();
  assert.equal(count, 1);
  reader.releaseLock();
  assert.deepEqual(await fromReadableStream(output).toArray(), [2]);
  assert.equal(output.locked, false);
});

test("output cancellation interrupts a pending input and awaits finalizers", async () => {
  let received;
  let finalized = false;
  const input = new ReadableStream({
    cancel(reason) {
      received = reason;
    },
  });
  const output = fromReadableStream(input)
    .finalize(async () => {
      await tick();
      finalized = true;
    })
    .toReadableStream();
  const reader = output.getReader();
  const pending = reader.read();
  await tick();
  const reason = new Error("consumer stopped");
  await reader.cancel(reason);
  assert.equal((await pending).done, true);
  assert.equal(received, reason);
  assert.equal(finalized, true);
  assert.equal(input.locked, false);
  reader.releaseLock();
});

test("cancel before demand never opens source", async () => {
  let opened = false;
  await toReadableStream(
    defer(() => {
      opened = true;
      return [];
    }),
  ).cancel();
  assert.equal(opened, false);
});

test("external abort errors idle output without opening source", async () => {
  const controller = new AbortController();
  let opened = false;
  const output = toReadableStream(
    defer(() => {
      opened = true;
      return [];
    }),
    { signal: controller.signal },
  );
  const reason = new Error("abort");
  controller.abort(reason);
  await tick();
  await assert.rejects(output.getReader().read(), (e) => e === reason);
  assert.equal(opened, false);
});

test("output forwards processing errors and rejects cancellation cleanup failures", async () => {
  const reason = new Error("failure");
  await assert.rejects(
    of(1)
      .map(() => {
        throw reason;
      })
      .toReadableStream()
      .getReader()
      .read(),
    (e) => e === reason,
  );
  const output = of(1, 2)
    .finalize(() => {
      throw reason;
    })
    .toReadableStream();
  const reader = output.getReader();
  await reader.read();
  await assert.rejects(
    reader.cancel(),
    (e) => e instanceof AggregateError && e.errors.includes(reason),
  );
});

test("output serializes bursts of pending reads across async delivery", async () => {
  let produced = 0;
  let active = 0;
  let maxActive = 0;
  const source = {
    [Symbol.asyncIterator]() {
      return {
        async next() {
          active++;
          maxActive = Math.max(maxActive, active);
          await tick();
          active--;
          return produced < 50
            ? { value: produced++, done: false }
            : { done: true };
        },
      };
    },
  };
  const reader = toReadableStream(source).getReader();
  const results = await Promise.all(
    Array.from({ length: 60 }, () => reader.read()),
  );
  assert.deepEqual(
    results.slice(0, 50).map((r) => r.value),
    Array.from({ length: 50 }, (_, i) => i),
  );
  assert.ok(results.slice(50).every((r) => r.done));
  assert.equal(maxActive, 1);
  reader.releaseLock();
});

test("output drains synchronous sources with queued demand", async () => {
  const reader = of(...Array.from({ length: 100 }, (_, i) => i))
    .toReadableStream()
    .getReader();
  const results = await Promise.all(
    Array.from({ length: 110 }, () => reader.read()),
  );
  assert.deepEqual(
    results.slice(0, 100).map((r) => r.value),
    Array.from({ length: 100 }, (_, i) => i),
  );
  assert.ok(results.slice(100).every((r) => r.done));
  reader.releaseLock();
});

test("pipeTo waits for slow writes without accumulating upstream reads", async () => {
  let produced = 0;
  let consumed = 0;
  let maxAhead = 0;
  const input = new ReadableStream(
    {
      pull(controller) {
        if (produced === 20) return controller.close();
        controller.enqueue(produced++);
        maxAhead = Math.max(maxAhead, produced - consumed);
      },
    },
    { highWaterMark: 0 },
  );
  await fromReadableStream(input)
    .toReadableStream()
    .pipeTo(
      new WritableStream({
        async write(value) {
          assert.equal(value, consumed);
          await tick();
          consumed++;
        },
      }),
    );
  assert.equal(consumed, 20);
  assert.equal(maxAhead, 1);
  assert.equal(input.locked, false);
});

test("pipeTo sink failure cancels input with the original error", async () => {
  const reason = new Error("sink failed");
  let cancelled;
  let cleaned = false;
  const input = new ReadableStream(
    {
      pull(controller) {
        controller.enqueue(1);
      },
      async cancel(value) {
        cancelled = value;
        await tick();
        cleaned = true;
      },
    },
    { highWaterMark: 0 },
  );
  await assert.rejects(
    fromReadableStream(input)
      .toReadableStream()
      .pipeTo(
        new WritableStream({
          write() {
            throw reason;
          },
        }),
      ),
    (error) => error === reason,
  );
  assert.equal(cancelled, reason);
  assert.equal(cleaned, true);
  assert.equal(input.locked, false);
});

test("output resumes through a new reader after releasing a pending reader", async () => {
  let deliver;
  const input = new ReadableStream(
    {
      start(controller) {
        deliver = controller;
      },
    },
    { highWaterMark: 0 },
  );
  const output = fromReadableStream(input).toReadableStream();
  const first = output.getReader();
  const abandoned = first.read();
  const rejected = assert.rejects(abandoned, TypeError);
  first.releaseLock();
  await rejected;
  const second = output.getReader();
  const pending = second.read();
  deliver.enqueue(42);
  assert.deepEqual(await pending, { done: false, value: 42 });
  deliver.close();
  assert.equal((await second.read()).done, true);
  assert.equal(input.locked, false);
  second.releaseLock();
});
