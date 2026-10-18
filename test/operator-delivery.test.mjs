import { test } from "node:test";
import assert from "node:assert/strict";
import * as stream from "../dist/index.js";
import { create } from "../dist/core/stream.js";
import { controlled, deferred, flush } from "./helpers.mjs";

// Exercise the synchronous delivery permitted by the internal Reader protocol.
function readySource(values, onRead = () => {}) {
  return create(() => {
    let index = 0;
    const pull = () => {
      onRead();
      return index < values.length
        ? { done: false, value: values[index++] }
        : { done: true };
    };
    return {
      next: () => Promise.resolve(pull()),
      read: (receive) => receive(pull()),
    };
  });
}

test("zipLatest does not lose synchronous refill notifications before initialization", async () => {
  let reads = 0;
  const other = controlled();
  const iterator = stream.iterate(
    stream.zipLatest([readySource([0, 1, 2, 3], () => reads++), other.source]),
  );
  const first = iterator.next();
  await flush();
  assert.equal(reads, 5);
  assert.equal(other.reads.length, 1);
  other.reads[0].resolve({ value: 9 });
  assert.deepEqual((await first).value, [3, 9]);
  await iterator.return();
  assert.equal((await iterator.closed).kind, "completed");
});

test("concurrent concat refills a synchronous outer source without recursive calls", async () => {
  const count = 4_000;
  let reads = 0;
  const result = await stream.toArray(
    readySource(
      Array.from({ length: count }, () => []),
      () => reads++,
    ).pipe(stream.concat({ concurrency: count })),
  );
  assert.deepEqual(result, []);
  assert.equal(reads, count + 1);
});

test("concurrent child acquisition cancellation still waits for the acquired child's cleanup", async () => {
  for (const combine of [stream.flatten, stream.concat]) {
    const controller = new AbortController();
    const cleanup = deferred();
    const reason = new Error("cancel during child acquisition");
    let reads = 0;
    let returns = 0;
    let finalized = false;
    const child = {
      [Symbol.iterator]() {
        controller.abort(reason);
        return {
          next() {
            reads++;
            return { value: 1 };
          },
          return() {
            returns++;
            return cleanup.promise;
          },
        };
      },
    };
    const iterator = stream.iterate(
      stream.of(child).pipe(
        combine({ concurrency: 2 }),
        stream.finalize(() => {
          finalized = true;
        }),
      ),
      { signal: controller.signal },
    );
    await assert.rejects(iterator.next(), (error) => error === reason);
    await flush();
    assert.equal(reads, 0);
    assert.equal(returns, 1);
    assert.equal(finalized, false);
    cleanup.resolve({ done: true });
    const report = await iterator.closed;
    assert.equal(finalized, true);
    assert.equal(report.kind, "cancelled");
    assert.equal(report.reason, reason);
    assert.deepEqual(report.suppressedErrors, []);
  }
});

for (const name of ["flatten", "concat", "flatMap", "concatMap"]) {
  test(`${name} drains synchronous empty children without overflowing the stack`, async () => {
    let reads = 0;
    const count = 4_000;
    const operator = name.endsWith("Map")
      ? stream[name]((value) => value)
      : stream[name]();
    const source = readySource(
      Array.from({ length: count }, () => readySource([])),
      () => reads++,
    );
    assert.deepEqual(await source.pipe(operator).toArray(), []);
    assert.equal(reads, count + 1);
  });
}
