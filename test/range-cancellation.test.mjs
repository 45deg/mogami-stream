import assert from "node:assert/strict";
import { test } from "node:test";
import { from, range, takeWhile } from "../dist/index.js";

test("takeWhile does not invoke its predicate after cancellation between read and delivery", async () => {
  const controller = new AbortController();
  const reason = new Error("cancel between read and predicate");
  let calls = 0;
  let closed = false;
  const source = {
    *[Symbol.iterator]() {
      try {
        queueMicrotask(() =>
          queueMicrotask(() => queueMicrotask(() => controller.abort(reason))),
        );
        yield 1;
      } finally {
        closed = true;
      }
    },
  };
  const iterator = from(source)
    .pipe(
      takeWhile(() => {
        calls++;
        return true;
      }),
    )
    .iterate({ signal: controller.signal });
  await assert.rejects(iterator.next(), (error) => error === reason);
  const report = await iterator.closed;
  assert.equal(calls, 0);
  assert.equal(closed, true);
  assert.equal(report.kind, "cancelled");
  assert.deepEqual(report.suppressedErrors, []);
});

test("range matches exact integer arithmetic across safe-integer boundaries", async () => {
  const max = Number.MAX_SAFE_INTEGER;
  const bounds = [-max, -max + 1, -1, 0, 1, max - 1, max];
  for (const start of bounds) {
    for (const end of bounds) {
      for (const step of [-max, -max + 1, max - 1, max]) {
        const expected = [];
        for (
          let value = BigInt(start);
          step > 0 ? value < BigInt(end) : value > BigInt(end);
          value += BigInt(step)
        ) {
          expected.push(Number(value));
        }
        assert.deepEqual(await range(start, end, step).toArray(), expected);
      }
    }
  }
});
