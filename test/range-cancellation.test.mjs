import assert from "node:assert/strict";
import { test } from "node:test";
import { from, range, takeWhile, skipWhile, tap } from "../dist/index.js";

for (const name of ["takeWhile", "skipWhile", "tap", "reduce", "forEach"]) {
  test(`${name} does not invoke its callback after cancellation between read and delivery`, async () => {
    const controller = new AbortController();
    const reason = new Error("cancel between read and callback");
    let calls = 0;
    let closed = false;
    const source = {
      *[Symbol.iterator]() {
        try {
          queueMicrotask(() =>
            queueMicrotask(() =>
              queueMicrotask(() => controller.abort(reason)),
            ),
          );
          yield 1;
        } finally {
          closed = true;
        }
      },
    };
    const callback = () => {
      calls++;
      return true;
    };
    const options = { signal: controller.signal };
    const stream = from(source);
    if (name === "reduce" || name === "forEach") {
      const work =
        name === "reduce"
          ? stream.reduce(callback, false, options)
          : stream.forEach(callback, options);
      await assert.rejects(work, (error) => error === reason);
    } else {
      const iterator = stream
        .pipe({ tap, takeWhile, skipWhile }[name](callback))
        .iterate(options);
      await assert.rejects(iterator.next(), (error) => error === reason);
      const report = await iterator.closed;
      assert.equal(report.kind, "cancelled");
      assert.deepEqual(report.suppressedErrors, []);
    }
    assert.equal(calls, 0);
    assert.equal(closed, true);
  });
}

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
