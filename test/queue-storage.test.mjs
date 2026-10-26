import { test } from "node:test";
import assert from "node:assert/strict";
import { fromCallback, broadcast } from "../dist/index.js";
import { flush } from "./helpers.mjs";

function payload(index) {
  switch (index % 7) {
    case 0:
      return undefined;
    case 1:
      return null;
    case 2:
      return false;
    case 3:
      return 0;
    case 4:
      return "";
    case 5:
      return Promise.resolve(index);
    default:
      return { index };
  }
}

for (const capacity of [1, 256, 257, Number.MAX_SAFE_INTEGER]) {
  for (const overflow of ["drop-oldest", "drop-newest"]) {
    test(`push preserves payloads across capacity ${capacity} with ${overflow}`, async () => {
      const values = Array.from({ length: 1000 }, (_, index) =>
        index % 3 === 0
          ? Promise.resolve(index)
          : index % 3 === 1
            ? undefined
            : { index },
      );
      let cleaned = 0;
      const iterator = fromCallback(
        (observer) => {
          for (const value of values) observer.next(value);
          observer.complete();
          return () => {
            cleaned++;
          };
        },
        { capacity, overflow },
      ).iterate();
      const expected =
        overflow === "drop-oldest"
          ? values.slice(-Math.min(capacity, values.length))
          : values.slice(0, capacity);
      for (const value of expected) {
        const result = await iterator.next();
        assert.equal(result.done, false);
        assert.equal(result.value, value);
      }
      assert.equal((await iterator.next()).done, true);
      assert.equal((await iterator.closed).kind, "completed");
      assert.equal(cleaned, 1);
    });
  }
}

for (const capacity of [1, 256, 257, Math.floor(Number.MAX_SAFE_INTEGER / 2)]) {
  test(`broadcast preserves queued payloads with capacity ${capacity}`, async () => {
    const values = [Promise.resolve(1), undefined, { id: 2 }];
    const group = broadcast(values, { capacity, branches: 2 });
    const [left, right] = group.branches.map((branch) => branch.iterate());
    for (const value of values) {
      // Read left first so right exercises queue storage rather than direct handoff.
      assert.equal((await left.next()).value, value);
      assert.equal((await right.next()).value, value);
    }
    const results = await Promise.all([left.next(), right.next()]);
    assert.ok(results.every((result) => result.done));
    assert.equal((await group.closed).source.kind, "completed");
  });
}

for (const capacity of [256, 257]) {
  for (const overflow of ["error", "drop-oldest", "drop-newest"]) {
    test(`push wraps and refills capacity ${capacity} with ${overflow}`, async () => {
      let observer;
      let cleaned = 0;
      let index = 0;
      let drops = 0;
      let reportedDrops = 0;
      const expected = [];
      const iterator = fromCallback(
        (sink) => {
          observer = sink;
          sink.next("ready");
          return () => {
            cleaned++;
          };
        },
        {
          capacity,
          overflow,
          onDrop(info) {
            reportedDrops = info.count;
          },
        },
      ).iterate();
      assert.deepEqual(await iterator.next(), { done: false, value: "ready" });

      function fill(count) {
        for (let i = 0; i < count; i++) {
          const value = payload(index++);
          let accepted = true;
          if (expected.length === capacity) {
            assert.notEqual(overflow, "error");
            drops++;
            if (overflow === "drop-oldest") expected.shift();
            else accepted = false;
          }
          if (accepted) expected.push(value);
          assert.equal(observer.next(value), accepted);
          assert.equal(reportedDrops, drops);
        }
      }
      async function drain(count) {
        for (let i = 0; i < count; i++) {
          const result = await iterator.next();
          assert.equal(result.done, false);
          assert.equal(result.value, expected.shift());
        }
      }

      fill(capacity);
      for (let cycle = 0; cycle < 8; cycle++) {
        const count = cycle % 2 ? capacity : Math.floor(capacity / 2) + 1;
        await drain(count);
        fill(count + (overflow === "error" ? 0 : 3));
        assert.equal(expected.length, capacity);
        assert.equal(cleaned, 0);
      }
      observer.complete();
      await drain(expected.length);
      assert.equal((await iterator.next()).done, true);
      assert.equal((await iterator.closed).kind, "completed");
      assert.equal(cleaned, 1);
    });
  }

  test(`broadcast wraps and refills capacity ${capacity} while bounding a lagging branch`, async () => {
    const batches = Array.from({ length: 8 }, (_, cycle) =>
      cycle % 2 ? capacity : Math.floor(capacity / 2) + 1,
    );
    const values = Array.from(
      { length: capacity + batches.reduce((sum, count) => sum + count, 0) },
      (_, index) => payload(index),
    );
    let pulled = 0;
    const group = broadcast(
      {
        *[Symbol.iterator]() {
          for (const value of values) {
            pulled++;
            yield value;
          }
        },
      },
      { capacity, branches: 2 },
    );
    const [fast, slow] = group.branches.map((branch) => branch.iterate());
    let fastIndex = 0;
    let slowIndex = 0;
    async function read(iterator, index) {
      const result = await iterator.next();
      assert.equal(result.done, false);
      assert.equal(result.value, values[index]);
    }
    try {
      for (let i = 0; i < capacity; i++) await read(fast, fastIndex++);
      for (const count of batches) {
        let settled = false;
        const before = pulled;
        const blocked = fast.next().then((result) => {
          settled = true;
          return result;
        });
        await flush();
        assert.equal(settled, false);
        assert.equal(pulled, before);

        for (let i = 0; i < count; i++) await read(slow, slowIndex++);
        const result = await blocked;
        assert.equal(result.done, false);
        assert.equal(result.value, values[fastIndex++]);
        for (let i = 1; i < count; i++) await read(fast, fastIndex++);
        assert.equal(fastIndex - slowIndex, capacity);
        assert.equal(pulled, fastIndex);
      }
      const eof = fast.next();
      await read(slow, slowIndex++);
      assert.equal((await eof).done, true);
      while (slowIndex < values.length) await read(slow, slowIndex++);
      assert.equal((await slow.closed).kind, "completed");
      assert.equal((await slow.next()).done, true);
      assert.equal((await group.closed).source.kind, "completed");
    } finally {
      await group.close();
    }
  });
}
