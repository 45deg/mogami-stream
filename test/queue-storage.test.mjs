import { test } from "node:test";
import assert from "node:assert/strict";
import { fromCallback, broadcast } from "../dist/index.js";

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
