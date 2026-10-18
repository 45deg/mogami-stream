import { test } from "node:test";
import assert from "node:assert/strict";
import * as s from "../dist/index.js";
import { deferred, flush, controlled } from "./helpers.mjs";
import { iterateWithScheduler } from "../dist/terminal/iterate.js";
import { createTestClock } from "../dist/internal/test-clock.js";

test("dedupe compares consecutive keys with Object.is and resets per execution", async () => {
  const source = s
    .of(undefined, undefined, NaN, NaN, 0, -0, -0, 1, 1, 0)
    .pipe(s.dedupe());
  for (let i = 0; i < 2; i++)
    assert.deepEqual(await source.toArray(), [undefined, NaN, 0, -0, 1, 0]);
  const a = { id: 1 },
    b = { id: 1 },
    c = { id: 2 };
  const calls = [];
  assert.deepEqual(
    await s
      .of(a, b, c, a)
      .pipe(
        s.dedupe(async (value, { index, signal }) => {
          assert.equal(signal.aborted, false);
          calls.push(index);
          return value.id;
        }),
      )
      .toArray(),
    [a, c, a],
  );
  assert.deepEqual(calls, [0, 1, 2, 3]);
  const promise = Promise.resolve(1);
  assert.deepEqual(await s.of(promise, promise).pipe(s.dedupe()).toArray(), [
    promise,
  ]);
});

test("filterMap awaits mapping and drops only nullish results", async () => {
  const calls = [];
  const source = s.of(null, 0, undefined, false, "", 1).pipe(
    s.filterMap(async (value, { index, signal }) => {
      assert.equal(signal.aborted, false);
      calls.push(index);
      return value;
    }),
  );
  assert.deepEqual(await source.toArray(), [0, false, "", 1]);
  assert.deepEqual(calls, [0, 1, 2, 3, 4, 5]);
  calls.length = 0;
  await source.toArray();
  assert.deepEqual(calls, [0, 1, 2, 3, 4, 5]);
});

test("skipWhile stops testing at the first false and resets per execution", async () => {
  const calls = [];
  const source = s.of(1, 2, 3, 1).pipe(
    s.skipWhile(async (value, { index, signal }) => {
      assert.equal(signal.aborted, false);
      calls.push(index);
      return value < 3;
    }),
  );
  for (let i = 0; i < 2; i++) {
    calls.length = 0;
    assert.deepEqual(await source.toArray(), [3, 1]);
    assert.deepEqual(calls, [0, 1, 2]);
  }
  assert.deepEqual(
    await s
      .of(1, 2)
      .pipe(s.skipWhile(() => true))
      .toArray(),
    [],
  );
  const promise = Promise.resolve(1);
  assert.deepEqual(
    await s
      .of(promise)
      .pipe(s.skipWhile(() => false))
      .toArray(),
    [promise],
  );
});

for (const name of ["dedupe", "filterMap", "skipWhile"]) {
  test(`${name} is lazy, handles empty inputs, and closes on callback failure`, async () => {
    const error = new Error("callback");
    let opened = 0,
      closed = 0;
    const source = s
      .defer(() => {
        opened++;
        return s.of(1).pipe(
          s.finalize(() => {
            closed++;
          }),
        );
      })
      .pipe(
        s[name](() => {
          throw error;
        }),
      );
    assert.equal(opened, 0);
    assert.deepEqual(await source.pipe(s.take(0)).toArray(), []);
    assert.equal(opened, 0);
    assert.deepEqual(
      await s
        .empty()
        .pipe(s[name](() => assert.fail()))
        .toArray(),
      [],
    );
    await assert.rejects(source.toArray(), (e) => e === error);
    assert.equal(closed, 1);
  });

  test(`${name} cancels a pending callback and waits for it before closing`, async () => {
    const gate = deferred();
    let signal;
    const iterator = s
      .of(1, 2)
      .pipe(
        s[name](async (_value, context) => {
          signal = context.signal;
          return gate.promise;
        }),
      )
      .iterate();
    const pending = iterator.next();
    await flush();
    const reason = new Error("cancel");
    iterator.cancel(reason);
    await assert.rejects(pending, (e) => e === reason);
    assert.equal(signal.aborted, true);
    let closed = false;
    void iterator.closed.then(() => {
      closed = true;
    });
    await flush();
    assert.equal(closed, false);
    gate.resolve(true);
    assert.equal((await iterator.closed).kind, "cancelled");
  });
}

test("catchError passes values, recovers once after cleanup, and resets", async () => {
  const failure = new Error("upstream");
  const calls = [];
  const source = s
    .defer(() =>
      (async function* () {
        try {
          yield 1;
          throw failure;
        } finally {
          calls.push("cleanup");
        }
      })(),
    )
    .pipe(
      s.catchError(async (error, { signal }) => {
        assert.equal(error, failure);
        assert.equal(signal.aborted, false);
        calls.push("handler");
        return [2, 3];
      }),
      s.map((value) => value * 10),
    );
  assert.deepEqual(calls, []);
  for (let i = 0; i < 2; i++)
    assert.deepEqual(await source.toArray(), [10, 20, 30]);
  assert.deepEqual(calls, ["cleanup", "handler", "cleanup", "handler"]);
  assert.deepEqual(
    await s
      .empty()
      .pipe(s.catchError(() => assert.fail()))
      .toArray(),
    [],
  );
  assert.deepEqual(await source.pipe(s.take(0)).toArray(), []);
});

test("catchError handles acquisition and undefined failures, retaining Promise items", async () => {
  const promise = Promise.resolve(1);
  for (const failure of [new Error("acquire"), undefined]) {
    const source = {
      [Symbol.asyncIterator]() {
        throw failure;
      },
    };
    assert.deepEqual(
      await s
        .from(source)
        .pipe(
          s.catchError((error) => {
            assert.equal(error, failure);
            return [promise];
          }),
        )
        .toArray(),
      [promise],
    );
  }
});

test("catchError waits for failed concurrent work and finalizers before recovery", async () => {
  const work = deferred(),
    cleanup = deferred();
  const failure = new Error("mapper");
  let recovered = false,
    firstSignal;
  const iterator = s
    .of(1, 2)
    .pipe(
      s.map(
        async (value, { signal }) => {
          if (value === 1) {
            firstSignal = signal;
            return work.promise;
          }
          throw failure;
        },
        { concurrency: 2 },
      ),
      s.finalize(() => cleanup.promise),
      s.catchError(() => {
        recovered = true;
        return [3];
      }),
    )
    .iterate();
  const pending = iterator.next();
  await flush();
  assert.equal(firstSignal.aborted, true);
  assert.equal(recovered, false);
  work.resolve(1);
  await flush();
  assert.equal(recovered, false);
  cleanup.resolve();
  assert.deepEqual(await pending, { done: false, value: 3 });
  await iterator.return();
});

test("catchError does not recover handler, replacement, downstream, or cleanup failures", async () => {
  const primary = new Error("primary"),
    secondary = new Error("secondary");
  const broken = () =>
    s.defer(() => {
      throw primary;
    });
  for (const handler of [
    () => {
      throw secondary;
    },
    async () => {
      throw secondary;
    },
    () =>
      s.defer(() => {
        throw secondary;
      }),
  ])
    await assert.rejects(
      broken().pipe(s.catchError(handler)).toArray(),
      (e) => e === secondary,
    );
  await assert.rejects(
    s
      .of(1)
      .pipe(
        s.catchError(() => assert.fail()),
        s.map(() => {
          throw secondary;
        }),
      )
      .toArray(),
    (e) => e === secondary,
  );
  let called = false;
  await assert.rejects(
    broken()
      .pipe(
        s.finalize(() => {
          throw secondary;
        }),
        s.catchError(() => {
          called = true;
          return [];
        }),
      )
      .toArray(),
    (e) => e === secondary,
  );
  assert.equal(called, false);
});

test("catchError does not recover cancellation or late errors", async () => {
  const input = controlled();
  let called = false;
  const iterator = s
    .from(input.source)
    .pipe(
      s.catchError(() => {
        called = true;
        return [];
      }),
    )
    .iterate();
  const pending = iterator.next();
  await flush();
  const reason = new Error("cancel"),
    late = new Error("late");
  iterator.cancel(reason);
  await assert.rejects(pending, (e) => e === reason);
  input.reads[0].reject(late);
  const report = await iterator.closed;
  assert.equal(report.kind, "cancelled");
  assert.equal(report.reason, reason);
  assert.ok(report.suppressedErrors.includes(late));
  assert.equal(called, false);
  assert.equal(input.returns, 1);
});

test("catchError keeps execution-wide iterator ownership", async () => {
  const iterator = [1, 2][Symbol.iterator]();
  await assert.rejects(
    s.chain(s.from(iterator).pipe(s.catchError(() => [])), iterator).toArray(),
    TypeError,
  );
});

test("catchError recovers deadline failures while downstream is idle using the execution clock", async () => {
  const clock = createTestClock();
  let failure;
  const iterator = iterateWithScheduler(
    s.of(1, 2).pipe(
      s.deadline(10),
      s.catchError((error) => {
        failure = error;
        return [3];
      }),
    ),
    {},
    clock,
  );
  assert.deepEqual(await iterator.next(), { done: false, value: 1 });
  await clock.advanceBy(10);
  assert.equal(failure, undefined);
  assert.deepEqual(await iterator.next(), { done: false, value: 3 });
  assert.ok(failure instanceof s.DeadlineExceededError);
  assert.deepEqual(await iterator.next(), { done: true, value: undefined });
  const report = await iterator.closed;
  assert.equal(report.kind, "completed");
  assert.deepEqual(report.suppressedErrors, []);
  assert.equal(clock.pendingTimers, 0);
});

test("catchError cancellation during an asynchronous handler prevents fallback acquisition", async () => {
  const gate = deferred();
  let signal,
    opened = false;
  const iterator = s
    .defer(() => {
      throw new Error("source");
    })
    .pipe(
      s.catchError(async (_error, context) => {
        signal = context.signal;
        await gate.promise;
        return s.defer(() => {
          opened = true;
          return [1];
        });
      }),
    )
    .iterate();
  const pending = iterator.next();
  await flush();
  const reason = new Error("cancel handler");
  iterator.cancel(reason);
  await assert.rejects(pending, (e) => e === reason);
  assert.equal(signal.aborted, true);
  gate.resolve();
  assert.equal((await iterator.closed).kind, "cancelled");
  assert.equal(opened, false);
});

test("a downstream catchError can recover a replacement failure", async () => {
  const failure = new Error("replacement");
  assert.deepEqual(
    await s
      .defer(() => {
        throw new Error("source");
      })
      .pipe(
        s.catchError(() =>
          s.defer(() => {
            throw failure;
          }),
        ),
        s.catchError((error) => {
          assert.equal(error, failure);
          return [1];
        }),
      )
      .toArray(),
    [1],
  );
});
