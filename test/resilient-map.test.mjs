import { test } from "node:test";
import assert from "node:assert/strict";
import * as s from "../dist/index.js";
import { iterateWithScheduler } from "../dist/terminal/iterate.js";
import { createTestClock } from "../dist/internal/test-clock.js";
import { deferred, flush } from "./helpers.mjs";

for (const concurrency of [1, 3]) {
  test(`map retries only the failing item with stable context (${concurrency})`, async () => {
    const calls = [],
      signals = new Map();
    const operator = s.map(
      (value, { index, signal, attempt }) => {
        calls.push([value, index, attempt]);
        if (signals.has(index)) assert.equal(signal, signals.get(index));
        signals.set(index, signal);
        if (value === 2 && attempt < 3) throw new Error("transient");
        return Promise.resolve(value * 2);
      },
      { concurrency, retry: { retries: 2 } },
    );
    assert.deepEqual(await s.of(1, 2, 3).pipe(operator).toArray(), [2, 4, 6]);
    assert.deepEqual(
      calls.filter(([value]) => value === 2),
      [
        [2, 1, 1],
        [2, 1, 2],
        [2, 1, 3],
      ],
    );
    assert.equal(calls.filter(([value]) => value === 1).length, 1);
    assert.equal(calls.filter(([value]) => value === 3).length, 1);
    calls.length = 0;
    signals.clear();
    await s.of(2).pipe(operator).toArray();
    assert.deepEqual(calls, [
      [2, 0, 1],
      [2, 0, 2],
      [2, 0, 3],
    ]);
  });

  test(`mapSettled captures synchronous, asynchronous and thenable failures (${concurrency})`, async () => {
    const errors = [
      new Error("throw"),
      undefined,
      new Error("then getter"),
      new Error("then reject"),
    ];
    const contexts = [];
    const values = await s
      .range(0, 5)
      .pipe(
        s.mapSettled(
          (value, { index, signal }) => {
            contexts.push(index);
            assert.equal(signal.aborted, false);
            if (value === 0) throw errors[0];
            if (value === 1) return Promise.reject(errors[1]);
            if (value === 2)
              return {
                get then() {
                  throw errors[2];
                },
              };
            if (value === 3)
              return {
                then(_yes, no) {
                  no(errors[3]);
                },
              };
            return Promise.resolve("ok");
          },
          { concurrency },
        ),
      )
      .toArray();
    assert.deepEqual(values, [
      ...errors.map((reason) => ({ status: "rejected", reason })),
      { status: "fulfilled", value: "ok" },
    ]);
    assert.deepEqual(contexts, [0, 1, 2, 3, 4]);
  });
}

test("map exhausts its retry budget and rejects with the final error", async () => {
  const errors = [new Error("first"), new Error("last")];
  let calls = 0,
    hooks = 0;
  await assert.rejects(
    s
      .of(1)
      .pipe(
        s.map(
          () => {
            throw errors[calls++];
          },
          {
            retry: {
              retries: 1,
              shouldRetry: () => {
                hooks++;
                return true;
              },
            },
          },
        ),
      )
      .toArray(),
    (e) => e === errors[1],
  );
  assert.equal(calls, 2);
  assert.equal(hooks, 1);
  calls = 0;
  await assert.rejects(
    s
      .of(1)
      .pipe(
        s.map(
          () => {
            calls++;
            throw errors[0];
          },
          { retry: { retries: 0 } },
        ),
      )
      .toArray(),
    (e) => e === errors[0],
  );
  assert.equal(calls, 1);
});

test("map awaits retry selection and passes the failed attempt to delay hooks", async () => {
  const clock = createTestClock(),
    gate = deferred(),
    seen = [];
  const error = new Error("retry");
  const iterator = iterateWithScheduler(
    s.of("job").pipe(
      s.map(
        (value, context) => {
          seen.push(["map", context.index, context.attempt, clock.now()]);
          if (context.attempt < 3) throw error;
          return value;
        },
        {
          retry: {
            retries: 2,
            shouldRetry: async (reason, context) => {
              assert.equal(reason, error);
              assert.equal(context.signal.aborted, false);
              seen.push(["select", context.attempt]);
              await gate.promise;
              return true;
            },
            delayMs: (reason, context) => {
              assert.equal(reason, error);
              seen.push(["delay", context.attempt]);
              return context.attempt * 10;
            },
          },
        },
      ),
    ),
    {},
    clock,
  );
  const first = iterator.next();
  await flush();
  assert.equal(clock.pendingTimers, 0);
  gate.resolve();
  await clock.advanceBy(9);
  assert.equal(seen.filter(([kind]) => kind === "map").length, 1);
  await clock.advanceTo(30);
  assert.equal((await first).value, "job");
  assert.deepEqual(seen, [
    ["map", 0, 1, 0],
    ["select", 1],
    ["delay", 1],
    ["map", 0, 2, 10],
    ["select", 2],
    ["delay", 2],
    ["map", 0, 3, 30],
  ]);
  await iterator.return();
  assert.equal(clock.pendingTimers, 0);
});

test("retry policy can decline, fail, or produce an invalid delay", async () => {
  const original = new Error("mapper"),
    policy = new Error("policy");
  for (const [options, expected] of [
    [{ shouldRetry: async () => false }, original],
    [
      {
        shouldRetry: () => {
          throw policy;
        },
      },
      policy,
    ],
    [{ shouldRetry: () => Promise.reject(policy) }, policy],
    [
      {
        delayMs: () => {
          throw policy;
        },
      },
      policy,
    ],
    [{ delayMs: () => -1 }, RangeError],
  ]) {
    let attempts = 0;
    await assert.rejects(
      s
        .of(1)
        .pipe(
          s.map(
            () => {
              attempts++;
              throw original;
            },
            { retry: { retries: 2, ...options } },
          ),
        )
        .toArray(),
      typeof expected === "function" ? expected : (e) => e === expected,
    );
    assert.equal(attempts, 1);
  }
});

test("map validates all numeric options at construction", () => {
  for (const retries of [-1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
    assert.throws(() => s.map((x) => x, { retry: { retries } }), RangeError);
  }
  for (const delayMs of [-1, NaN, Infinity])
    assert.throws(
      () => s.map((x) => x, { retry: { retries: 1, delayMs } }),
      RangeError,
    );
  for (const operator of [s.map, s.mapSettled]) {
    for (const options of [
      { concurrency: 0 },
      { capacity: 0 },
      { concurrency: 2, capacity: 1 },
    ]) {
      assert.throws(
        () => operator((x) => x, { ...options, retry: { retries: 1 } }),
        RangeError,
      );
    }
  }
});

test("retry waits occupy concurrency slots and ordered outputs remain bounded", async () => {
  const clock = createTestClock(),
    calls = [];
  let reads = 0;
  const source = {
    *[Symbol.iterator]() {
      for (let i = 0; i < 8; i++) {
        reads++;
        yield i;
      }
    },
  };
  const iterator = iterateWithScheduler(
    s.from(source).pipe(
      s.map(
        (value, { attempt }) => {
          calls.push([value, attempt]);
          if (value === 0 && attempt === 1) throw new Error("retry");
          return value;
        },
        { concurrency: 2, capacity: 3, retry: { retries: 1, delayMs: 100 } },
      ),
    ),
    {},
    clock,
  );
  const first = iterator.next();
  await flush();
  assert.equal(reads, 3);
  assert.deepEqual(calls, [
    [0, 1],
    [1, 1],
    [2, 1],
  ]);
  await clock.advanceBy(100);
  assert.equal((await first).value, 0);
  await flush();
  assert.equal(reads, 3);
  const rest = [];
  for (;;) {
    const result = await iterator.next();
    if (result.done) break;
    rest.push(result.value);
  }
  assert.deepEqual(rest, [1, 2, 3, 4, 5, 6, 7]);
});

test("all waiting retries retain their concurrency slots", async () => {
  const clock = createTestClock();
  let reads = 0;
  const iterator = iterateWithScheduler(
    s.range(0, 10).pipe(
      s.tap(() => {
        reads++;
      }),
      s.map(
        (value, { attempt }) => {
          if (attempt === 1) throw new Error("retry");
          return value;
        },
        { concurrency: 2, capacity: 5, retry: { retries: 1, delayMs: 100 } },
      ),
    ),
    {},
    clock,
  );
  const first = iterator.next();
  await flush();
  assert.equal(reads, 2);
  assert.equal(clock.pendingTimers, 2);
  iterator.cancel("stop");
  await assert.rejects(first, (e) => e === "stop");
  await iterator.closed;
  assert.equal(clock.pendingTimers, 0);
});

for (const operator of [s.map, s.mapSettled]) {
  test(`${operator.name} emits unordered completions and stops refilling while idle`, async () => {
    const head = deferred();
    let reads = 0;
    const iterator = s
      .range(0, 5)
      .pipe(
        s.tap(() => {
          reads++;
        }),
        operator((value) => (value === 0 ? head.promise : value), {
          concurrency: 2,
          ordered: false,
          retry: { retries: 1 },
        }),
      )
      .iterate();
    const unwrap = (result) =>
      operator === s.mapSettled ? result.value.value : result.value;
    assert.equal(unwrap(await iterator.next()), 1);
    await flush();
    assert.equal(reads, 2);
    head.resolve(0);
    await flush();
    assert.equal(reads, 2);
    assert.equal(unwrap(await iterator.next()), 0);
    await iterator.return();
  });

  test(`${operator.name} propagates upstream and cleanup failures`, async () => {
    const error = new Error("upstream");
    let calls = 0;
    const broken = {
      *[Symbol.iterator]() {
        throw error;
      },
    };
    await assert.rejects(
      s
        .from(broken)
        .pipe(
          operator(
            () => {
              calls++;
            },
            { retry: { retries: 3 } },
          ),
        )
        .toArray(),
      (e) => e === error,
    );
    assert.equal(calls, 0);
    await assert.rejects(
      s
        .of(1)
        .pipe(
          operator((x) => x, { retry: { retries: 3 } }),
          s.finalize(() => {
            throw error;
          }),
        )
        .toArray(),
      (e) => e === error,
    );
  });

  test(`${operator.name} is lazy and does not open pre-aborted inputs`, async () => {
    let calls = 0;
    const source = s
      .defer(() => {
        calls++;
        return [1];
      })
      .pipe(operator((x) => x, { retry: { retries: 1 } }));
    const iterator = source.iterate();
    assert.equal(calls, 0);
    await iterator.return();
    assert.deepEqual(await source.pipe(s.take(0)).toArray(), []);
    const controller = new AbortController();
    controller.abort("stop");
    await assert.rejects(
      source.toArray({ signal: controller.signal }),
      (e) => e === "stop",
    );
    assert.equal(calls, 0);
  });

  for (const concurrency of [1, 2]) {
    test(`${operator.name} cancellation observes late mapper errors (${concurrency})`, async () => {
      const gate = deferred(),
        late = new Error("late");
      let signal,
        closed = false,
        calls = 0;
      const iterator = s
        .of(1)
        .pipe(
          operator(
            (_value, context) => {
              calls++;
              signal = context.signal;
              return gate.promise;
            },
            { concurrency, retry: { retries: 3 } },
          ),
        )
        .iterate();
      const pending = assert.rejects(iterator.next(), (e) => e === "stop");
      await flush();
      iterator.cancel("stop");
      await pending;
      const closing = iterator.closed.then((report) => {
        closed = true;
        return report;
      });
      await flush();
      assert.equal(closed, false);
      assert.equal(signal.aborted, true);
      gate.reject(late);
      const report = await closing;
      assert.equal(report.reason, "stop");
      assert.deepEqual(report.suppressedErrors, [late]);
      assert.equal(calls, 1);
    });
  }
}

test("map cancels a delay without another attempt", async () => {
  const clock = createTestClock();
  let attempts = 0;
  const iterator = iterateWithScheduler(
    s.of(1).pipe(
      s.map(
        () => {
          attempts++;
          throw new Error("retry");
        },
        { retry: { retries: 3, delayMs: 100 } },
      ),
    ),
    {},
    clock,
  );
  const pending = assert.rejects(iterator.next(), (e) => e === "stop");
  await flush();
  assert.equal(clock.pendingTimers, 1);
  iterator.cancel("stop");
  await pending;
  await iterator.closed;
  assert.equal(clock.pendingTimers, 0);
  await clock.advanceBy(1000);
  assert.equal(attempts, 1);
});

test("map waits for asynchronous retry selection during cancellation", async () => {
  const policy = deferred();
  let attempts = 0,
    delays = 0;
  const iterator = s
    .of(1)
    .pipe(
      s.map(
        () => {
          attempts++;
          throw new Error("retry");
        },
        {
          retry: {
            retries: 2,
            shouldRetry: () => policy.promise,
            delayMs: () => {
              delays++;
              return 1;
            },
          },
        },
      ),
    )
    .iterate();
  const pending = assert.rejects(iterator.next(), (e) => e === "stop");
  await flush();
  iterator.cancel("stop");
  await pending;
  let closed = false;
  const closing = iterator.closed.then(() => {
    closed = true;
  });
  await flush();
  assert.equal(closed, false);
  policy.resolve(true);
  await closing;
  assert.equal(attempts, 1);
  assert.equal(delays, 0);
});

test("an exhausted ordered retry fails promptly behind an unfinished head", async () => {
  const gate = deferred(),
    error = new Error("exhausted");
  let attempts = 0;
  const iterator = s
    .of(0, 1)
    .pipe(
      s.map(
        (value) => {
          if (value === 0) return gate.promise;
          attempts++;
          throw error;
        },
        { concurrency: 2, retry: { retries: 2 } },
      ),
    )
    .iterate();
  await assert.rejects(iterator.next(), (e) => e === error);
  assert.equal(attempts, 3);
  gate.resolve(0);
  assert.equal((await iterator.closed).reason, error);
});

test("map catches throwing then getters and rejecting thenables", async () => {
  for (const mode of ["then-getter", "then-reject", "promise-reject"]) {
    const error = new Error(mode);
    let attempts = 0;
    const result = await s
      .of(1)
      .pipe(
        s.map(
          (_value, { attempt }) => {
            attempts++;
            if (attempt === 2) return "ok";
            if (mode === "then-getter")
              return {
                get then() {
                  throw error;
                },
              };
            if (mode === "then-reject")
              return {
                then(_resolve, reject) {
                  reject(error);
                },
              };
            return Promise.reject(error);
          },
          { retry: { retries: 1 } },
        ),
      )
      .toArray();
    assert.deepEqual(result, ["ok"]);
    assert.equal(attempts, 2);
  }
});

test("ordered mapSettled buffers item failures within capacity behind a pending head", async () => {
  const head = deferred(),
    error = new Error("item");
  let reads = 0,
    ready = false;
  const iterator = s
    .range(0, 8)
    .pipe(
      s.tap(() => {
        reads++;
      }),
      s.mapSettled(
        (value) => {
          if (value === 0) return head.promise;
          if (value === 1) throw error;
          return value;
        },
        { concurrency: 2, capacity: 3 },
      ),
    )
    .iterate();
  const first = iterator.next().then((result) => {
    ready = true;
    return result;
  });
  await flush();
  assert.equal(ready, false);
  assert.equal(reads, 3);
  head.resolve(0);
  assert.deepEqual((await first).value, { status: "fulfilled", value: 0 });
  await flush();
  assert.equal(reads, 3);
  assert.deepEqual((await iterator.next()).value, {
    status: "rejected",
    reason: error,
  });
  await iterator.return();
});

for (const operator of [s.map, s.mapSettled]) {
  test(`${operator.name} observes work returned after synchronous mapper cancellation`, async () => {
    const controller = new AbortController(),
      gate = deferred(),
      late = new Error("late");
    let calls = 0;
    const iterator = s
      .range(0, 10)
      .pipe(
        operator(
          () => {
            calls++;
            controller.abort("stop");
            return gate.promise;
          },
          { concurrency: 3, retry: { retries: 2 } },
        ),
      )
      .iterate({ signal: controller.signal });
    await assert.rejects(iterator.next(), (e) => e === "stop");
    assert.equal(calls, 1);
    gate.reject(late);
    assert.deepEqual((await iterator.closed).suppressedErrors, [late]);
  });
}

test("map and mapSettled expose attempt one without a retry option", async () => {
  for (const operator of [s.map, s.mapSettled]) {
    const attempts = [];
    await s
      .of(1, 2)
      .pipe(
        operator((value, { attempt }) => {
          attempts.push(attempt);
          return value;
        }),
      )
      .toArray();
    assert.deepEqual(attempts, [1, 1]);
  }
  assert.equal("mapRetry" in s, false);
});

for (const concurrency of [1, 3]) {
  test(`mapSettled emits eventual success or final failure after retries (${concurrency})`, async () => {
    const errors = [new Error("first"), new Error("second"), new Error("last")];
    const calls = [];
    const outcomes = await s
      .of("eventual", "failed", "ok")
      .pipe(
        s.mapSettled(
          (value, { index, attempt }) => {
            calls.push([value, index, attempt]);
            if (value === "failed" || (value === "eventual" && attempt < 3))
              throw errors[attempt - 1];
            return value;
          },
          { concurrency, retry: { retries: 2 } },
        ),
      )
      .toArray();
    assert.deepEqual(outcomes, [
      { status: "fulfilled", value: "eventual" },
      { status: "rejected", reason: errors[2] },
      { status: "fulfilled", value: "ok" },
    ]);
    assert.deepEqual(
      calls.filter(([value]) => value === "failed"),
      [
        ["failed", 1, 1],
        ["failed", 1, 2],
        ["failed", 1, 3],
      ],
    );
    assert.equal(calls.filter(([value]) => value === "ok").length, 1);
  });
}

test("mapSettled obeys retry selection and settles policy failures", async () => {
  const error = new Error("mapper"),
    policy = new Error("policy");
  for (const [retry, expected] of [
    [{ retries: 3, shouldRetry: async () => false }, error],
    [
      {
        retries: 3,
        shouldRetry: async () => {
          throw policy;
        },
      },
      policy,
    ],
    [{ retries: 3, delayMs: () => -1 }, RangeError],
  ]) {
    let calls = 0;
    const [result] = await s
      .of(1)
      .pipe(
        s.mapSettled(
          () => {
            calls++;
            throw error;
          },
          { retry },
        ),
      )
      .toArray();
    assert.equal(result.status, "rejected");
    if (expected === RangeError) assert.ok(result.reason instanceof RangeError);
    else assert.equal(result.reason, expected);
    assert.equal(calls, 1);
  }
  assert.throws(
    () => s.mapSettled((x) => x, { retry: { retries: -1 } }),
    RangeError,
  );
  assert.throws(
    () => s.mapSettled((x) => x, { retry: { retries: 1, delayMs: Infinity } }),
    RangeError,
  );
});

test("mapSettled retry delays retain slots and release timers on cancellation", async () => {
  const clock = createTestClock();
  let reads = 0,
    calls = 0;
  const iterator = iterateWithScheduler(
    s.range(0, 5).pipe(
      s.tap(() => {
        reads++;
      }),
      s.mapSettled(
        () => {
          calls++;
          throw new Error("retry");
        },
        { concurrency: 2, capacity: 5, retry: { retries: 2, delayMs: 100 } },
      ),
    ),
    {},
    clock,
  );
  const failed = assert.rejects(iterator.next(), (e) => e === "stop");
  await flush();
  assert.equal(reads, 2);
  assert.equal(clock.pendingTimers, 2);
  iterator.cancel("stop");
  await failed;
  const report = await iterator.closed;
  assert.equal(report.kind, "cancelled");
  assert.deepEqual(report.suppressedErrors, []);
  assert.equal(clock.pendingTimers, 0);
  await clock.advanceBy(1000);
  assert.equal(calls, 2);
});

test("retry contexts remain immutable snapshots and policies see the failed attempt", async () => {
  for (const operator of [s.map, s.mapSettled]) {
    const contexts = [],
      policies = [];
    await s
      .of(1)
      .pipe(
        operator(
          (_value, context) => {
            contexts.push(context);
            if (context.attempt < 3) throw new Error("retry");
            return 1;
          },
          {
            retry: {
              retries: 2,
              shouldRetry: (_error, context) => {
                policies.push(context);
                return true;
              },
            },
          },
        ),
      )
      .toArray();
    assert.deepEqual(
      contexts.map(({ attempt }) => attempt),
      [1, 2, 3],
    );
    assert.equal(new Set(contexts).size, 3);
    assert.equal(policies[0], contexts[0]);
    assert.equal(policies[1], contexts[1]);
    assert.ok(
      contexts.every(
        ({ index, signal }) => index === 0 && signal === contexts[0].signal,
      ),
    );
  }
});
