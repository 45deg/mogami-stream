import assert from "node:assert/strict";
import { test } from "node:test";
import * as fluent from "../dist/index.js";
import * as core from "../dist/core/index.js";
import { controlled, deferred, flush, turn } from "./helpers.mjs";

const done = { done: true, value: undefined };
const item = (value) => ({ done: false, value });

test("retained route handles release delivered, queued, and coordinator payloads", async () => {
  const { execFile } = await import("node:child_process");
  const { promisify } = await import("node:util");
  const { fileURLToPath } = await import("node:url");
  const args = [
    "--expose-gc",
    fileURLToPath(new URL("./fixtures/route-retention.mjs", import.meta.url)),
  ];
  await promisify(execFile)(process.execPath, args, { timeout: 8000 });
  await assert.rejects(
    promisify(execFile)(process.execPath, [...args, "--retain-payloads"], {
      timeout: 8000,
    }),
    (error) =>
      error.code === 1 &&
      /route retained delivered, queued, or coordinator payloads/.test(
        error.stderr,
      ),
  );
});

for (const [entry, api] of [
  ["root", fluent],
  ["core", core],
]) {
  test(`${entry} route validates construction before source acquisition`, async () => {
    let acquired = 0;
    const source = {
      [Symbol.iterator]() {
        acquired++;
        return [1][Symbol.iterator]();
      },
    };
    const options = { branches: 2, capacity: 1, select: () => 0 };
    for (const branches of [0, -1, 1.5, NaN, Infinity, [], ["a", "a"], {}])
      assert.throws(() => api.route(source, { ...options, branches }));
    for (const capacity of [0, -1, 1.5, NaN, Infinity])
      assert.throws(() => api.route(source, { ...options, capacity }));
    assert.throws(() => api.route(source, { ...options, default: 2 }));
    assert.throws(() => api.route(source, { ...options, fallback: "unknown" }));
    assert.throws(() => api.route(source, { ...options, select: undefined }));
    assert.throws(() =>
      api.route(source, {
        branches: { a: () => true },
        capacity: 1,
        select: () => "a",
      }),
    );
    const group = api.route(source, options);
    assert.equal(acquired, 0);
    assert.equal((await group.close()).source.kind, "completed");
    assert.equal(acquired, 0);
  });

  test(`${entry} route sends every input to exactly one keyed branch in input order`, async () => {
    const inputs = [{ id: 0 }, { id: 1 }, { id: 2 }, { id: 3 }];
    const calls = [];
    const group = api.route(inputs, {
      branches: ["even", "odd"],
      capacity: 1,
      select(value, { index, signal }) {
        assert.equal(signal.aborted, false);
        calls.push([value, index]);
        return value.id % 2 ? "odd" : "even";
      },
    });
    const [even, odd] = await Promise.all([
      api.toArray(group.branches.even),
      api.toArray(group.branches.odd),
    ]);
    assert.deepEqual(even, [inputs[0], inputs[2]]);
    assert.deepEqual(odd, [inputs[1], inputs[3]]);
    assert.equal(even[0], inputs[0]);
    assert.equal(odd[0], inputs[1]);
    assert.deepEqual(
      calls,
      inputs.map((value, index) => [value, index]),
    );
    const report = await group.closed;
    assert.equal(report.source.kind, "completed");
    assert.deepEqual(Object.keys(report.branches), ["even", "odd"]);
    assert.equal(report.branches.even.kind, "completed");
    assert.equal(report.branches.odd.kind, "completed");
  });

  test(`${entry} route preserves Promise payloads and serializes asynchronous selection`, async () => {
    const selection = deferred();
    const first = Promise.resolve(1);
    const second = Promise.resolve(2);
    const calls = [];
    const group = api.route([first, second], {
      branches: 2,
      capacity: 1,
      select(value, { index }) {
        calls.push(value);
        return index === 0 ? selection.promise : 1;
      },
    });
    const jobs = group.branches.map((branch) => api.toArray(branch));
    await flush();
    assert.deepEqual(calls, [first]);
    selection.resolve(0);
    const result = await Promise.all(jobs);
    assert.equal(result[0][0], first);
    assert.equal(result[1][0], second);
    assert.deepEqual(calls, [first, second]);
    assert.equal((await group.closed).source.kind, "completed");
  });

  test(`${entry} route retains at most one coordinator item behind the selected full branch`, async () => {
    const upstream = controlled();
    const seen = [];
    const group = api.route(upstream.source, {
      branches: 2,
      capacity: 1,
      select(value) {
        seen.push(value);
        return 0;
      },
    });
    const idle = group.branches[1].iterate();
    const demand = idle.next();
    await flush();
    upstream.reads[0].resolve(item(1));
    await flush();
    assert.equal(upstream.reads.length, 2);
    upstream.reads[1].resolve(item(2));
    await flush();
    assert.equal(upstream.reads.length, 2);
    assert.deepEqual(seen, [1, 2]);
    const selected = group.branches[0].iterate();
    assert.deepEqual(await selected.next(), item(1));
    await flush();
    assert.equal(upstream.reads.length, 3);
    assert.deepEqual(await selected.next(), item(2));
    const closing = group.close();
    upstream.reads[2].resolve(done);
    assert.deepEqual(await demand, done);
    await closing;
  });

  test(`${entry} route redirects an item when its full target leaves without replaying queued items`, async () => {
    let reads = 0;
    const group = api.route(
      (function* () {
        for (const value of [1, 2, 3]) {
          reads++;
          yield value;
        }
      })(),
      {
        branches: ["selected", "other"],
        capacity: 1,
        select: () => "selected",
        default: "other",
      },
    );
    const other = group.branches.other.iterate();
    const pending = other.next();
    await flush();
    assert.equal(reads, 2);
    group.branches.selected.detach();
    assert.deepEqual(await pending, item(2));
    assert.deepEqual(await other.next(), item(3));
    assert.deepEqual(await other.next(), done);
    assert.equal((await group.closed).source.kind, "completed");
  });

  test(`${entry} route evaluates departed predicates and never falls through to a later match`, async () => {
    const calls = [];
    const group = api.route([1, 2], {
      branches: {
        first(value) {
          calls.push(["first", value]);
          return true;
        },
        second(value) {
          calls.push(["second", value]);
          return true;
        },
        backup(value) {
          calls.push(["backup", value]);
          return true;
        },
      },
      capacity: 1,
      default: "backup",
    });
    group.branches.first.detach();
    const [second, backup] = await Promise.all([
      api.toArray(group.branches.second),
      api.toArray(group.branches.backup),
    ]);
    assert.deepEqual(second, []);
    assert.deepEqual(backup, [1, 2]);
    assert.deepEqual(calls, [
      ["first", 1],
      ["first", 2],
    ]);
    await group.closed;
  });

  test(`${entry} route evaluates predicates serially and stops at the first match`, async () => {
    const gate = deferred();
    const calls = [];
    const group = api.route([1, 2], {
      branches: {
        async a(value, { index }) {
          calls.push(["a", value, index]);
          if (value === 1) await gate.promise;
          return value === 1;
        },
        b(value, { index }) {
          calls.push(["b", value, index]);
          return true;
        },
      },
      capacity: 1,
    });
    const jobs = Object.values(group.branches).map((branch) =>
      api.toArray(branch),
    );
    await flush();
    assert.deepEqual(calls, [["a", 1, 0]]);
    gate.resolve();
    assert.deepEqual(await Promise.all(jobs), [[1], [2]]);
    assert.deepEqual(calls, [
      ["a", 1, 0],
      ["a", 2, 1],
      ["b", 2, 1],
    ]);
    await group.closed;
  });

  test(`${entry} route ignores unmatched inputs unless fallback is error`, async () => {
    const group = api.route([1, 2, 3], {
      branches: { a: (value) => value === 2 },
      capacity: 1,
    });
    assert.deepEqual(await api.toArray(group.branches.a), [2]);
    assert.equal((await group.closed).source.kind, "completed");
    const failing = api.route([1], {
      branches: { a: () => false },
      capacity: 1,
      fallback: "error",
    });
    await assert.rejects(api.toArray(failing.branches.a));
    const report = await failing.closed;
    assert.equal(report.source.kind, "failed");
    assert.equal(report.branches.a.reason, report.source.reason);
  });

  test(`${entry} route falls back after its default destination leaves while full`, async () => {
    let reads = 0;
    const group = api.route(
      (function* () {
        for (const value of [1, 2, 3]) {
          reads++;
          yield value;
        }
      })(),
      {
        branches: ["selected", "backup", "waiting"],
        capacity: 1,
        select: () => "selected",
        default: "backup",
        fallback: "error",
      },
    );
    group.branches.selected.detach();
    const pending = group.branches.waiting.iterate().next();
    const rejection = assert.rejects(pending);
    await flush();
    assert.equal(reads, 2);
    group.branches.backup.detach();
    await rejection;
    const report = await group.closed;
    assert.equal(report.source.kind, "failed");
    assert.equal(reads, 2);
    assert.equal(report.branches.waiting.kind, "failed");
  });

  test(`${entry} route rejects invalid selection even when fallback is ignore`, async () => {
    for (const invalid of [-1, 2, 0.5, NaN, undefined, "unknown"]) {
      const group = api.route([1], {
        branches: 2,
        capacity: 1,
        select: () => invalid,
        fallback: "ignore",
      });
      await assert.rejects(api.toArray(group.branches[0]));
      assert.equal((await group.closed).source.kind, "failed");
    }
  });

  test(`${entry} route does not acquire on already-aborted signals`, async () => {
    let acquired = 0;
    const reason = new Error("stop before start");
    const group = api.route(
      {
        [Symbol.iterator]() {
          acquired++;
          return [1][Symbol.iterator]();
        },
      },
      {
        branches: 1,
        capacity: 1,
        select: () => 0,
        signal: AbortSignal.abort(reason),
      },
    );
    await assert.rejects(
      group.branches[0].iterate().next(),
      (error) => error === reason,
    );
    assert.equal((await group.closed).source.reason, reason);
    assert.equal(acquired, 0);
  });

  test(`${entry} route cancels endpoints promptly but joins pending classification and cleanup`, async () => {
    const started = deferred();
    const selection = deferred();
    const cleanup = deferred();
    let classifierSignal;
    const reason = new Error("cancel");
    const late = new Error("late classify");
    const group = api.route(
      api.from([1]).pipe(api.finalize(() => cleanup.promise)),
      {
        branches: 2,
        capacity: 1,
        select(_value, { signal }) {
          classifierSignal = signal;
          started.resolve();
          return selection.promise;
        },
      },
    );
    const reads = group.branches.map((branch) => branch.iterate().next());
    const rejected = reads.map((read) =>
      assert.rejects(read, (error) => error === reason),
    );
    await started.promise;
    group.cancel(reason);
    await Promise.all(rejected);
    assert.equal(classifierSignal.aborted, true);
    let closed = false;
    void group.closed.then(() => {
      closed = true;
    });
    cleanup.resolve();
    await flush();
    assert.equal(closed, false);
    selection.reject(late);
    const report = await group.closed;
    assert.equal(report.source.reason, reason);
    assert.ok(report.source.suppressedErrors.includes(late));
  });

  test(`${entry} route preserves one-shot acquisition through wrappers and branch-local cancellation`, async () => {
    const upstream = controlled();
    const group = api.route(upstream.source, {
      branches: 2,
      capacity: 1,
      select: () => 1,
    });
    const first = group.branches[0].iterate();
    const second = group.branches[1].iterate();
    assert.throws(() => group.branches[0].iterate(), TypeError);
    const reason = new Error("local");
    const pending = first.next();
    const rejection = assert.rejects(pending, (error) => error === reason);
    await assert.rejects(first.next(), TypeError);
    first.cancel(reason);
    await rejection;
    assert.deepEqual(await first.next(), done);
    const receiving = second.next();
    await flush();
    upstream.reads[0].resolve(item(9));
    assert.deepEqual(await receiving, item(9));
    await second.return();
    assert.equal((await group.closed).source.kind, "completed");
    await assert.rejects(api.toArray(api.from(group.branches[1])), TypeError);
  });

  test(`${entry} route honors explicit upstream waits while discarding input`, async () => {
    const group = api.route(
      api.tap((_, { index }) => (index % 1024 === 0 ? turn() : undefined))(
        (function* () {
          for (;;) yield 0;
        })(),
      ),
      {
        branches: { selected: () => false },
        capacity: 1,
      },
    );
    const job = api.toArray(group.branches.selected);
    await turn();
    const closing = group.close();
    assert.deepEqual(await job, []);
    await closing;
  });
}

test("fluent route and broadcast branches share acquisition rights with their derived streams", async () => {
  const group = fluent.from([1, 2, 3, 4]).route({
    branches: ["even", "odd"],
    capacity: 1,
    select: (value) => (value % 2 ? "odd" : "even"),
  });
  assert.deepEqual(
    await Promise.all([
      group.branches.even.map((value) => value * 10).toArray(),
      group.branches.odd.take(1).toArray(),
    ]),
    [[20, 40], [1]],
  );
  await group.closed;
  await assert.rejects(group.branches.even.map(String).toArray(), TypeError);
  assert.throws(() => group.branches.even.iterate(), TypeError);

  const broadcast = fluent.from([1, 2]).broadcast({
    branches: ["left", "right"],
    capacity: 1,
  });
  assert.deepEqual(
    await Promise.all([
      broadcast.branches.left.map(String).toArray(),
      broadcast.branches.right.toArray(),
    ]),
    [
      ["1", "2"],
      [1, 2],
    ],
  );
  await broadcast.closed;
  await assert.rejects(broadcast.branches.left.toArray(), TypeError);
});

test("keyed routes retain special property names without inheriting destinations", async () => {
  const group = core.route([1, 2], {
    branches: ["__proto__", "constructor"],
    capacity: 1,
    select: (value) => (value === 1 ? "__proto__" : "constructor"),
  });
  assert.deepEqual(
    await Promise.all([
      core.toArray(group.branches.__proto__),
      core.toArray(group.branches.constructor),
    ]),
    [[1], [2]],
  );
  const report = await group.closed;
  assert.equal(report.branches.__proto__.kind, "completed");
  assert.equal(report.branches.constructor.kind, "completed");
});

test("route does not insert host waits while discarding synchronous input", async () => {
  let hostTurn = false;
  const host = turn().then(() => {
    hostTurn = true;
  });
  const group = fluent.route(fluent.range(0, 4096), {
    branches: { none: () => false },
    capacity: 1,
  });
  assert.deepEqual(await fluent.toArray(group.branches.none), []);
  await group.closed;
  assert.equal(hostTurn, false);
  await host;
});
