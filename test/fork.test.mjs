import assert from "node:assert/strict";
import { test } from "node:test";
import { fork, from, defer, finalize, range } from "../dist/index.js";
import { fork as coreFork } from "../dist/core/index.js";
import { controlled, deferred, flush, turn } from "./helpers.mjs";

const done = { done: true, value: undefined };
const value = (value) => ({ done: false, value });
const settings = { branches: 2, capacity: 1 };

test("fork validates bounds before acquiring the source and exports both entries", async () => {
  assert.equal(fork, coreFork);
  let opened = 0;
  const source = {
    [Symbol.iterator]() {
      opened++;
      return [1][Symbol.iterator]();
    },
  };
  for (const invalid of [
    0,
    -1,
    1.5,
    NaN,
    Infinity,
    Number.MAX_SAFE_INTEGER + 1,
  ]) {
    assert.throws(
      () => fork(source, { ...settings, branches: invalid }),
      RangeError,
    );
    assert.throws(
      () => fork(source, { ...settings, capacity: invalid }),
      RangeError,
    );
  }
  assert.throws(
    () => fork(source, { branches: 2, capacity: Number.MAX_SAFE_INTEGER }),
    RangeError,
  );
  const group = fork(source, settings);
  assert.equal(opened, 0);
  assert.equal((await group.close()).source.kind, "completed");
  assert.equal(opened, 0);
});

test("fork shares one execution, references, and Promise values without awaiting them", async () => {
  let opened = 0;
  const values = [{ id: 1 }, Promise.resolve(2), undefined];
  const group = fork(
    {
      [Symbol.iterator]() {
        opened++;
        return values[Symbol.iterator]();
      },
    },
    settings,
  );
  const results = await Promise.all(
    group.branches.map((b) => from(b).toArray()),
  );
  assert.equal(opened, 1);
  for (const result of results)
    for (let i = 0; i < values.length; i++) assert.equal(result[i], values[i]);
  assert.equal((await group.closed).source.kind, "completed");
});

test("fork bounds an unstarted branch, does not prefetch, and detach wakes existing demand", async () => {
  const upstream = controlled();
  const group = fork(upstream.source, settings);
  const a = group.branches[0].iterate();
  const first = a.next();
  await flush();
  assert.equal(upstream.reads.length, 1);
  upstream.reads[0].resolve(value(0));
  assert.deepEqual(await first, value(0));
  await flush();
  assert.equal(upstream.reads.length, 1);
  const second = a.next();
  await flush();
  assert.equal(upstream.reads.length, 1);
  group.branches[1].detach();
  await flush();
  assert.equal(upstream.reads.length, 2);
  upstream.reads[1].resolve(value(1));
  assert.deepEqual(await second, value(1));
  await a.return();
  assert.equal((await group.closed).source.kind, "completed");
  assert.equal(upstream.returns, 1);
});

test("capacity two preserves FIFO when a queued branch reads during another source read", async () => {
  const upstream = controlled();
  const group = fork(upstream.source, { ...settings, capacity: 2 });
  const [a, b] = group.branches.map((b) => b.iterate());
  let reading = a.next();
  await flush();
  upstream.reads[0].resolve(value(0));
  await reading;
  reading = a.next();
  await flush();
  const queued = b.next();
  upstream.reads[1].resolve(value(1));
  assert.deepEqual(await queued, value(0));
  assert.deepEqual(await reading, value(1));
  assert.deepEqual(await b.next(), value(1));
  await group.close();
});

test("queued values drain independently of a full sibling", async () => {
  const group = fork([0, 1, 2], { branches: 3, capacity: 2 });
  const [a, b] = group.branches.map((b) => b.iterate());
  assert.deepEqual(await a.next(), value(0));
  assert.deepEqual(await a.next(), value(1));
  const blocked = a.next();
  assert.deepEqual(await b.next(), value(0));
  assert.deepEqual(await b.next(), value(1));
  group.branches[2].detach();
  assert.deepEqual(await blocked, value(2));
  await group.close();
});

test("one subscription and one pending next are enforced without disturbing demand", async () => {
  const upstream = controlled();
  const group = fork(upstream.source, { branches: 1, capacity: 1 });
  const branch = group.branches[0];
  const iterator = branch.iterate();
  assert.equal(iterator[Symbol.asyncIterator](), iterator);
  assert.throws(() => branch.iterate(), TypeError);
  const pending = iterator.next();
  await assert.rejects(iterator.next(), TypeError);
  await flush();
  upstream.reads[0].resolve(value(4));
  assert.deepEqual(await pending, value(4));
  await iterator.return();
  assert.throws(() => branch[Symbol.asyncIterator](), TypeError);
  await group.closed;
});

test("from wrappers remain one-shot; take(0) requires owner detachment", async () => {
  const group = fork([1, 2], settings);
  const [unused, branch] = group.branches;
  assert.deepEqual(await from(unused).take(0).toArray(), []);
  unused.detach();
  const wrapper = from(branch);
  assert.deepEqual(await wrapper.toArray(), [1, 2]);
  await assert.rejects(wrapper.toArray(), TypeError);
  await group.closed;
});

test("early take and for-await break leave siblings running", async () => {
  const group = fork(range(0, 5), { branches: 3, capacity: 1 });
  const jobs = [
    from(group.branches[0]).take(1).toArray(),
    (async () => {
      for await (const entry of group.branches[1]) {
        assert.equal(entry, 0);
        break;
      }
    })(),
    from(group.branches[2]).toArray(),
  ];
  const result = await Promise.all(jobs);
  assert.deepEqual(result[0], [0]);
  assert.deepEqual(result[2], [0, 1, 2, 3, 4]);
  assert.equal((await group.closed).source.kind, "completed");
});

test("branch cancellation rejects once and does not cancel siblings", async () => {
  const group = fork([1, 2], settings);
  const [a, b] = group.branches.map((b) => b.iterate());
  const reason = new Error("local");
  const pending = a.next();
  a.cancel(reason);
  await assert.rejects(pending, (e) => e === reason);
  assert.deepEqual(await a.next(), done);
  assert.equal((await a.closed).reason, reason);
  assert.deepEqual(await from(b).toArray(), [1, 2]);
  assert.equal((await group.closed).source.kind, "completed");
});

test("branch throw fails locally, preserves its reason, and never forwards throw", async () => {
  const group = fork([1, 2], settings);
  const [a, b] = group.branches.map((b) => b.iterate());
  const error = new Error("failure");
  const pending = a.next();
  const rejected = assert.rejects(pending, (e) => e === error);
  await assert.rejects(a.throw(error), (e) => e === error);
  await rejected;
  assert.deepEqual(await a.next(), done);
  await assert.rejects(a.return(), (e) => e === error);
  assert.deepEqual(await from(b).toArray(), [1, 2]);
  const report = await group.closed;
  assert.equal(report.branches[0].kind, "failed");
  assert.equal(report.source.kind, "completed");
});

test("already aborted group and branch signals avoid acquisition", async () => {
  let acquired = 0;
  const source = {
    [Symbol.iterator]() {
      acquired++;
      return [1][Symbol.iterator]();
    },
  };
  const error = new Error("abort");
  const group = fork(source, { ...settings, signal: AbortSignal.abort(error) });
  for (const branch of group.branches) {
    const iterator = branch.iterate();
    await assert.rejects(iterator.next(), (e) => e === error);
    assert.deepEqual(await iterator.next(), done);
  }
  assert.equal((await group.closed).source.kind, "cancelled");
  const local = fork(source, { branches: 1, capacity: 1 });
  const iterator = local.branches[0].iterate({
    signal: AbortSignal.abort(error),
  });
  await assert.rejects(iterator.next(), (e) => e === error);
  await local.closed;
  assert.equal(acquired, 0);
});

test("return releases endpoints while a source read and cleanup remain blocked", async () => {
  const read = deferred();
  const cleanup = deferred();
  let returns = 0;
  const group = fork(
    {
      [Symbol.asyncIterator]() {
        return this;
      },
      next() {
        return read.promise;
      },
      return() {
        returns++;
        return cleanup.promise;
      },
    },
    settings,
  );
  const [a, b] = group.branches.map((b) => b.iterate());
  const pending = a.next();
  await flush();
  assert.deepEqual(await a.return(), done);
  assert.deepEqual(await pending, done);
  assert.equal(returns, 0);
  assert.deepEqual(await b.return(), done);
  assert.equal(returns, 1);
  let closed = false;
  void group.closed.then(() => {
    closed = true;
  });
  cleanup.resolve(done);
  await flush();
  assert.equal(closed, false);
  read.resolve(value({ discarded: true }));
  await group.closed;
  assert.equal(returns, 1);
});

test("group stop broadcasts all departures before reentrant source abort handlers", async () => {
  const started = deferred();
  const read = deferred();
  let group;
  const reason = new Error("stop");
  group = fork(
    defer(({ signal }) => {
      signal.addEventListener("abort", () => {
        // These later local failures must not replace the group cancellation.
        for (const branch of group.branches)
          branch.cancel(new Error("reentrant"));
      });
      return {
        [Symbol.asyncIterator]() {
          return this;
        },
        next() {
          started.resolve();
          return read.promise;
        },
      };
    }),
    settings,
  );
  const reads = group.branches.map((b) => b.iterate().next());
  await started.promise;
  group.cancel(reason);
  for (const pending of reads)
    await assert.rejects(pending, (e) => e === reason);
  for (const b of group.branches) assert.equal((await b.closed).reason, reason);
  read.resolve(done);
  assert.equal((await group.closed).source.reason, reason);
});

test("reentrant cancellation during acquisition closes once without calling next", async () => {
  let group,
    reads = 0,
    returns = 0;
  const reason = new Error("acquisition");
  group = fork(
    {
      [Symbol.asyncIterator]() {
        group.cancel(reason);
        return {
          next() {
            reads++;
            return done;
          },
          return() {
            returns++;
            return done;
          },
        };
      },
    },
    settings,
  );
  await assert.rejects(group.branches[0].iterate().next(), (e) => e === reason);
  assert.equal((await group.closed).source.reason, reason);
  assert.equal(reads, 0);
  assert.equal(returns, 1);
});

test("EOF completes endpoints before cleanup, including cleanup that awaits endpoints", async () => {
  let group;
  group = fork(
    from([1]).pipe(
      finalize(async () => {
        await Promise.all(group.branches.map((b) => b.closed));
      }),
    ),
    settings,
  );
  assert.deepEqual(
    await Promise.all(group.branches.map((b) => from(b).toArray())),
    [[1], [1]],
  );
  assert.equal((await group.closed).source.kind, "completed");
});

test("EOF drains unread queues while source cleanup is pending", async () => {
  const cleanup = deferred();
  const group = fork(from([1]).pipe(finalize(() => cleanup.promise)), {
    ...settings,
    capacity: 2,
  });
  const [a, b] = group.branches.map((b) => b.iterate());
  assert.deepEqual(await a.next(), value(1));
  assert.deepEqual(await a.next(), done);
  assert.equal((await a.closed).kind, "completed");
  let groupDone = false;
  void group.closed.then(() => {
    groupDone = true;
  });
  cleanup.resolve();
  await flush();
  assert.equal(groupDone, false);
  assert.deepEqual(await b.next(), value(1));
  assert.equal((await b.closed).kind, "completed");
  await group.closed;
});

test("late cleanup failure stays observable without rewriting completed branch reports", async () => {
  const cleanup = deferred();
  const error = new Error("cleanup");
  const group = fork(from([1]).pipe(finalize(() => cleanup.promise)), {
    ...settings,
    capacity: 2,
  });
  const a = group.branches[0].iterate();
  await a.next();
  assert.deepEqual(await a.next(), done);
  cleanup.reject(error);
  const report = await group.closed;
  assert.equal(report.source.reason, error);
  assert.equal(report.branches[0].kind, "completed");
  assert.equal(report.branches[1].kind, "failed");
  const b = group.branches[1].iterate();
  await assert.rejects(b.next(), (e) => e === error);
  assert.deepEqual(await b.next(), done);
});

test("source rejection discards queues and preserves secondary cleanup failures", async () => {
  const primary = new Error("read");
  const secondary = new Error("cleanup");
  let reads = 0;
  const group = fork(
    {
      [Symbol.asyncIterator]() {
        return this;
      },
      next() {
        if (reads++) throw primary;
        return value(1);
      },
      return() {
        throw secondary;
      },
    },
    { ...settings, capacity: 2 },
  );
  const a = group.branches[0].iterate();
  await a.next();
  await assert.rejects(a.next(), (e) => e === primary);
  const report = await group.closed;
  assert.equal(report.source.reason, primary);
  assert.deepEqual(report.source.suppressedErrors, [secondary]);
  await assert.rejects(
    group.branches[1].iterate().next(),
    (e) => e === primary,
  );
});

test("last failed endpoint cancels source; late read rejection is observed", async () => {
  const upstream = controlled();
  const group = fork(upstream.source, { branches: 1, capacity: 1 });
  const iterator = group.branches[0].iterate();
  const pending = iterator.next();
  const reason = new Error("local");
  const late = new Error("late read");
  await flush();
  const rejection = assert.rejects(pending, (e) => e === reason);
  await assert.rejects(iterator.throw(reason), (e) => e === reason);
  await rejection;
  upstream.reads[0].reject(late);
  const report = await group.closed;
  assert.equal(report.source.kind, "cancelled");
  assert.equal(report.source.reason, reason);
  assert.deepEqual(report.source.suppressedErrors, [late]);
});

test("sustained synchronous input yields to the event loop", async () => {
  const group = fork(
    (function* () {
      let n = 0;
      while (true) yield n++;
    })(),
    settings,
  );
  const jobs = group.branches.map((b) => from(b).forEach(() => {}));
  await turn();
  const closing = group.close();
  await Promise.all(jobs);
  await closing;
});

test("seeded demand and departure schedules preserve bounds and ordered delivery", async () => {
  for (let seed = 1; seed <= 40; seed++) {
    let random = seed;
    const pick = (n) => {
      random = (Math.imul(random, 1664525) + 1013904223) >>> 0;
      return random % n;
    };
    const capacity = 1 + (seed % 3);
    const upstream = controlled();
    const group = fork(upstream.source, { branches: 3, capacity });
    const state = group.branches.map((branch) => ({
      branch,
      iterator: branch.iterate(),
      active: true,
      pending: false,
      delivered: 0,
    }));
    let published = 0;
    const tasks = [];
    for (let step = 0; step < 120; step++) {
      const b = state[pick(state.length)];
      if (b.active && !b.pending && pick(4) !== 0) {
        b.pending = true;
        tasks.push(
          b.iterator.next().then((result) => {
            b.pending = false;
            if (!result.done) assert.equal(result.value, b.delivered++);
          }),
        );
      } else if (step > 50 && b.active && pick(5) === 0) {
        b.active = false;
        b.branch.detach();
      }
      await flush();
      for (const endpoint of state) {
        if (endpoint.active)
          assert.ok(
            upstream.reads.length - endpoint.delivered <= capacity,
            `seed ${seed}: queued + reserved exceeds capacity`,
          );
      }
      assert.ok(upstream.reads.length - published <= 1);
      if (upstream.reads.length > published && pick(3) !== 0) {
        upstream.reads[published].resolve(value(published));
        published++;
      }
      await flush();
    }
    const closing = group.close();
    if (upstream.reads.length > published)
      upstream.reads[published].resolve(value(published));
    await Promise.all(tasks);
    await closing;
    assert.ok(upstream.returns <= 1);
  }
});

test("retained fork handles release delivered and detached payloads", async () => {
  const { execFile } = await import("node:child_process");
  const { promisify } = await import("node:util");
  const { fileURLToPath } = await import("node:url");
  const args = [
    "--expose-gc",
    fileURLToPath(new URL("./fixtures/fork-retention.mjs", import.meta.url)),
  ];
  await promisify(execFile)(process.execPath, args, { timeout: 8000 });
  await assert.rejects(
    promisify(execFile)(process.execPath, [...args, "--retain-payloads"], {
      timeout: 8000,
    }),
    (error) =>
      error.code === 1 &&
      /fork retained delivered or detached payloads/.test(error.stderr),
  );
});

test("EOF preserves a scheduled yield that must drain a lagging endpoint", async () => {
  const upstream = controlled();
  const group = fork(upstream.source, { branches: 2, capacity: 2 });
  const [a, b] = group.branches.map((branch) => branch.iterate());
  for (let i = 0; i < 1023; i++) {
    const pending = a.next();
    await flush();
    upstream.reads[i].resolve(value(i));
    await pending;
    if (i > 0) assert.deepEqual(await b.next(), value(i - 1));
  }
  const eof = a.next();
  await flush();
  const queued = b.next();
  upstream.reads[1023].resolve(done);
  assert.deepEqual(await eof, done);
  assert.deepEqual(await queued, value(1022));
  await group.closed;
});

test("throw after endpoint cancellation preserves the unobserved read rejection", async () => {
  const group = fork([1], { branches: 1, capacity: 1 });
  const iterator = group.branches[0].iterate();
  const reason = new Error("cancelled");
  iterator.cancel(reason);
  await iterator.closed;
  await assert.rejects(
    iterator.throw(new Error("later")),
    (error) => error === reason,
  );
  await assert.rejects(iterator.next(), (error) => error === reason);
  assert.deepEqual(await iterator.next(), done);
  await group.closed;
});
