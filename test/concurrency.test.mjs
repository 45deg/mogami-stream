import { test } from "node:test";
import assert from "node:assert/strict";
import * as s from "../dist/index.js";
import { iterateWithScheduler } from "../dist/terminal/iterate.js";
import {
  VirtualScheduler,
  controlled,
  deferred,
  flush,
  turn,
} from "./helpers.mjs";

test("ordered map stops at capacity behind a blocked head, then preserves order", async () => {
  const head = deferred();
  let reads = 0,
    active = 0,
    peak = 0;
  const source = {
    *[Symbol.iterator]() {
      for (let i = 0; i < 20; i++) {
        reads++;
        yield i;
      }
    },
  };
  const iterator = s.iterate(
    s.from(source).pipe(
      s.map(
        async (value) => {
          active++;
          peak = Math.max(peak, active);
          if (value === 0) await head.promise;
          active--;
          return value;
        },
        { concurrency: 3, capacity: 5 },
      ),
    ),
  );
  const first = iterator.next();
  await flush();
  assert.equal(reads, 5);
  assert.ok(peak <= 3);
  head.resolve();
  assert.equal((await first).value, 0);
  const paused = reads;
  await flush();
  await turn();
  assert.equal(reads, paused);
  const rest = [];
  for (;;) {
    const result = await iterator.next();
    if (result.done) break;
    rest.push(result.value);
  }
  assert.deepEqual(
    rest,
    Array.from({ length: 19 }, (_, i) => i + 1),
  );
});

test("map limits pending reservations and running callbacks together", async () => {
  const input = controlled(),
    mapper = deferred();
  let starts = 0;
  const iterator = s.iterate(
    s.from(input.source).pipe(
      s.map(
        () => {
          starts++;
          return mapper.promise;
        },
        { concurrency: 2, capacity: 2 },
      ),
    ),
  );
  const first = iterator.next();
  await flush();
  assert.equal(input.reads.length, 1);
  input.reads[0].resolve({ value: 0 });
  await flush();
  assert.equal(starts, 1);
  assert.equal(input.reads.length, 2);
  input.reads[1].resolve({ value: 1 });
  await flush();
  assert.equal(starts, 2);
  assert.equal(input.reads.length, 2);
  mapper.resolve("done");
  await first;
  await iterator.return();
});

test("unordered map emits in observed completion order", async () => {
  const gates = [deferred(), deferred(), deferred()];
  const iterator = s.iterate(
    s
      .of(0, 1, 2)
      .pipe(s.map((i) => gates[i].promise, { concurrency: 3, ordered: false })),
  );
  const first = iterator.next();
  await flush();
  gates[2].resolve(2);
  assert.equal((await first).value, 2);
  gates[1].resolve(1);
  gates[0].resolve(0);
  await flush();
  assert.equal((await iterator.next()).value, 1);
  assert.equal((await iterator.next()).value, 0);
  assert.equal((await iterator.next()).done, true);
});

test("a later ordered mapper failure interrupts a blocked first mapper", async () => {
  const gate = deferred(),
    error = new Error("second");
  const iterator = s.iterate(
    s.of(0, 1).pipe(
      s.map((i) => (i === 0 ? gate.promise : Promise.reject(error)), {
        concurrency: 2,
      }),
    ),
  );
  await assert.rejects(iterator.next(), (e) => e === error);
  gate.resolve(0);
  assert.equal((await iterator.closed).reason, error);
});

test("merge rotates ready inputs and never refills without demand", async () => {
  const counts = [0, 0];
  const source = (i) => ({
    *[Symbol.iterator]() {
      for (let j = 0; j < 5; j++) {
        counts[i]++;
        yield `${i}:${j}`;
      }
    },
  });
  const iterator = s.iterate(s.merge(source(0), source(1)));
  assert.equal((await iterator.next()).value, "0:0");
  await flush();
  assert.deepEqual(counts, [1, 1]);
  assert.equal((await iterator.next()).value, "1:0");
  assert.equal((await iterator.next()).value, "0:1");
  await iterator.return();
});

test("zip ends on the shortest input without waiting to report a hung sibling read", async () => {
  const hung = controlled();
  const iterator = s.iterate(s.zip([], hung.source));
  const reading = iterator.next();
  await flush();
  // Normal completion waits for ownership cleanup, unlike failure notification.
  assert.equal(hung.returns, 1);
  hung.reads[0].resolve({ done: true });
  assert.equal((await reading).done, true);
  assert.equal((await iterator.closed).kind, "completed");
});

test("chain opens inputs sequentially", async () => {
  const opened = [];
  const source = (i) =>
    s.defer(() => {
      opened.push(i);
      return [i];
    });
  const iterator = s.iterate(s.chain(source(1), source(2)));
  assert.equal((await iterator.next()).value, 1);
  assert.deepEqual(opened, [1]);
  assert.equal((await iterator.next()).value, 2);
  assert.deepEqual(opened, [1, 2]);
  await iterator.return();
});

test("zipLatest distinguishes undefined from unseen and partial mode is explicit", async () => {
  assert.deepEqual(await s.toArray(s.zipLatest([s.of(undefined), s.of("x")])), [
    [undefined, "x"],
  ]);
  assert.deepEqual(await s.toArray(s.zipLatest([s.empty(), s.of(1)])), []);
  assert.deepEqual(
    await s.toArray(s.zipLatest([s.empty(), s.of(1)], { partial: true })),
    [[undefined, 1]],
  );
  assert.deepEqual(await s.toArray(s.zipLatest([])), []);
});

test("zipLatest preserves each ready update after initialization", async () => {
  const a = controlled(),
    b = controlled();
  const iterator = s.iterate(s.zipLatest([a.source, b.source]));
  const first = iterator.next();
  await flush();
  a.reads[0].resolve({ value: 1 });
  b.reads[0].resolve({ value: "a" });
  assert.deepEqual((await first).value, [1, "a"]);
  const second = iterator.next();
  await flush();
  a.reads.at(-1).resolve({ value: 2 });
  b.reads.at(-1).resolve({ value: "b" });
  assert.deepEqual((await second).value, [2, "a"]);
  await flush();
  const count = b.reads.length;
  assert.deepEqual((await iterator.next()).value, [2, "b"]);
  assert.equal(b.reads.length, count);
  // Resolve every already reserved read before waiting for cleanup.
  const closing = iterator.return();
  for (const read of [...a.reads, ...b.reads]) read.resolve({ done: true });
  await closing;
});

test("concat and flatten compose children and mapper context", async () => {
  assert.deepEqual(
    await s.toArray(s.of([1, 2], [], [3]).pipe(s.concat({ concurrency: 2 }))),
    [1, 2, 3],
  );
  assert.deepEqual(
    (
      await s.toArray(s.of([1, 2], [3, 4]).pipe(s.flatten({ concurrency: 2 })))
    ).sort(),
    [1, 2, 3, 4],
  );
  assert.deepEqual(
    await s.toArray(
      s
        .of("a", "b")
        .pipe(
          s.concatMap((value, { index }) => [value, index], { concurrency: 2 }),
        ),
    ),
    ["a", 0, "b", 1],
  );
  assert.deepEqual(
    (
      await s.toArray(
        s.of(1, 2).pipe(s.flatMap((x) => [x, x * 2], { concurrency: 2 })),
      )
    ).sort(),
    [1, 2, 2, 4],
  );
});

test("concat retains completed successors behind a blocked head within the child limit", async () => {
  const head = controlled();
  let accepted = 0;
  const outer = {
    *[Symbol.iterator]() {
      accepted++;
      yield head.source;
      for (let i = 0; i < 100; i++) {
        accepted++;
        yield [];
      }
    },
  };
  const iterator = s.iterate(s.from(outer).pipe(s.concat({ concurrency: 3 })));
  const read = iterator.next();
  await flush();
  assert.equal(accepted, 3);
  head.reads[0].resolve({ value: 1 });
  assert.equal((await read).value, 1);
  await flush();
  assert.equal(accepted, 3);
  await iterator.return();
});

test("empty children and synchronous filtering yield control to the scheduler", async () => {
  const scheduler = new VirtualScheduler();
  let iterator;
  const source = {
    *[Symbol.iterator]() {
      for (;;) yield [];
    },
  };
  iterator = iterateWithScheduler(
    s.from(source).pipe(s.flatten()),
    {},
    scheduler,
  );
  const read = iterator.next();
  const rejected = assert.rejects(read, (e) => e === "done");
  await turn();
  iterator.cancel("done");
  await rejected;
  await iterator.closed;
  assert.ok(scheduler.yields > 0);
  const filtered = iterateWithScheduler(
    s.range(0, 1000).pipe(s.filter(() => false)),
    {},
    scheduler,
  );
  assert.equal((await filtered.next()).done, true);
  assert.ok(scheduler.yields > 1);
});
