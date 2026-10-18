import assert from "node:assert/strict";
import { test } from "node:test";
import { adaptersFor } from "../benchmarks/compare/adapters.mjs";
import {
  parseOptions,
  shuffle,
  summarize,
} from "../benchmarks/lib/harness.mjs";
import {
  poolArray,
  prepareWorkload,
  workloads,
} from "../benchmarks/lib/workloads.mjs";

test("benchmark options reject invalid and ambiguous configurations", () => {
  for (const args of [
    ["--samples=0"],
    ["--warmup=-1"],
    ["--items=1.5"],
    ["--concurrency=8", "--capacity=4"],
    ["--io-items=1"],
    ["--case=missing"],
    ["--items=1", "--items=2"],
    ["--unknown=1"],
    ["--toString=1"],
  ])
    assert.throws(() => parseOptions(args));
  assert.equal(parseOptions(["--warmup=0"]).warmup, 0);
});

test("benchmark summaries use the middle pair for even sample counts", () => {
  assert.deepEqual(summarize([8, 2, 4, 6]), { median: 5, min: 2, max: 8 });
  assert.equal(summarize([9, 1, 3]).median, 3);
  const values = Array.from({ length: 20 }, (_, i) => i);
  assert.deepEqual(shuffle(values, 42), shuffle(values, 42));
  assert.notDeepEqual(shuffle(values, 42), shuffle(values, 43));
  assert.deepEqual(
    [...shuffle(values, 42)].sort((a, b) => a - b),
    values,
  );
});

test("benchmark oracle rejects reorderings that preserve count and checksum", async () => {
  const workload = workloads.find((w) => w.name === "early-stop");
  const context = prepareWorkload(
    workload,
    parseOptions(["--items=3", "--stop-after=3"]),
  )();
  context.begin();
  await assert.rejects(
    context.consume([
      { id: 1, value: 2 },
      { id: 0, value: 0 },
      { id: 2, value: 4 },
    ]),
    /output order/,
  );
});

test("benchmark prefix verification detects extra pulls and missing source cleanup", async () => {
  const workload = workloads.find((w) => w.name === "early-stop");
  const make = prepareWorkload(
    workload,
    parseOptions(["--items=10", "--stop-after=2"]),
  );
  const context = make();
  context.begin();
  await context.consume([
    { id: 0, value: 0 },
    { id: 1, value: 2 },
  ]);
  assert.throws(() => context.verify(), /source was not closed/);
  context.metrics.sourceClosed = true;
  context.metrics.pulled = 3;
  assert.throws(() => context.verify(), /read beyond/);
});

test("native pool fills available slots, preserves order, and closes its source", async () => {
  let closed = false;
  let started = 0;
  let release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  function* source() {
    try {
      yield* [0, 1, 2, 3];
    } finally {
      closed = true;
    }
  }
  const work = poolArray(
    source(),
    async (id) => {
      started++;
      if (id < 2) await gate;
      return id * 2;
    },
    2,
  );
  await Promise.resolve();
  assert.equal(started, 2);
  assert.equal(closed, false);
  release();
  assert.deepEqual(await work, [0, 2, 4, 6]);
  assert.equal(closed, true);
});

test("early exit and cancellation await the workload's asynchronous resource cleanup", async () => {
  const config = parseOptions(["--items=13", "--io-items=8", "--stop-after=3"]);
  for (const name of ["early-stop", "cancel-inflight"]) {
    const workload = workloads.find((w) => w.name === name);
    for (const adapter of await adaptersFor("local")) {
      const context = prepareWorkload(workload, config)();
      context.begin();
      await adapter.run(workload, context);
      context.verify();
      assert.equal(context.metrics.sourceClosed, true);
      assert.equal(context.metrics.active, 0);
      assert.equal(typeof context.metrics.stopToClosedMs, "number");
      if (name === "cancel-inflight")
        assert.equal(context.metrics.completed, 0);
    }
  }
});
