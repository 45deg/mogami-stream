import assert from "node:assert/strict";
import { test } from "node:test";
import { adaptersFor } from "../benchmarks/compare/adapters.mjs";
import {
  parseOptions,
  shuffle,
  summarize,
} from "../benchmarks/lib/harness.mjs";
import { prepareWorkload, workloads } from "../benchmarks/lib/workloads.mjs";

test("benchmark options reject invalid and ambiguous configurations", () => {
  for (const args of [
    ["--samples=0"],
    ["--warmup=-1"],
    ["--items=1.5"],
    ["--concurrency=8", "--capacity=4"],
    ["--io-items=1"],
    ["--latency-ms=1"],
    ["--case=early-stop"],
    ["--case=missing"],
    ["--case=async-map-serial,missing"],
    ["--case=async-map-serial,async-map-serial"],
    ["--items=1", "--items=2"],
    ["--unknown=1"],
    ["--toString=1"],
  ])
    assert.throws(() => parseOptions(args));
  assert.equal(parseOptions(["--warmup=0"]).warmup, 0);
  assert.equal(parseOptions(["--case=all"]).selectedCase, undefined);
  assert.equal(
    parseOptions(["--case=async-map-serial,async-pipeline"]).selectedCase,
    "async-map-serial,async-pipeline",
  );
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
  const workload = workloads.find((w) => w.name === "async-map-serial");
  const context = prepareWorkload(workload, parseOptions(["--items=3"]))();
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

test("benchmark verification detects missing source cleanup and incomplete input", async () => {
  const workload = workloads.find((w) => w.name === "async-map-serial");
  const context = prepareWorkload(workload, parseOptions(["--items=2"]))();
  context.begin();
  await context.consume([
    { id: 0, value: 0 },
    { id: 1, value: 2 },
  ]);
  assert.throws(() => context.verify(), /source was not closed/);
  context.metrics.sourceClosed = true;
  assert.throws(() => context.verify(), { name: "AssertionError" });
});

for (const [name, libraries] of [
  ["async-map-serial", ["mogami", "ix", "iter-tools", "p-map-iterable"]],
  ["async-map-concurrent", ["mogami", "p-map-iterable"]],
  ["async-pipeline", ["mogami", "ix", "iter-tools"]],
]) {
  test(`${name} uses async inputs and verifies ordered, bounded, delay-free consumption`, async () => {
    const config = parseOptions(["--items=103"]);
    const workload = workloads.find((w) => w.name === name);
    const adapters = (await adaptersFor()).filter((a) => a.supports(workload));
    assert.deepEqual(
      adapters.map((a) => a.name),
      libraries,
    );
    for (const adapter of adapters) {
      const context = prepareWorkload(workload, config)();
      assert.equal(typeof context.source[Symbol.asyncIterator], "function");
      assert.equal(context.source[Symbol.iterator], undefined);
      if (workload.kind === "pipeline") {
        assert.equal(context.pipelineSteps.length, 13);
        for (const [index, step] of context.pipelineSteps.entries()) {
          const result = (step.map ?? step.filter)(
            index === 0 ? 0 : { id: 0, value: 1 },
          );
          assert.ok(result instanceof Promise);
          await result;
        }
      }
      context.begin();
      await adapter.run(workload, context);
      context.verify();
      assert.equal(
        context.metrics.consumed,
        workload.kind === "pipeline" ? 97 : 103,
      );
      if (workload.kind === "async-map") {
        assert.ok(
          context.metrics.maxActive > 0 &&
            context.metrics.maxActive <= context.concurrency,
        );
        assert.ok(context.metrics.maxAhead <= context.capacity + 1);
        const ahead = context.metrics.maxAhead;
        context.metrics.maxAhead = context.capacity + 2;
        assert.throws(() => context.verify(), /read too far ahead/);
        context.metrics.maxAhead = ahead;
        context.metrics.maxActive = context.concurrency + 1;
        assert.throws(() => context.verify(), /configured concurrency/);
      }
    }
  });
}
