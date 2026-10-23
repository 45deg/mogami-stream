import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { adaptersFor } from "../benchmarks/compare/adapters.mjs";
import {
  parseOptions,
  shuffle,
  summarize,
} from "../benchmarks/lib/harness.mjs";
import { prepareWorkload, workloads } from "../benchmarks/lib/workloads.mjs";
import { broadcast, route } from "../dist/index.js";

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

test("broadcast workloads use the shared runner and verify bounded branch delivery", () => {
  const directory = mkdtempSync(join(tmpdir(), "mogami-broadcast-bench-"));
  try {
    const file = join(directory, "results.json");
    const child = spawnSync(
      process.execPath,
      [
        "benchmarks/run.mjs",
        "--case=broadcast-balanced,broadcast-lagging",
        "--items=131",
        "--warmup=1",
        "--samples=2",
        `--json=${file}`,
      ],
      {
        cwd: new URL("../", import.meta.url),
        encoding: "utf8",
        timeout: 10000,
      },
    );
    assert.ifError(child.error);
    assert.equal(child.status, 0, child.stderr);
    const report = JSON.parse(readFileSync(file, "utf8"));
    assert.deepEqual(
      report.results.map(({ case: name, library }) => [name, library]),
      [
        ["broadcast-balanced", "mogami"],
        ["broadcast-lagging", "mogami"],
      ],
    );
    for (const result of report.results) {
      assert.equal(result.samples.length, 2);
      assert.ok(result.elapsedMs.median > 0);
      assert.ok(result.firstItemMs.median >= 0);
      assert.ok(result.maxAhead <= 16);
      assert.equal(result.maxReady, null);
      for (const sample of result.samples) {
        assert.equal(sample.pulled, 131);
        assert.equal(sample.consumed, 131);
        assert.equal(sample.sourceClosed, true);
      }
    }
    assert.equal(report.results[1].maxAhead, 16);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("broadcast verification rejects branch reorderings with matching counts and checksums", async () => {
  const workload = workloads.find((w) => w.name === "broadcast-balanced");
  const context = prepareWorkload(workload, parseOptions(["--items=3"]))();
  const group = broadcast(
    (function* () {
      for (const value of context.source)
        yield value === 0 ? 1 : value === 1 ? 0 : value;
    })(),
    { branches: workload.branches, capacity: context.capacity },
  );
  context.begin();
  try {
    await assert.rejects(
      context.consumeBroadcast(group),
      /branch output order/,
    );
  } finally {
    await group.close();
  }
});

test("route workloads use the shared runner and verify destination delivery", () => {
  const names = [
    "route-numeric-pair",
    "route-keyed-eight",
    "route-guards-eight",
    "route-async-source",
    "route-async-select",
    "route-ignored",
    "route-detached-default",
    "route-lagging",
  ];
  const directory = mkdtempSync(join(tmpdir(), "mogami-route-bench-"));
  try {
    const file = join(directory, "results.json");
    const child = spawnSync(
      process.execPath,
      [
        "benchmarks/run.mjs",
        `--case=${names.join(",")}`,
        "--items=131",
        "--warmup=1",
        "--samples=2",
        `--json=${file}`,
      ],
      {
        cwd: new URL("../", import.meta.url),
        encoding: "utf8",
        timeout: 15000,
      },
    );
    assert.ifError(child.error);
    assert.equal(child.status, 0, child.stderr);
    const report = JSON.parse(readFileSync(file, "utf8"));
    assert.deepEqual(
      report.results.map((result) => result.case),
      names,
    );
    for (const result of report.results) {
      assert.equal(result.library, "mogami");
      assert.equal(result.samples.length, 2);
      assert.ok(result.elapsedMs.median > 0);
      assert.ok(result.cpuMs.median >= 0);
      assert.ok(result.firstItemMs.median >= 0);
      assert.equal(result.maxReady, null);
      for (const sample of result.samples) {
        assert.equal(sample.pulled, 131);
        assert.equal(sample.sourceClosed, true);
        assert.equal(sample.delivered.length, sample.checksums.length);
        assert.equal(
          sample.consumed,
          sample.delivered.reduce((sum, count) => sum + count, 0),
        );
        assert.equal(
          sample.consumed,
          result.case === "route-ignored" ? 67 : 131,
        );
        if (result.case === "route-detached-default")
          assert.deepEqual(sample.delivered, [0, 0, 0, 131]);
      }
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("route verification rejects branch reorderings with matching counts and checksums", async () => {
  const workload = workloads.find((w) => w.name === "route-numeric-pair");
  const context = prepareWorkload(workload, parseOptions(["--items=4"]))();
  const group = route(
    (function* () {
      for (const value of context.source)
        yield value === 0 ? 2 : value === 2 ? 0 : value;
    })(),
    context.routeOptions,
  );
  context.begin();
  try {
    await assert.rejects(
      context.consumeRoute(group),
      /route branch output order/,
    );
  } finally {
    await group.close();
  }
});

test("GC is explicit, validated, and independent of runtime exposure", () => {
  for (const gcAvailable of [false, true]) {
    assert.equal(parseOptions([], { gcAvailable }).gc, "none");
    assert.equal(parseOptions(["--gc=none"], { gcAvailable }).gc, "none");
  }
  assert.throws(
    () => parseOptions(["--gc=between"], { gcAvailable: false }),
    /expose-gc/,
  );
  assert.equal(
    parseOptions(["--gc=between"], { gcAvailable: true }).gc,
    "between",
  );
  assert.throws(() => parseOptions(["--gc=invalid"]), /gc mode/);
});

test("benchmark workers honor explicit GC and count warmups separately from samples", () => {
  const directory = mkdtempSync(join(tmpdir(), "mogami-gc-"));
  try {
    for (const exposed of [false, true]) {
      for (const mode of ["none", "between"]) {
        const file = join(directory, `${exposed}-${mode}.json`);
        const child = spawnSync(
          process.execPath,
          [
            ...(exposed ? ["--expose-gc"] : []),
            "benchmarks/run.mjs",
            "--case=async-map-concurrent",
            "--items=5",
            "--warmup=1",
            "--samples=2",
            `--gc=${mode}`,
            `--json=${file}`,
          ],
          {
            cwd: new URL("../", import.meta.url),
            encoding: "utf8",
            timeout: 10000,
          },
        );
        assert.ifError(child.error);
        if (mode === "between" && !exposed) {
          assert.notEqual(child.status, 0);
          assert.match(child.stderr, /expose-gc/);
        } else {
          assert.equal(child.status, 0, child.stderr);
          const report = JSON.parse(readFileSync(file, "utf8"));
          assert.equal(report.runtime.gc, mode);
          assert.equal(report.configuration.gc, mode);
          assert.equal(report.runtime.forcedGcBetweenRuns, mode === "between");
          for (const row of report.results) {
            assert.equal(row.manualGcCalls, mode === "between" ? 3 : 0);
            assert.equal(row.samples.length, 2);
          }
        }
      }
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
