import assert from "node:assert/strict";
import { test } from "node:test";
import {
  memorySnapshot,
  memoryFieldStatus,
  readOsRss,
} from "../benchmarks/lib/memory.mjs";
import { parseOptions } from "../benchmarks/representative.mjs";
import {
  jitter,
  measureScenario,
  scenarios,
} from "../benchmarks/lib/representative.mjs";

test("representative options reject configurations that hide missing samples", () => {
  for (const args of [
    ["--samples=0"],
    ["--warmup=-1"],
    ["--seed=NaN"],
    ["--sampleMs=0"],
    ["--case=missing"],
    ["--samples=1", "--samples=2"],
    ["--unknown=1"],
  ])
    assert.throws(() => parseOptions(args));
  assert.equal(parseOptions(["--seed=0", "--warmup=0"]).seed, 0);
  const first = Array.from({ length: 100 }, (_, id) => jitter(42, id, 1, 6));
  assert.deepEqual(
    first,
    Array.from({ length: 100 }, (_, id) => jitter(42, id, 1, 6)),
  );
  assert.notDeepEqual(
    first,
    Array.from({ length: 100 }, (_, id) => jitter(43, id, 1, 6)),
  );
  assert.ok(first.every((delay) => delay >= 0 && delay <= 6));
});

test("delayed and retry profiles verify equivalent ordered adapters and actual overlap", async () => {
  for (const name of ["delayed-ordered", "retry"])
    for (const library of ["mogami", "p-map-iterable"]) {
      const profile = {
        ...scenarios.find((profile) => profile.name === name),
        items: 32,
        concurrency: 4,
        capacity: 8,
      };
      const result = await measureScenario(profile, library, 42);
      assert.equal(result.maxActive, 4);
      assert.equal(result.received, 32);
      assert.equal(result.attempts, name === "retry" ? 39 : 32);
      assert.ok(result.maxAhead <= 9);
      assert.ok(result.memory.runtimeApi.samples > 1);
      assert.equal(result.memory.runtimeApi.method, "process.memoryUsage");
      assert.equal(
        result.memory.runtimeApi.runtime,
        process.versions.bun ? "bun" : "node",
      );
      for (const observation of [
        result.memory.os.before,
        result.memory.os.after,
      ]) {
        assert.ok(observation.observedAt);
        if (observation.status === "measured")
          assert.ok(observation.rssBytes > 0);
        else assert.equal(observation.rssBytes, null);
      }
    }
});

test("cancellation distinguishes immediate next settlement and drained cleanup", async () => {
  for (const cooperative of [true, false]) {
    const profile = {
      ...scenarios.find((profile) => profile.name === "cancel-cooperative"),
      items: 32,
      concurrency: 4,
      capacity: 8,
      cooperative,
    };
    const result = await measureScenario(profile, "mogami", 42);
    assert.equal(result.received, 0);
    assert.equal(result.abortedAttempts, cooperative ? 4 : 0);
    assert.ok(result.cancelToClosedMs >= result.cancelToSettlementMs);
    if (!cooperative) assert.ok(result.cancelToClosedMs >= 20);
  }
});

test("rate profile checks sliding windows and the terminal upstream read", async () => {
  const profile = {
    ...scenarios.find((profile) => profile.rateCount),
    items: 41,
    rateCount: 10,
    perMs: 10,
  };
  const result = await measureScenario(profile, "mogami", 42);
  assert.equal(result.received, 41);
  assert.equal(result.rateStarts, 42);
  assert.equal(result.maxActive, 0);
  assert.ok(result.maxRateWindow >= 10);
  assert.ok(result.elapsedMs >= 30);
});

test("memory reports preserve reported zero and mark unavailable or unverified accounting", () => {
  const snapshot = memorySnapshot({ rss: 1024, heapUsed: 0, external: NaN });
  assert.equal(snapshot.heapUsed, 0);
  assert.equal(snapshot.external, null);
  assert.equal(snapshot.arrayBuffers, null);
  assert.equal(memoryFieldStatus(snapshot, "node").heapUsed, "reported");
  assert.equal(memoryFieldStatus(snapshot, "bun").arrayBuffers, "unavailable");
  const reported = memorySnapshot({ arrayBuffers: 0, external: 100 });
  assert.equal(
    memoryFieldStatus(reported, "bun").arrayBuffers,
    "unverified-accounting",
  );
  assert.equal(
    memoryFieldStatus(reported, "bun").external,
    "unverified-accounting",
  );
  assert.equal(reported.arrayBuffers, 0);
  const os = readOsRss();
  if (os.status === "measured") assert.ok(os.rssBytes > 0);
  else {
    assert.equal(os.rssBytes, null);
    assert.ok(os.reason);
  }
});
