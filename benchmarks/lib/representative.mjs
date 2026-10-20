import assert from "node:assert/strict";
import { memorySnapshot, memoryFieldStatus, readOsRss } from "./memory.mjs";
import { setTimeout as sleep } from "node:timers/promises";
import { createRequire } from "node:module";
import { from, map, rateLimit } from "../../dist/index.js";

const requireCompare = createRequire(
  new URL("../compare/package.json", import.meta.url),
);
export const pMapPath = requireCompare.resolve("p-map");
const { pMapIterable } = await import(pMapPath);

// Fixed profiles keep the default run bounded and make load changes explicit in reports.
export const scenarios = [
  {
    name: "delayed-ordered",
    items: 2048,
    concurrency: 64,
    capacity: 128,
    delayMs: 2,
    jitterMs: 6,
    sinkEvery: 0,
    sinkMs: 0,
    ordered: true,
    compare: true,
  },
  {
    name: "slow-sink",
    items: 2048,
    concurrency: 64,
    capacity: 128,
    delayMs: 2,
    jitterMs: 6,
    sinkEvery: 1,
    sinkMs: 1,
    ordered: true,
    compare: true,
  },
  {
    name: "large-unordered",
    items: 16384,
    concurrency: 1024,
    capacity: 2048,
    delayMs: 2,
    jitterMs: 6,
    sinkEvery: 128,
    sinkMs: 2,
    ordered: false,
  },
  {
    name: "retry",
    items: 2048,
    concurrency: 64,
    capacity: 128,
    delayMs: 2,
    jitterMs: 6,
    sinkEvery: 0,
    sinkMs: 0,
    ordered: true,
    retryEvery: 5,
    retryMs: 2,
    compare: true,
  },
  {
    name: "cancel-cooperative",
    items: 4096,
    concurrency: 64,
    capacity: 128,
    delayMs: 40,
    jitterMs: 10,
    sinkEvery: 0,
    sinkMs: 0,
    ordered: true,
    cancel: true,
    cooperative: true,
    cancelMs: 5,
    cleanupMs: 5,
  },
  {
    name: "cancel-uncooperative",
    items: 4096,
    concurrency: 64,
    capacity: 128,
    delayMs: 40,
    jitterMs: 10,
    sinkEvery: 0,
    sinkMs: 0,
    ordered: true,
    cancel: true,
    cooperative: false,
    cancelMs: 5,
    cleanupMs: 5,
  },
  {
    name: "large-rate-window",
    items: 30000,
    concurrency: 1,
    capacity: 1,
    delayMs: 0,
    jitterMs: 0,
    sinkEvery: 0,
    sinkMs: 0,
    ordered: true,
    rateCount: 10000,
    perMs: 100,
  },
];

// Delay is a pure function of seed/item/attempt, independent of completion order.
export function jitter(seed, id, attempt, span) {
  let value =
    (seed ^ Math.imul(id + 1, 0x9e3779b1) ^ Math.imul(attempt, 0x85ebca6b)) >>>
    0;
  value = Math.imul(value ^ (value >>> 16), 0x7feb352d) >>> 0;
  return ((value ^ (value >>> 15)) >>> 0) % (span + 1);
}

export function verifyRateWindows(requested, observed, count, perMs) {
  for (let index = count; index < observed.length; index++) {
    // Admission lies between the downstream request and source observation.
    assert.ok(
      observed[index] - requested[index - count] >= perMs,
      "rate window exceeded between request and source observation",
    );
  }
}

export async function measureScenario(profile, library, seed, sampleMs = 5) {
  assert.ok(
    library === "mogami" || (library === "p-map-iterable" && profile.compare),
  );
  const reason = new Error("representative benchmark cancellation");
  const controller = new AbortController();
  let active = 0,
    maxActive = 0,
    pulled = 0,
    received = 0,
    completed = 0;
  let maxAhead = 0,
    maxReady = 0,
    attempts = 0,
    abortedAttempts = 0;
  let sourceClosed = false,
    cancelAt,
    settledAt,
    firstItemMs = null;
  let cancelTimer;
  const seen = new Uint8Array(profile.items);
  const starts = new Float64Array(profile.rateCount ? profile.items + 1 : 0);
  const requestedStarts = new Float64Array(starts.length);
  let startCount = 0,
    rateHead = 0,
    maxRateWindow = 0;
  function recordBounds() {
    maxAhead = Math.max(maxAhead, pulled - received);
    maxReady = Math.max(maxReady, completed - received);
    assert.ok(active <= profile.concurrency, "mapper concurrency exceeded");
    assert.ok(
      pulled - received <= profile.capacity + 1,
      "read-ahead exceeded capacity plus sink item",
    );
  }
  async function* source() {
    try {
      for (let id = 0; id < profile.items; id++) {
        if (profile.rateCount) recordStart();
        pulled++;
        recordBounds();
        yield id;
      }
      // rateLimit counts the terminal next() as well as value-producing reads.
      if (profile.rateCount) recordStart();
    } finally {
      if (profile.cleanupMs) await sleep(profile.cleanupMs);
      sourceClosed = true;
    }
  }
  function recordStart() {
    const now = performance.now();
    starts[startCount++] = now;
    while (rateHead < startCount && starts[rateHead] <= now - profile.perMs)
      rateHead++;
    maxRateWindow = Math.max(maxRateWindow, startCount - rateHead);
  }
  async function mapper(id, { signal, attempt = 1 } = {}) {
    active++;
    attempts++;
    maxActive = Math.max(maxActive, active);
    recordBounds();
    if (
      profile.cancel &&
      active === profile.concurrency &&
      cancelTimer === undefined
    ) {
      cancelTimer = setTimeout(() => {
        cancelAt = performance.now();
        controller.abort(reason);
      }, profile.cancelMs);
    }
    try {
      const delay =
        profile.delayMs + jitter(seed, id, attempt, profile.jitterMs);
      if (delay) {
        // Cooperative waits clear their timers when the stage signal aborts.
        try {
          await sleep(
            delay,
            undefined,
            profile.cooperative ? { signal } : undefined,
          );
        } catch (error) {
          if (!signal?.aborted) throw error;
          abortedAttempts++;
          throw signal.reason;
        }
      }
      if (profile.retryEvery && id % profile.retryEvery === 0 && attempt === 1)
        throw new Error("deterministic transient failure");
      completed++;
      recordBounds();
      return { id, value: id * 2 };
    } finally {
      active--;
    }
  }
  const osBefore = readOsRss();
  const baseline = memorySnapshot();
  const peak = { ...baseline };
  let memorySamples = 0;
  function sampleMemory() {
    const memory = memorySnapshot();
    for (const key of Object.keys(peak))
      peak[key] =
        peak[key] === null || memory[key] === null
          ? null
          : Math.max(peak[key], memory[key]);
    memorySamples++;
  }
  const memoryTimer = setInterval(sampleMemory, sampleMs);
  const started = performance.now();
  let iterator;
  try {
    if (library === "mogami") {
      let output = from(source());
      if (profile.rateCount)
        output = output.pipe(
          rateLimit({ count: profile.rateCount, perMs: profile.perMs }),
        );
      else
        output = output.pipe(
          map(mapper, {
            concurrency: profile.concurrency,
            capacity: profile.capacity,
            ordered: profile.ordered,
            ...(profile.retryEvery
              ? { retry: { retries: 1, delayMs: profile.retryMs } }
              : {}),
          }),
        );
      iterator = output.iterate({ signal: controller.signal });
    } else {
      const invoke = async (id) => {
        try {
          return await mapper(id);
        } catch (error) {
          if (!profile.retryEvery || id % profile.retryEvery !== 0) throw error;
          await sleep(profile.retryMs);
          return mapper(id, { attempt: 2 });
        }
      };
      iterator = pMapIterable(source(), invoke, {
        concurrency: profile.concurrency,
        backpressure: profile.capacity,
      })[Symbol.asyncIterator]();
    }
    let cancelled = false;
    try {
      for (;;) {
        if (profile.rateCount) requestedStarts[startCount] = performance.now();
        const result = await iterator.next();
        if (result.done) break;
        const { id, value } = profile.rateCount
          ? { id: result.value, value: result.value * 2 }
          : result.value;
        assert.equal(value, id * 2);
        assert.ok(
          id >= 0 && id < profile.items && !seen[id],
          "invalid or duplicate output",
        );
        if (profile.ordered) assert.equal(id, received);
        seen[id] = 1;
        received++;
        firstItemMs ??= performance.now() - started;
        recordBounds();
        if (profile.sinkEvery && received % profile.sinkEvery === 0)
          await sleep(profile.sinkMs);
      }
    } catch (error) {
      if (!profile.cancel || error !== reason) throw error;
      cancelled = true;
    }
    settledAt = performance.now();
    const closeReport = iterator.closed ? await iterator.closed : undefined;
    const closedAt = performance.now();
    assert.equal(active, 0, "mapper work remains after cleanup");
    assert.equal(sourceClosed, true, "source was not closed");
    if (profile.cancel) {
      assert.equal(cancelled, true);
      assert.equal(closeReport.kind, "cancelled");
      assert.equal(closeReport.reason, reason);
      assert.equal(received, 0);
      assert.equal(maxActive, profile.concurrency);
      assert.equal(
        abortedAttempts,
        profile.cooperative ? profile.concurrency : 0,
      );
    } else {
      assert.equal(received, profile.items);
      const expectedAttempts = profile.rateCount
        ? 0
        : profile.items +
          (profile.retryEvery
            ? Math.ceil(profile.items / profile.retryEvery)
            : 0);
      assert.equal(attempts, expectedAttempts);
      if (profile.rateCount) {
        assert.equal(startCount, profile.items + 1);
        verifyRateWindows(
          requestedStarts,
          starts,
          profile.rateCount,
          profile.perMs,
        );
      } else assert.equal(maxActive, profile.concurrency);
    }
    sampleMemory();
    return {
      elapsedMs: closedAt - started,
      itemsPerSecond: received / ((closedAt - started) / 1000),
      firstItemMs,
      received,
      pulled,
      attempts,
      abortedAttempts,
      maxActive,
      maxAhead,
      maxReady,
      maxRateWindow: profile.rateCount ? maxRateWindow : null,
      rateStarts: profile.rateCount ? startCount : null,
      cancelToSettlementMs:
        cancelAt === undefined ? null : settledAt - cancelAt,
      cancelToClosedMs: cancelAt === undefined ? null : closedAt - cancelAt,
      memory: {
        runtimeApi: {
          method: "process.memoryUsage",
          runtime: process.versions.bun ? "bun" : process.release.name,
          version: process.versions.bun ?? process.version,
          sampleMs,
          samples: memorySamples,
          fieldStatus: memoryFieldStatus(
            peak,
            process.versions.bun ? "bun" : process.release.name,
          ),
          baseline,
          peak,
          delta: Object.fromEntries(
            Object.keys(peak).map((key) => [
              key,
              peak[key] === null || baseline[key] === null
                ? null
                : peak[key] - baseline[key],
            ]),
          ),
        },
        os: {
          method: "ps -o rss= -p PID (KiB converted to bytes)",
          platform: process.platform,
          scope: "before workload and after cleanup; not a peak measurement",
          before: osBefore,
          after: readOsRss(),
        },
      },
    };
  } finally {
    clearInterval(memoryTimer);
    clearTimeout(cancelTimer);
    if (active > 0) controller.abort(reason);
    await iterator?.return?.();
  }
}
