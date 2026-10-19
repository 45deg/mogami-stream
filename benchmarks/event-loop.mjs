import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import { cpus } from "node:os";
import { summarize } from "./lib/harness.mjs";
import { range, map, tap } from "../dist/index.js";
import { create, open } from "../dist/core/stream.js";
import {
  yieldAfter,
  yieldEvery,
  yieldToHost,
} from "../docs/examples/event-loop.mjs";

// A benchmark-only prototype: no public export or change to default scheduling.
function prototypeYieldEvery(count) {
  return (source) =>
    create((scope) => {
      const upstream = open(source, scope);
      let seen = 0;
      return {
        async next() {
          const result = await upstream.next();
          if (!result.done && ++seen % count === 0) {
            await scope.scheduler.sleep(0, scope.signal);
            scope.check();
          }
          return result;
        },
      };
    });
}

const items = Number(process.env.MOGAMI_YIELD_ITEMS ?? 10_000);
const samples = Number(process.env.MOGAMI_YIELD_SAMPLES ?? 3);
assert.ok(Number.isSafeInteger(items) && items > 0);
assert.ok(Number.isSafeInteger(samples) && samples > 0);
const policies = [
  ["default", (source) => source],
  ["tap-control", tap(() => {})],
  ["per-item", tap((_value, { signal }) => yieldToHost(signal))],
  ["count-256", yieldEvery(256)],
  ["count-1024", yieldEvery(1_024)],
  ["time-4ms", yieldAfter(4)],
  ["prototype-count-1024", prototypeYieldEvery(1_024)],
];
function work(value) {
  let result = value;
  for (let step = 0; step < 128; step++)
    result = (Math.imul(result, 1_664_525) + 1_013_904_223) | 0;
  return result;
}

async function measure(policy, cancel) {
  const controller = new AbortController();
  const reason = new Error("benchmark cancellation");
  let seen = 0;
  let firedAt;
  let seenAtTimer;
  const started = performance.now();
  const timer = setTimeout(() => {
    firedAt = performance.now();
    seenAtTimer = seen;
    if (cancel) controller.abort(reason);
  }, 0);
  let completedAt;
  let cancelled = false;
  try {
    await range(0, items)
      .pipe(map(work), policy)
      .forEach(
        (value) => {
          assert.equal(value, work(seen++));
        },
        { signal: controller.signal },
      );
  } catch (error) {
    if (!cancel || error !== reason) throw error;
    cancelled = true;
  } finally {
    completedAt = performance.now();
    // Let the observation timer run even if the default stream finished first.
    if (firedAt === undefined)
      await new Promise((resolve) => setTimeout(resolve, 0));
    clearTimeout(timer);
  }
  assert.ok(firedAt !== undefined);
  if (!cancel || !cancelled) assert.equal(seen, items);
  return {
    elapsedMs: completedAt - started,
    itemsPerSecond: seen / ((completedAt - started) / 1_000),
    timerDelayMs: firedAt - started,
    itemsAtTimer: seenAtTimer,
    completedBeforeTimer: completedAt < firedAt,
    // Timer scheduling delay and abort-to-settlement are distinct measurements.
    cancellationFromScheduledMs: cancelled ? completedAt - started : null,
    abortToSettlementMs: cancelled ? completedAt - firedAt : null,
    processed: seen,
  };
}

const report = {
  runtime: process.versions.bun
    ? `Bun ${process.versions.bun}`
    : `Node.js ${process.version}`,
  platform: `${process.platform} ${process.arch}`,
  cpu: cpus()[0]?.model,
  settings: { items, samples, timerMs: 0, workSteps: 128 },
  policies: {},
};
// Warm every shape outside the measured samples; cap the per-item timer cost.
for (const [, policy] of policies)
  await range(0, 256)
    .pipe(map(work), policy)
    .forEach(() => {});
for (const [name] of policies)
  report.policies[name] = { throughput: [], cancellation: [] };
for (let round = 0; round < samples; round++) {
  // Rotate case order so every policy does not always occupy the same position.
  const rotated = [...policies.slice(round), ...policies.slice(0, round)];
  for (const [name, policy] of rotated) {
    report.policies[name].throughput.push(await measure(policy, false));
    report.policies[name].cancellation.push(await measure(policy, true));
  }
}
const median = (values) =>
  values.includes(null) ? null : summarize(values).median;
for (const [name, runs] of Object.entries(report.policies)) {
  console.log(
    name,
    JSON.stringify({
      elapsedMs: median(runs.throughput.map((run) => run.elapsedMs)),
      itemsPerSecond: median(runs.throughput.map((run) => run.itemsPerSecond)),
      timerDelayMs: median(runs.throughput.map((run) => run.timerDelayMs)),
      cancellationFromScheduledMs: median(
        runs.cancellation.map((run) => run.cancellationFromScheduledMs),
      ),
      abortToSettlementMs: median(
        runs.cancellation.map((run) => run.abortToSettlementMs),
      ),
      itemsAtCancelTimer: median(
        runs.cancellation.map((run) => run.itemsAtTimer),
      ),
    }),
  );
}
if (process.argv[2])
  await writeFile(process.argv[2], JSON.stringify(report, null, 2) + "\n");
