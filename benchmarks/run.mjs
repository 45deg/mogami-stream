import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const runtime =
  typeof globalThis.Bun === "undefined"
    ? { name: process.release.name, version: process.version }
    : { name: "bun", version: globalThis.Bun.version };

const args = new Map(
  process.argv.slice(2).map((argument) => {
    const separator = argument.indexOf("=");
    if (!argument.startsWith("--") || separator < 0)
      throw new Error(`Expected --name=value: ${argument}`);
    return [argument.slice(2, separator), argument.slice(separator + 1)];
  }),
);
const supported = new Set([
  "items",
  "samples",
  "warmup",
  "case",
  "baseline",
  "json",
]);
for (const name of args.keys())
  if (!supported.has(name)) throw new Error(`Unknown option: ${name}`);
function integerOption(name, fallback, minimum = 1) {
  const value = Number(args.get(name) ?? fallback);
  if (!Number.isSafeInteger(value) || value < minimum)
    throw new RangeError(`${name} must be a safe integer >= ${minimum}.`);
  return value;
}
const items = integerOption("items", 5_000);
const samples = integerOption("samples", 5);
const warmup = integerOption("warmup", 2, 0);
const selectedCase = args.get("case");
const sum = (items * (items - 1)) / 2;
const asynchronousItems = Math.min(items, 128);
const cpuItems = Math.min(items, 256);
if (!Number.isSafeInteger(sum + items))
  throw new RangeError("items is too large for an exact checksum.");

async function consume(
  source,
  expectedCount = items,
  expectedSum = sum,
  batches = false,
) {
  let count = 0;
  let checksum = 0;
  for await (const value of source) {
    if (batches) {
      for (const item of value) {
        count++;
        checksum += item;
      }
    } else {
      count++;
      checksum += value;
    }
  }
  assert.equal(count, expectedCount);
  assert.equal(checksum, expectedSum);
}

const cases = [
  { name: "range", run: (api) => consume(api.range(0, items)) },
  {
    name: "from-array",
    run: (api) => consume(api.from(Array.from({ length: items }, (_, i) => i))),
  },
  {
    name: "map-sequential",
    run: (api) =>
      consume(
        api.range(0, items).pipe(api.map((value) => value + 1)),
        items,
        sum + items,
      ),
  },
  {
    name: "map-concurrent",
    run: (api) =>
      consume(
        api.range(0, items).pipe(
          api.map(async (value) => value + 1, {
            concurrency: 8,
            capacity: 32,
            ordered: false,
          }),
        ),
        items,
        sum + items,
      ),
  },
  {
    name: "map-timer-1ms",
    items: asynchronousItems,
    run: (api) =>
      consume(
        api
          .range(0, asynchronousItems)
          .pipe(
            api.map(
              (value) =>
                new Promise((resolve) => setTimeout(() => resolve(value), 1)),
              { concurrency: 8, capacity: 8, ordered: false },
            ),
          ),
        asynchronousItems,
        (asynchronousItems * (asynchronousItems - 1)) / 2,
      ),
  },
  {
    name: "map-cpu-250us",
    items: cpuItems,
    run: (api) =>
      consume(
        api.range(0, cpuItems).pipe(
          api.map((value) => {
            const until = performance.now() + 0.25;
            while (performance.now() < until) {
              /* Deliberate synchronous callback work. */
            }
            return value;
          }),
        ),
        cpuItems,
        (cpuItems * (cpuItems - 1)) / 2,
      ),
  },
  {
    name: "pipeline",
    run: (api) =>
      consume(
        api.range(0, items * 2).pipe(
          api.filter((value) => value % 2 === 0),
          api.map((value) => value / 2),
          api.skip(0),
          api.take(items),
        ),
      ),
  },
  {
    name: "merge-64",
    run: (api) =>
      consume(
        api.merge(
          ...Array.from({ length: 64 }, (_, i) => api.range(i, items, 64)),
        ),
      ),
  },
  {
    name: "merge-512",
    run: (api) =>
      consume(
        api.merge(
          ...Array.from({ length: 512 }, (_, i) => api.range(i, items, 512)),
        ),
      ),
  },
  {
    name: "merge-sparse-512",
    run: (api) =>
      consume(
        api.merge(
          api.range(0, items),
          ...Array.from({ length: 511 }, () => api.empty()),
        ),
      ),
  },
  {
    name: "flatten-singletons",
    run: (api) =>
      consume(
        api
          .range(0, items)
          .pipe(api.flatMap((value) => [value], { concurrency: 8 })),
      ),
  },
  {
    name: "buffer-100",
    run: (api) =>
      consume(
        api.range(0, items).pipe(api.bufferTime(60_000, { maxItems: 100 })),
        items,
        sum,
        true,
      ),
  },
];
if (selectedCase && !cases.some((entry) => entry.name === selectedCase))
  throw new Error(`Unknown case: ${selectedCase}`);
const selected = cases.filter(
  (entry) => !selectedCase || entry.name === selectedCase,
);
const implementations = [
  { name: "current", api: await import("../dist/index.js") },
];
if (args.has("baseline"))
  implementations.unshift({
    name: "baseline",
    api: await import(pathToFileURL(resolve(args.get("baseline"))).href),
  });
const median = (values) => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
};
const results = [];

console.log(
  `${runtime.name} ${runtime.version}; ${items} items; ${warmup} warmups; ${samples} samples; public API with default scheduler`,
);
for (const entry of selected) {
  for (let round = 0; round < warmup; round++)
    for (const implementation of implementations)
      await entry.run(implementation.api);
  const observations = new Map(
    implementations.map((implementation) => [implementation.name, []]),
  );
  for (let round = 0; round < samples; round++) {
    // Alternate order to reduce systematic warmup and thermal bias.
    const order =
      round % 2 === 0 ? implementations : [...implementations].reverse();
    for (const implementation of order) {
      const cpuStart = process.cpuUsage();
      const start = performance.now();
      await entry.run(implementation.api);
      const elapsedMs = performance.now() - start;
      const cpu = process.cpuUsage(cpuStart);
      observations
        .get(implementation.name)
        .push({ elapsedMs, cpuMs: (cpu.user + cpu.system) / 1_000 });
    }
  }
  const outputItems = entry.items ?? items;
  const row = { case: entry.name, items: outputItems, measurements: {} };
  for (const implementation of implementations) {
    const runs = observations.get(implementation.name);
    const times = runs.map((run) => run.elapsedMs);
    const medianMs = median(times);
    // Memory and timer delay are sampled in a separate, untimed pass.
    let retainedHeapBytes = null;
    let sampledPeakHeapGrowthBytes = null;
    let maximumTimerDelayMs = null;
    if (typeof globalThis.gc === "function") {
      globalThis.gc();
      const before = process.memoryUsage().heapUsed;
      let peak = before;
      let expectedTick = performance.now() + 1;
      let delay = 0;
      let ticks = 0;
      const monitor = setInterval(() => {
        ticks++;
        peak = Math.max(peak, process.memoryUsage().heapUsed);
        const now = performance.now();
        delay = Math.max(delay, now - expectedTick);
        expectedTick = now + 1;
      }, 1);
      try {
        await entry.run(implementation.api);
      } finally {
        clearInterval(monitor);
      }
      peak = Math.max(peak, process.memoryUsage().heapUsed);
      sampledPeakHeapGrowthBytes = Math.max(0, peak - before);
      maximumTimerDelayMs = ticks ? Math.max(0, delay) : null;
      globalThis.gc();
      retainedHeapBytes = process.memoryUsage().heapUsed - before;
    }
    row.measurements[implementation.name] = {
      medianMs,
      medianCpuMs: median(runs.map((run) => run.cpuMs)),
      minMs: Math.min(...times),
      maxMs: Math.max(...times),
      itemsPerSecond: outputItems / (medianMs / 1_000),
      retainedHeapBytes,
      sampledPeakHeapGrowthBytes,
      maximumTimerDelayMs,
    };
  }
  const current = row.measurements.current;
  const baseline = row.measurements.baseline;
  if (baseline) row.speedup = baseline.medianMs / current.medianMs;
  results.push(row);
  console.log(
    `${entry.name.padEnd(20)} ${current.medianMs.toFixed(2).padStart(9)} ms${outputItems === items ? "" : `; ${outputItems} items`}${baseline ? `; baseline ${baseline.medianMs.toFixed(2)} ms; ${row.speedup.toFixed(2)}x` : ""}${current.retainedHeapBytes === null ? "" : `; retained heap ${current.retainedHeapBytes} B`}`,
  );
}
const report = {
  runtime: runtime.name,
  version: runtime.version,
  platform: process.platform,
  arch: process.arch,
  items,
  samples,
  warmup,
  scheduler: "default",
  results,
};
if (args.has("json"))
  await writeFile(
    resolve(args.get("json")),
    JSON.stringify(report, null, 2) + "\n",
  );
