import { fork } from "node:child_process";
import { writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { adaptersFor } from "../compare/adapters.mjs";
import { workloads } from "./workloads.mjs";

export function parseOptions(
  argv,
  { gcAvailable = typeof globalThis.gc === "function" } = {},
) {
  const defaults = {
    items: 10_000,
    samples: 7,
    warmup: 2,
    concurrency: 8,
    capacity: 16,
    seed: 42,
    "timeout-ms": 30_000,
  };
  const options = new Map();
  for (const argument of argv) {
    const match = /^--([^=]+)=(.+)$/.exec(argument);
    if (!match) throw new Error(`Expected --name=value: ${argument}`);
    const [, name, value] = match;
    if (
      !Object.hasOwn(defaults, name) &&
      !["case", "json", "gc"].includes(name)
    )
      throw new Error(`Unknown option: ${name}`);
    if (options.has(name)) throw new Error(`Duplicate option: ${name}`);
    options.set(name, value);
  }
  const gc = options.get("gc") ?? "none";
  if (!["none", "between"].includes(gc)) throw new Error("Invalid gc mode");
  if (gc === "between" && !gcAvailable)
    throw new Error("Manual GC requires --expose-gc");
  const numbers = {};
  for (const [name, fallback] of Object.entries(defaults)) {
    const value = Number(options.get(name) ?? fallback);
    if (!Number.isSafeInteger(value) || value < (name === "warmup" ? 0 : 1))
      throw new RangeError(
        `${name} must be a safe integer >= ${name === "warmup" ? 0 : 1}`,
      );
    numbers[name] = value;
  }
  if (numbers.capacity < numbers.concurrency)
    throw new RangeError("capacity must be >= concurrency");
  const selectedCase =
    options.get("case") === "all" ? undefined : options.get("case");
  if (selectedCase) {
    const names = selectedCase.split(",");
    if (
      new Set(names).size !== names.length ||
      names.some((name) => !workloads.some((w) => w.name === name))
    )
      throw new Error(`Unknown or duplicate case: ${selectedCase}`);
  }
  return {
    gc,
    items: numbers.items,
    samples: numbers.samples,
    warmup: numbers.warmup,
    concurrency: numbers.concurrency,
    capacity: numbers.capacity,
    seed: numbers.seed,
    timeoutMs: numbers["timeout-ms"],
    selectedCase,
    json: options.get("json"),
  };
}

export function summarize(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return {
    median:
      sorted.length % 2
        ? sorted[middle]
        : (sorted[middle - 1] + sorted[middle]) / 2,
    min: sorted[0],
    max: sorted.at(-1),
  };
}

export function shuffle(values, seed) {
  const result = [...values];
  let state = seed >>> 0;
  for (let i = result.length - 1; i > 0; i--) {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    const j = Math.floor((state / 2 ** 32) * (i + 1));
    [result[i], result[j]] = [result[j], result[i]];
  }
  return result;
}

function runWorker(job, config) {
  return new Promise((accept, reject) => {
    const child = fork(
      fileURLToPath(new URL("./worker.mjs", import.meta.url)),
      [],
      {
        stdio: ["ignore", "ignore", "pipe", "ipc"],
        env: {
          ...process.env,
          MOGAMI_BENCH_JOB: JSON.stringify({ ...job, config }),
        },
      },
    );
    let errors = "";
    let result;
    child.stderr.on("data", (chunk) => {
      errors = (errors + chunk).slice(-16_000);
    });
    child.on("message", (message) => {
      result = message;
    });
    child.on("error", reject);
    const watchdog = setTimeout(
      () => {
        child.kill("SIGKILL");
      },
      config.timeoutMs * (config.warmup + config.samples + 2),
    );
    child.on("exit", (code, signal) => {
      clearTimeout(watchdog);
      if (code !== 0 || !result)
        reject(
          new Error(
            `${job.case}/${job.library} failed (${signal ?? code}): ${errors}`,
          ),
        );
      else accept(result);
    });
  });
}

export async function runBenchmarks(argv = process.argv.slice(2)) {
  const config = parseOptions(argv);
  const runtime = {
    name: process.versions.bun ? "bun" : process.release.name,
    version: process.versions.bun
      ? `v${process.versions.bun}`
      : process.version,
    gc: config.gc,
    forcedGcBetweenRuns: config.gc === "between",
  };
  const adapters = await adaptersFor();
  const names = config.selectedCase?.split(",");
  const selected = workloads.filter((w) => !names || names.includes(w.name));
  const jobs = selected.flatMap((w) =>
    adapters
      .filter((a) => a.supports(w))
      .map((a) => ({ case: w.name, library: a.name })),
  );
  const order = shuffle(jobs, config.seed);
  const results = [];
  console.log(
    `${runtime.name} ${runtime.version}; ${config.warmup} warmups + ${config.samples} samples; isolated processes; GC ${config.gc}; seed ${config.seed}`,
  );
  for (const job of order) {
    const result = await runWorker(job, config);
    results.push(result);
    console.log(
      `${job.case.padEnd(17)} ${job.library.padEnd(15)} ${result.elapsedMs.median.toFixed(2).padStart(9)} ms; first ${result.firstItemMs?.median.toFixed(2) ?? "n/a"} ms; active ${result.maxActive}; ahead ${result.maxAhead}`,
    );
  }
  results.sort(
    (a, b) =>
      jobs.findIndex((j) => j.case === a.case && j.library === a.library) -
      jobs.findIndex((j) => j.case === b.case && j.library === b.library),
  );
  const report = {
    schemaVersion: 3,
    createdAt: new Date().toISOString(),
    runtime,
    configuration: Object.fromEntries(
      Object.entries(config).filter(([name]) => name !== "json"),
    ),
    methodology: {
      isolation:
        "One fresh process per case/library; sequential jobs in seeded shuffled order; warmups excluded",
      timing:
        "Includes construction, consumption, assertions per output, instrumentation, and cleanup; excludes prepared inputs, imports, post-run assertions, and process startup",
      async:
        "async-* uses AsyncIterable inputs and Promise-returning transforms without timer waits; configured concurrency is an upper bound, not necessarily observed overlap",
      bounds:
        "maxAhead = pulled minus received by sink, including in-flight work; maxReady = mapper completions minus received; neither is a heap measurement",
      statistics:
        "Median, min, max and every sample; no significance or cross-workload aggregate ranking",
    },
    workloads: selected,
    executionOrder: order,
    results,
  };
  if (config.json) {
    const path = resolve(config.json);
    await writeFile(path, `${JSON.stringify(report, null, 2)}\n`);
    console.log(`Wrote ${path}`);
  }
  return report;
}
