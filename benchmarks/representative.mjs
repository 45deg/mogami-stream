import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { cpus } from "node:os";
import { fileURLToPath } from "node:url";
import { shuffle, summarize } from "./lib/harness.mjs";
import { measureScenario, pMapPath, scenarios } from "./lib/representative.mjs";

export function parseOptions(args) {
  const options = {
    samples: 3,
    warmup: 1,
    seed: 42,
    timeoutMs: 15000,
    sampleMs: 5,
  };
  const used = new Set();
  for (const arg of args) {
    const match =
      /^--(samples|warmup|seed|timeoutMs|sampleMs|case|json)=(.+)$/.exec(arg);
    assert.ok(match, `Unknown option or missing value: ${arg}`);
    const [, name, raw] = match;
    assert.ok(!used.has(name), `Duplicate option: ${name}`);
    used.add(name);
    if (name === "case" || name === "json") options[name] = raw;
    else {
      const value = Number(raw);
      assert.ok(
        Number.isSafeInteger(value) &&
          value >= (name === "warmup" || name === "seed" ? 0 : 1),
        `Invalid ${name}`,
      );
      options[name] = value;
    }
  }
  if (options.case)
    assert.ok(
      scenarios.some((profile) => profile.name === options.case),
      "Unknown case",
    );
  return options;
}

async function worker(job) {
  const samples = [];
  for (
    let round = 0;
    round < job.options.warmup + job.options.samples;
    round++
  ) {
    globalThis.gc?.();
    const watchdog = setTimeout(() => {
      console.error(`${job.profile.name} exceeded iteration timeout`);
      process.exit(1);
    }, job.options.timeoutMs);
    try {
      const sample = await measureScenario(
        job.profile,
        job.library,
        job.options.seed,
        job.options.sampleMs,
      );
      if (round >= job.options.warmup) samples.push(sample);
    } finally {
      clearTimeout(watchdog);
    }
  }
  const summary = {};
  for (const name of [
    "elapsedMs",
    "itemsPerSecond",
    "firstItemMs",
    "cancelToSettlementMs",
    "cancelToClosedMs",
  ])
    summary[name] =
      samples[0][name] === null
        ? null
        : summarize(samples.map((sample) => sample[name]));
  process.send({
    profile: job.profile,
    library: job.library,
    summary,
    samples,
  });
  process.disconnect();
}

function runWorker(job) {
  return new Promise((resolve, reject) => {
    const child = fork(fileURLToPath(import.meta.url), [], {
      stdio: ["ignore", "ignore", "pipe", "ipc"],
      env: { ...process.env, MOGAMI_REPRESENTATIVE_JOB: JSON.stringify(job) },
    });
    let errors = "",
      result;
    child.stderr.on("data", (data) => {
      errors = (errors + data).slice(-16000);
    });
    child.on("message", (message) => {
      result = message;
    });
    child.on("error", reject);
    const timeout = setTimeout(
      () => child.kill("SIGKILL"),
      job.options.timeoutMs * (job.options.samples + job.options.warmup + 1),
    );
    child.on("exit", (code, signal) => {
      clearTimeout(timeout);
      if (code !== 0 || !result)
        reject(
          new Error(
            `${job.profile.name}/${job.library} failed (${signal ?? code}): ${errors}`,
          ),
        );
      else resolve({ ...result, diagnostics: errors.trim() || null });
    });
  });
}

export async function runRepresentative(args = process.argv.slice(2)) {
  const options = parseOptions(args);
  const selected = scenarios.filter(
    (profile) => !options.case || profile.name === options.case,
  );
  const jobs = shuffle(
    selected.flatMap((profile) =>
      ["mogami", ...(profile.compare ? ["p-map-iterable"] : [])].map(
        (library) => ({ profile, library, options }),
      ),
    ),
    options.seed,
  );
  const pMapPackage = JSON.parse(
    await readFile(
      new URL("./compare/node_modules/p-map/package.json", import.meta.url),
      "utf8",
    ),
  );
  const report = {
    schemaVersion: 1,
    createdAt: new Date().toISOString(),
    runtime: {
      name: process.versions.bun ? "bun" : process.release.name,
      version: process.versions.bun ?? process.version,
      platform: process.platform,
      arch: process.arch,
      cpu: cpus()[0]?.model,
      forcedGcBetweenRuns: typeof globalThis.gc === "function",
    },
    settings: options,
    dependencies: {
      mogami: JSON.parse(
        await readFile(new URL("../package.json", import.meta.url), "utf8"),
      ).version,
      pMap: {
        version: pMapPackage.version,
        entrypointSha256: createHash("sha256")
          .update(await readFile(pMapPath))
          .digest("hex"),
      },
    },
    methodology: {
      isolation:
        "One fresh process per scenario/library; jobs run sequentially in seeded shuffled order; one default warmup, three default measured samples with the same seed; optional GC between iterations; imports, oracle arrays and GC outside timing",
      timing:
        "Includes pipeline construction, timer waits, output validation, instrumentation, and cleanup; excludes process startup; cancellation next() settlement and iterator.closed measured separately from actual abort time",
      memory:
        "process.memoryUsage() sampled every sampleMs and once after cleanup; independent peak for each field, not a simultaneous snapshot or guaranteed upper bound; event-loop delays can miss peaks; process RSS/heap includes runtime, library and instrumentation; preallocated oracle arrays scale with input count and are included in baseline; no memory ceiling or leak proof",
      bounds:
        "maxActive counts active attempts; retries retain stage slots between attempts; maxAhead = yielded inputs minus sink receipts; maxReady = successful attempts minus sink receipts; logical bounds asserted per transition, not heap limits; includes at most one item handed to sink",
      comparison:
        "Ordered delayed/slow-sink/retry only, same lazy async numeric source, payload, timer function, concurrency, capacity/backpressure, validation and sink; retry is one retry after 2 ms, failing every fifth ID only on attempt 1; p-map uses a matching callback wrapper; cancellation and unordered/rate have no library ranking",
      rate: "rateLimit counts source next() including final done; timestamp oracle observes inside source after admission, permits 1 ms observation skew at sliding-window boundaries; no per-attempt rate limiting claimed",
      statistics:
        "Every sample plus median/min/max; host-specific synthetic timers, not network/storage I/O; no confidence interval or causal queue comparison",
    },
    executionOrder: jobs.map(
      ({ profile, library }) => `${profile.name}/${library}`,
    ),
    results: [],
  };
  for (const job of jobs) {
    const result = await runWorker(job);
    report.results.push(result);
    console.log(
      `${job.profile.name}/${job.library}: ${result.summary.elapsedMs.median.toFixed(2)} ms; active ${Math.max(...result.samples.map((sample) => sample.maxActive))}; ahead ${Math.max(...result.samples.map((sample) => sample.maxAhead))}`,
    );
  }
  if (options.json)
    await writeFile(options.json, JSON.stringify(report, null, 2) + "\n");
  return report;
}

if (process.env.MOGAMI_REPRESENTATIVE_JOB)
  await worker(JSON.parse(process.env.MOGAMI_REPRESENTATIVE_JOB));
else if (process.argv[1] === fileURLToPath(import.meta.url))
  await runRepresentative();
