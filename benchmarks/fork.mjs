import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { writeFile } from "node:fs/promises";
import { cpus } from "node:os";
import { resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const cases = [
  { name: "sync-pair", branches: 2, capacity: 1 },
  { name: "sync-eight", branches: 8, capacity: 16 },
  { name: "async-pair", branches: 2, capacity: 1, async: true },
  { name: "lagging", branches: 2, capacity: 64, batch: 64 },
  { name: "detached", branches: 8, capacity: 16, detach: 7 },
];

function parse(argv) {
  const options = {
    items: 100_000,
    warmup: 3,
    samples: 5,
    trials: 5,
    case: "all",
    module: fileURLToPath(new URL("../dist/index.js", import.meta.url)),
    baseline: "",
    json: "",
  };
  const seen = new Set();
  for (const arg of argv) {
    const match = /^--([^=]+)=(.+)$/.exec(arg);
    if (!match || !Object.hasOwn(options, match[1]) || seen.has(match[1]))
      throw new Error(`Unknown, duplicate, or malformed option: ${arg}`);
    const [, key, value] = match;
    seen.add(key);
    options[key] = typeof options[key] === "number" ? Number(value) : value;
  }
  for (const key of ["items", "warmup", "samples", "trials"])
    if (
      !Number.isSafeInteger(options[key]) ||
      options[key] < (key === "warmup" ? 0 : 1)
    )
      throw new RangeError(`Invalid ${key}`);
  if (options.case !== "all" && !cases.some((c) => c.name === options.case))
    throw new Error(`Unknown case: ${options.case}`);
  options.module = pathToFileURL(resolve(options.module)).href;
  if (options.baseline)
    options.baseline = pathToFileURL(resolve(options.baseline)).href;
  return options;
}

function summary(values) {
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

async function workload(fork, spec, items) {
  let pulled = 0;
  let ended = 0;
  let returned = 0;
  const delivered = Array(spec.branches).fill(0);
  const source = {
    [spec.async ? Symbol.asyncIterator : Symbol.iterator]() {
      return {
        next() {
          // Include the reserved read in each branch's capacity check.
          for (let b = spec.detach ?? 0; b < spec.branches; b++)
            assert.ok(pulled - delivered[b] < spec.capacity);
          if (pulled === items) ended++;
          const result =
            pulled < items
              ? { done: false, value: pulled++ }
              : { done: true, value: undefined };
          return spec.async ? Promise.resolve(result) : result;
        },
        return() {
          returned++;
          return { done: true, value: undefined };
        },
      };
    },
  };
  const group = fork(source, spec);
  for (let b = 0; b < (spec.detach ?? 0); b++) group.branches[b].detach();
  const iterators = group.branches.map((b) => b.iterate());
  async function read(b) {
    const result = await iterators[b].next();
    if (!result.done) assert.equal(result.value, delivered[b]++);
    return result.done;
  }
  if (spec.batch) {
    // Fill and drain a sibling's queue, including its wraparound path.
    for (let start = 0; start < items; start += spec.batch) {
      const count = Math.min(spec.batch, items - start);
      for (let i = 0; i < count; i++) assert.equal(await read(0), false);
      for (let i = 0; i < count; i++) assert.equal(await read(1), false);
    }
    await Promise.all(
      iterators.map(async (_, b) => assert.equal(await read(b), true)),
    );
  } else {
    await Promise.all(
      iterators.slice(spec.detach ?? 0).map(async (_, index) => {
        const b = index + (spec.detach ?? 0);
        while (!(await read(b))) {}
      }),
    );
  }
  const report = await group.closed;
  assert.equal(report.source.kind, "completed");
  assert.deepEqual(report.source.suppressedErrors, []);
  assert.ok(report.branches.every((b) => b.kind === "completed"));
  assert.equal(pulled, items);
  assert.equal(ended, 1);
  assert.equal(returned, 0);
  assert.deepEqual(
    delivered,
    Array.from({ length: spec.branches }, (_, b) =>
      b < (spec.detach ?? 0) ? 0 : items,
    ),
  );
}

async function worker(job) {
  const { fork } = await import(job.options.module);
  const samples = [];
  for (
    let round = 0;
    round < job.options.warmup + job.options.samples;
    round++
  ) {
    const cpu = process.cpuUsage();
    const start = performance.now();
    await workload(fork, job.spec, job.options.items);
    const elapsedMs = performance.now() - start;
    const used = process.cpuUsage(cpu);
    if (round >= job.options.warmup)
      samples.push({ elapsedMs, cpuMs: (used.user + used.system) / 1000 });
  }
  return {
    case: job.spec.name,
    variant: job.variant,
    trial: job.trial,
    elapsedMs: summary(samples.map((s) => s.elapsedMs)),
    cpuMs: summary(samples.map((s) => s.cpuMs)),
    samples,
  };
}

function run(job) {
  return new Promise((accept, reject) => {
    const child = spawn(
      process.execPath,
      [...process.execArgv, fileURLToPath(import.meta.url)],
      {
        env: { ...process.env, MOGAMI_FORK_JOB: JSON.stringify(job) },
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    let output = "";
    let errors = "";
    child.stdout.on("data", (s) => {
      output += s;
    });
    child.stderr.on("data", (s) => {
      errors = (errors + s).slice(-16_000);
    });
    const timeout = setTimeout(() => child.kill("SIGKILL"), 120_000);
    child.on("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });
    child.on("close", (code, signal) => {
      clearTimeout(timeout);
      if (code !== 0)
        reject(new Error(`${job.spec.name}: ${signal ?? code}\n${errors}`));
      else {
        try {
          accept(JSON.parse(output));
        } catch (error) {
          reject(error);
        }
      }
    });
  });
}

if (process.env.MOGAMI_FORK_JOB) {
  console.log(
    JSON.stringify(await worker(JSON.parse(process.env.MOGAMI_FORK_JOB))),
  );
} else {
  const options = parse(process.argv.slice(2));
  const results = [];
  const variants = options.baseline
    ? [
        { name: "baseline", module: options.baseline },
        { name: "candidate", module: options.module },
      ]
    : [{ name: "candidate", module: options.module }];
  for (let trial = 0; trial < options.trials; trial++) {
    const selected = cases.filter(
      (c) => options.case === "all" || c.name === options.case,
    );
    if (trial % 2) selected.reverse();
    for (const spec of selected) {
      for (const variant of trial % 2 ? [...variants].reverse() : variants) {
        const result = await run({
          options: { ...options, module: variant.module },
          spec,
          trial,
          variant: variant.name,
        });
        results.push(result);
        console.log(
          `${spec.name}/${variant.name} trial ${trial + 1}: ${result.elapsedMs.median.toFixed(2)} ms; CPU ${result.cpuMs.median.toFixed(2)} ms`,
        );
      }
    }
  }
  const report = {
    schemaVersion: 1,
    createdAt: new Date().toISOString(),
    runtime: {
      version: process.version,
      versions: process.versions,
      executable: process.execPath,
      execArgv: process.execArgv,
      platform: process.platform,
      arch: process.arch,
      cpu: cpus()[0]?.model,
    },
    options,
    cases,
    methodology:
      "Fresh process per case/variant/trial; sequential jobs, alternating case and variant order; normal GC; warmups excluded. Timing includes construction, per-value order/bounds assertions, consumption and cleanup. Summaries use independent process medians.",
    results,
    summary: Object.fromEntries(
      cases
        .filter((c) => options.case === "all" || c.name === options.case)
        .flatMap((c) =>
          variants.map((v) => {
            const matching = results.filter(
              (r) => r.case === c.name && r.variant === v.name,
            );
            return [
              `${c.name}/${v.name}`,
              {
                elapsedMs: summary(matching.map((r) => r.elapsedMs.median)),
                cpuMs: summary(matching.map((r) => r.cpuMs.median)),
              },
            ];
          }),
        ),
    ),
  };
  console.log(JSON.stringify(report.summary, null, 2));
  if (options.json)
    await writeFile(options.json, JSON.stringify(report, null, 2) + "\n");
}
