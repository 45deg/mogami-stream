import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { writeFile } from "node:fs/promises";
import { cpus } from "node:os";
import { resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const cases = [
  { name: "numeric-pair", branches: 2, capacity: 1 },
  { name: "keyed-eight", branches: 8, capacity: 16, keyed: true },
  { name: "guards-eight", branches: 8, capacity: 16, guards: true },
  { name: "async-source", branches: 2, capacity: 8, async: true },
  { name: "async-select", branches: 2, capacity: 8, asyncSelect: true },
  { name: "ignored", branches: 4, capacity: 16, guards: true, ignored: true },
  {
    name: "detached-default",
    branches: 4,
    capacity: 8,
    keyed: true,
    detached: true,
  },
  { name: "lagging", branches: 2, capacity: 64, pauseEvery: 64 },
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

async function workload(route, spec, items) {
  let pulled = 0;
  let ended = 0;
  let returned = 0;
  const delivered = Array(spec.branches).fill(0);
  const produced = Array(spec.branches).fill(0);
  const checksums = Array(spec.branches).fill(0);
  const keys = Array.from({ length: spec.branches }, (_, b) => `branch${b}`);
  const period = spec.branches * (spec.ignored ? 2 : 1);
  const destination = spec.detached
    ? () => spec.branches - 1
    : (value) => (value % period < spec.branches ? value % period : -1);
  const source = {
    [spec.async ? Symbol.asyncIterator : Symbol.iterator]() {
      return {
        next() {
          // A handed-off item may still await its consumer's continuation.
          for (let b = 0; b < spec.branches; b++)
            assert.ok(produced[b] - delivered[b] <= spec.capacity + 1);
          let result;
          if (pulled === items) {
            ended++;
            result = { done: true, value: undefined };
          } else {
            const value = pulled++;
            const target = destination(value);
            if (target >= 0) produced[target]++;
            result = { done: false, value };
          }
          return spec.async ? Promise.resolve(result) : result;
        },
        return() {
          returned++;
          return { done: true, value: undefined };
        },
      };
    },
  };
  const branches = spec.guards
    ? Object.fromEntries(
        keys.map((key, b) => [key, (value) => value % period === b]),
      )
    : spec.keyed
      ? keys
      : spec.branches;
  const options = { branches, capacity: spec.capacity };
  if (!spec.guards) {
    options.select = (value) => {
      const target =
        value % (spec.detached ? spec.branches - 1 : spec.branches);
      const key = spec.keyed ? keys[target] : target;
      return spec.asyncSelect ? Promise.resolve(key) : key;
    };
  }
  if (spec.detached) options.default = keys.at(-1);
  const group = route(source, options);
  const handles =
    spec.keyed || spec.guards
      ? keys.map((key) => group.branches[key])
      : group.branches;
  if (spec.detached) for (const branch of handles.slice(0, -1)) branch.detach();
  const iterators = handles.map((branch) => branch.iterate());
  await Promise.all(
    iterators.map(async (iterator, b) => {
      let expected = spec.detached ? 0 : b;
      for (;;) {
        const result = await iterator.next();
        if (result.done) break;
        assert.equal(result.value, expected);
        expected += spec.detached ? 1 : period;
        delivered[b]++;
        checksums[b] += result.value;
        if (spec.pauseEvery && b === 1 && delivered[b] % spec.pauseEvery === 0)
          await new Promise((resolve) => setImmediate(resolve));
      }
    }),
  );
  const report = await group.closed;
  assert.equal(report.source.kind, "completed");
  assert.deepEqual(report.source.suppressedErrors, []);
  for (const branch of Object.values(report.branches))
    assert.equal(branch.kind, "completed");
  assert.equal(pulled, items);
  assert.equal(ended, 1);
  assert.equal(returned, 0);
  assert.deepEqual(delivered, produced);
  for (let b = 0; b < spec.branches; b++) {
    const count = spec.detached
      ? b === spec.branches - 1
        ? items
        : 0
      : b < items
        ? Math.floor((items - 1 - b) / period) + 1
        : 0;
    const first = spec.detached ? 0 : b;
    const step = spec.detached ? 1 : period;
    assert.equal(delivered[b], count);
    assert.equal(
      checksums[b],
      count ? (count * (2 * first + (count - 1) * step)) / 2 : 0,
    );
  }
  return { delivered, checksums };
}

async function worker(job) {
  const { route } = await import(job.options.module);
  const samples = [];
  for (
    let round = 0;
    round < job.options.warmup + job.options.samples;
    round++
  ) {
    const cpu = process.cpuUsage();
    const start = performance.now();
    const verified = await workload(route, job.spec, job.options.items);
    const elapsedMs = performance.now() - start;
    const used = process.cpuUsage(cpu);
    if (round >= job.options.warmup)
      samples.push({
        elapsedMs,
        cpuMs: (used.user + used.system) / 1000,
        ...verified,
      });
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
        env: { ...process.env, MOGAMI_ROUTE_JOB: JSON.stringify(job) },
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

if (process.env.MOGAMI_ROUTE_JOB) {
  console.log(
    JSON.stringify(await worker(JSON.parse(process.env.MOGAMI_ROUTE_JOB))),
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
      "Fresh process per case/variant/trial; sequential jobs, alternating case and variant order; normal GC; warmups excluded. Timing includes construction, per-value order/bounds/checksum assertions, consumption and cleanup. Summaries use independent process medians.",
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
