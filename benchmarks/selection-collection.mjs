import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { cpus } from "node:os";
import { writeFile } from "node:fs/promises";
import { from } from "../dist/index.js";
import { summarize, shuffle } from "./lib/harness.mjs";

// Each job runs in a fresh process; verify outputs outside the timed interval.
const items = 20_000;
const width = 16;
const values = Array.from({ length: items }, (_, i) => i);
const cases = {
  drain: [(s) => s.forEach(() => {}), (s) => s.drain(), undefined],
  count: [(s) => s.reduce((n) => n + 1, 0), (s) => s.count(), items],
  toSet: [
    (s) => s.reduce((set, x) => set.add(x), new Set()),
    (s) => s.toSet(),
    new Set(values),
  ],
  toMap: [
    (s) => s.reduce((map, x) => map.set(x % 1024, x), new Map()),
    (s) => s.toMap((x) => x % 1024),
    new Map(values.map((x) => [x % 1024, x])),
  ],
  slice: [
    (s) =>
      s
        .skip(items / 4)
        .take(items / 2)
        .toArray(),
    (s) => s.slice(items / 4, (3 * items) / 4).toArray(),
    values.slice(items / 4, (3 * items) / 4),
  ],
  at: [
    (s) => s.skip(items / 2).find(() => true),
    (s) => s.at(items / 2),
    items / 2,
  ],
  "at-tail": [
    async (s) => (await s.toArray()).at(-width),
    (s) => s.at(-width),
    items - width,
  ],
  slidingWindow: [
    (s) =>
      s
        .scan((window, x) => {
          if (window.length === width) window.shift();
          window.push(x);
          return window;
        }, [])
        .filter((w) => w.length === width)
        .map((w) => w.slice())
        .toArray(),
    (s) => s.slidingWindow(width).toArray(),
    values.slice(width - 1).map((_, i) => values.slice(i, i + width)),
  ],
};

if (process.argv[2] === "--worker") {
  const [name, variant] = process.argv.slice(3);
  const [baseline, specialized, expected] = cases[name];
  const run = variant === "baseline" ? baseline : specialized;
  for (let i = 0; i < 3; i++)
    assert.deepEqual(await run(from(values)), expected);
  const samples = [];
  for (let i = 0; i < 5; i++) {
    const start = performance.now();
    const result = await run(from(values));
    samples.push(performance.now() - start);
    assert.deepEqual(result, expected);
  }
  process.send({ samples, ...summarize(samples) });
} else {
  const jobs = [];
  for (let trial = 0; trial < 3; trial++)
    for (const name of Object.keys(cases))
      for (const variant of ["baseline", "specialized"])
        jobs.push({ name, variant, trial });
  const results = [];
  for (const job of shuffle(jobs, 42)) {
    const measurement = await new Promise((resolve, reject) => {
      const child = fork(
        import.meta.filename,
        ["--worker", job.name, job.variant],
        { stdio: ["ignore", "ignore", "inherit", "ipc"] },
      );
      let result;
      const timer = setTimeout(() => {
        child.kill();
        reject(new Error("Benchmark timed out"));
      }, 60_000);
      child.on("message", (value) => {
        result = value;
      });
      child.on("error", (error) => {
        clearTimeout(timer);
        reject(error);
      });
      child.on("exit", (code) => {
        clearTimeout(timer);
        code === 0 && result
          ? resolve(result)
          : reject(new Error(`Worker exited: ${code}`));
      });
    });
    results.push({ ...job, ...measurement });
  }
  const metadata = {
    node: process.version,
    versions: process.versions,
    executable: process.execPath,
    platform: process.platform,
    arch: process.arch,
    cpu: cpus()[0]?.model,
    items,
    width,
    warmup: 3,
    samplesPerProcess: 5,
    independentTrials: 3,
  };
  const summary = Object.keys(cases).map((name) => {
    const baseline = summarize(
      results
        .filter((x) => x.name === name && x.variant === "baseline")
        .map((x) => x.median),
    );
    const specialized = summarize(
      results
        .filter((x) => x.name === name && x.variant === "specialized")
        .map((x) => x.median),
    );
    return { name, baseline, specialized };
  });
  const output = process.argv[2];
  if (output)
    await writeFile(
      output,
      JSON.stringify({ metadata, results, summary }, null, 2) + "\n",
    );
  console.log(metadata);
  console.table(
    summary.map(({ name, baseline, specialized }) => ({
      name,
      "baseline ms": baseline.median.toFixed(2),
      "specialized ms": specialized.median.toFixed(2),
      "specialized range ms": `${specialized.min.toFixed(2)}–${specialized.max.toFixed(2)}`,
    })),
  );
}
