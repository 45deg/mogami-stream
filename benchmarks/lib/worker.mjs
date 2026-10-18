import { adaptersFor } from "../compare/adapters.mjs";
import { summarize } from "./harness.mjs";
import { prepareWorkload, workloads } from "./workloads.mjs";

const job = JSON.parse(process.env.MOGAMI_BENCH_JOB);
const { config } = job;
const workload = workloads.find((w) => w.name === job.case);
const adapter = (await adaptersFor(job.suite)).find(
  (a) => a.name === job.library,
);
if (!adapter?.supports(workload)) throw new Error("Unsupported case/library");
const execution = prepareWorkload(workload, config);
const samples = [];
for (let round = 0; round < config.warmup + config.samples; round++) {
  globalThis.gc?.();
  const context = execution();
  const watchdog = setTimeout(() => {
    console.error(`Run exceeded ${config.timeoutMs} ms`);
    process.exit(1);
  }, config.timeoutMs);
  try {
    const cpu = process.cpuUsage();
    const start = performance.now();
    context.begin();
    await adapter.run(workload, context);
    const elapsedMs = performance.now() - start;
    const used = process.cpuUsage(cpu);
    context.verify();
    if (round >= config.warmup)
      samples.push({
        elapsedMs,
        cpuMs: (used.user + used.system) / 1000,
        ...context.metrics,
      });
  } finally {
    clearTimeout(watchdog);
  }
}
const metric = (name) =>
  samples[0][name] === null ? null : summarize(samples.map((s) => s[name]));
process.send({
  case: job.case,
  library: job.library,
  version: adapter.version,
  elapsedMs: metric("elapsedMs"),
  cpuMs: metric("cpuMs"),
  firstItemMs: metric("firstItemMs"),
  stopToClosedMs: metric("stopToClosedMs"),
  maxActive: Math.max(...samples.map((s) => s.maxActive)),
  maxAhead:
    samples[0].maxAhead === null
      ? null
      : Math.max(...samples.map((s) => s.maxAhead)),
  maxReady:
    samples[0].maxReady === null
      ? null
      : Math.max(...samples.map((s) => s.maxReady)),
  samples,
});
process.disconnect();
