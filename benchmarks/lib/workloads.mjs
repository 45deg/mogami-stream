import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";

export const pipelineSteps = [
  { map: (id) => ({ id, value: id * 2 }) },
  ...Array.from({ length: 6 }, () => [
    { map: ({ id, value }) => ({ id, value: value + 1 }) },
    { filter: ({ value }) => value % 97 !== 0 },
  ]).flat(),
];

export const expandGroup = (id) =>
  Array.from({ length: 4 }, (_, offset) => ({
    id: id * 4 + offset,
    value: (id * 4 + offset) * 2,
  }));

export const workloads = [
  {
    name: "pipeline-depth",
    description:
      "13 synchronous map/filter stages with ordered incremental consumption",
    kind: "pipeline",
    delivery: "stream",
  },
  {
    name: "flatmap-sync",
    description:
      "Sequentially expand each input into four synchronous child values",
    kind: "flatten",
    delivery: "stream",
  },
  {
    name: "records-stream",
    description:
      "Parse JSON, filter active records, project values, consume incrementally",
    kind: "records",
    delivery: "stream",
  },
  {
    name: "records-batch",
    description:
      "Parse JSON, filter active records, project values, collect an ordered array",
    kind: "records",
    delivery: "batch",
  },
  {
    name: "io-serial",
    description:
      "Variable-latency simulated I/O, ordered incremental consumption, concurrency 1",
    kind: "io",
    delivery: "stream",
    serial: true,
  },
  {
    name: "io-batch",
    description:
      "Variable-latency simulated I/O, bounded concurrency, collect an ordered array",
    kind: "io",
    delivery: "batch",
  },
  {
    name: "io-stream",
    description:
      "Variable-latency simulated I/O, bounded ordered incremental consumption",
    kind: "io",
    delivery: "stream",
  },
  {
    name: "slow-sink",
    description:
      "Bounded ordered I/O with 4 KiB results and an awaited write per result",
    kind: "io",
    delivery: "stream",
    slow: true,
  },
  {
    name: "early-stop",
    description:
      "Read a prefix from a lazy resource-owning source, then close it",
    kind: "prefix",
    delivery: "stream",
  },
  {
    name: "cancel-inflight",
    description:
      "Cancel after all mapper slots start; await in-flight I/O and source cleanup",
    kind: "cancel",
    delivery: "stream",
  },
];

export function prepareWorkload(workload, config) {
  const count = ["io", "cancel"].includes(workload.kind)
    ? config.ioItems
    : config.items;
  const concurrency = workload.serial ? 1 : config.concurrency;
  const capacity = workload.serial ? 1 : config.capacity;
  const inputs = Array.from({ length: count }, (_, id) =>
    workload.kind === "records"
      ? JSON.stringify({ id, amount: (id % 100) * 7, active: id % 3 !== 0 })
      : id,
  );
  const expected = Array.from(
    { length: workload.kind === "flatten" ? count * 4 : count },
    (_, id) => ({
      id,
      value:
        workload.kind === "records"
          ? (id % 100) * 14
          : id * 2 + (workload.kind === "pipeline" ? 6 : 0),
    }),
  )
    .filter(({ id }) => workload.kind !== "records" || id % 3 !== 0)
    .filter(
      ({ id }) =>
        workload.kind !== "pipeline" ||
        Array.from({ length: 6 }, (_, i) => id * 2 + i + 1).every(
          (value) => value % 97 !== 0,
        ),
    )
    .slice(
      0,
      workload.kind === "prefix"
        ? Math.min(count, config.stopAfter)
        : undefined,
    );

  return function execution() {
    const controller = new AbortController();
    const metrics = {
      pulled: 0,
      started: 0,
      completed: 0,
      consumed: 0,
      active: 0,
      maxActive: 0,
      maxAhead: ["records", "pipeline", "flatten"].includes(workload.kind)
        ? null
        : 0,
      maxReady: ["records", "pipeline", "flatten"].includes(workload.kind)
        ? null
        : 0,
      sourceClosed: false,
      firstItemMs: null,
      stopToClosedMs: null,
    };
    let slotsStarted;
    const ready = new Promise((resolve) => {
      slotsStarted = resolve;
    });
    const update = () => {
      if (metrics.maxAhead === null) return;
      metrics.maxAhead = Math.max(
        metrics.maxAhead,
        metrics.pulled - metrics.consumed,
      );
      metrics.maxReady = Math.max(
        metrics.maxReady,
        metrics.completed - metrics.consumed,
      );
    };
    function* readInputs() {
      for (const input of inputs) {
        metrics.pulled++;
        update();
        yield input;
      }
    }
    const source =
      workload.kind === "prefix" || workload.kind === "cancel"
        ? {
            async *[Symbol.asyncIterator]() {
              try {
                yield* readInputs();
              } finally {
                // Model releasing a cursor/resource whose close operation must be awaited.
                await sleep(config.latencyMs);
                metrics.sourceClosed = true;
              }
            },
          }
        : {
            *[Symbol.iterator]() {
              try {
                yield* readInputs();
              } finally {
                metrics.sourceClosed = true;
              }
            },
          };
    async function mapper(id, signal = controller.signal) {
      metrics.started++;
      metrics.active++;
      metrics.maxActive = Math.max(metrics.maxActive, metrics.active);
      if (metrics.active === concurrency) slotsStarted();
      try {
        // Cancellation is triggered by a slot barrier, not a racing short timer.
        await sleep(
          workload.kind === "cancel"
            ? 60_000
            : config.latencyMs * (id % 5 === 0 ? 4 : 1),
          undefined,
          { signal },
        );
        metrics.completed++;
        update();
        return {
          id,
          value: id * 2,
          ...(workload.slow ? { payload: Buffer.alloc(4096, id % 256) } : {}),
        };
      } finally {
        metrics.active--;
      }
    }
    let start;
    async function consume(iterable) {
      let stopped;
      for await (const value of iterable) {
        const wanted = expected[metrics.consumed];
        // Exact position-by-position verification also detects duplicates and omissions.
        assert.ok(wanted, "unexpected output");
        assert.equal(value.id, wanted.id, "output order/id");
        assert.equal(value.value, wanted.value, "output value");
        if (workload.slow) {
          assert.equal(value.payload.length, 4096);
          assert.equal(value.payload[0], value.id % 256);
        }
        metrics.consumed++;
        metrics.firstItemMs ??= performance.now() - start;
        if (workload.slow) await sleep(config.sinkMs);
        if (
          workload.kind === "prefix" &&
          metrics.consumed === expected.length
        ) {
          stopped = performance.now();
          break;
        }
      }
      if (stopped !== undefined)
        metrics.stopToClosedMs = performance.now() - stopped;
    }
    return {
      source,
      mapper,
      consume,
      controller,
      ready,
      metrics,
      concurrency,
      capacity,
      begin() {
        start = performance.now();
      },
      verify() {
        assert.equal(
          metrics.active,
          0,
          "in-flight work leaked past completion",
        );
        assert.equal(metrics.sourceClosed, true, "source was not closed");
        assert.equal(
          metrics.consumed,
          workload.kind === "cancel" ? 0 : expected.length,
        );
        if (workload.kind === "prefix")
          assert.equal(
            metrics.pulled,
            expected.length,
            "prefix read beyond the last requested item",
          );
        else if (workload.kind !== "cancel")
          assert.equal(metrics.pulled, count);
        if (workload.kind === "io" || workload.kind === "cancel") {
          assert.equal(
            metrics.maxActive,
            concurrency,
            "configured concurrency was not exercised or was exceeded",
          );
          assert.equal(
            metrics.started,
            workload.kind === "cancel" ? concurrency : count,
          );
          if (workload.kind !== "cancel")
            assert.equal(metrics.completed, count);
        }
      },
    };
  };
}

export async function* serialMap(source, mapper) {
  for await (const value of source) yield await mapper(value);
}

export async function* projectRecords(source) {
  for (const line of source) {
    const record = JSON.parse(line);
    if (record.active) yield { id: record.id, value: record.amount * 2 };
  }
}

// A work-conserving pool for the array-result comparison, with ordered slots.
export async function poolArray(source, mapper, concurrency) {
  const iterator =
    source[Symbol.asyncIterator]?.() ?? source[Symbol.iterator]();
  const results = [];
  let index = 0;
  try {
    await Promise.all(
      Array.from({ length: concurrency }, async () => {
        for (;;) {
          const item = await iterator.next();
          if (item.done) return;
          const slot = index++;
          results[slot] = await mapper(item.value);
        }
      }),
    );
    return results;
  } finally {
    await iterator.return?.();
  }
}

// Fused native baseline; library adapters compose all 13 stages separately.
export async function* deepPipeline(source) {
  for (const id of source) {
    let value = id * 2;
    let accepted = true;
    for (let stage = 0; stage < 6; stage++) {
      value++;
      if (value % 97 === 0) {
        accepted = false;
        break;
      }
    }
    if (accepted) yield { id, value };
  }
}

export async function* expandGroups(source) {
  for (const id of source) yield* expandGroup(id);
}
