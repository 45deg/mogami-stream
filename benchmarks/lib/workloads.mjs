import assert from "node:assert/strict";

const pipelineSteps = [
  { map: async (id) => ({ id, value: id * 2 }) },
  ...Array.from({ length: 6 }, () => [
    { map: async ({ id, value }) => ({ id, value: value + 1 }) },
    { filter: async ({ value }) => value % 97 !== 0 },
  ]).flat(),
];

export const workloads = [
  {
    name: "async-map-serial",
    description: "Async input and mapper without timer waits, concurrency 1",
    kind: "async-map",
    serial: true,
  },
  {
    name: "async-map-concurrent",
    description:
      "Async input and mapper without timer waits, bounded ordered streaming",
    kind: "async-map",
  },
  {
    name: "async-pipeline",
    description:
      "Async input and 13 Promise-returning map/filter stages without timer waits",
    kind: "pipeline",
  },
];

export function prepareWorkload(workload, config) {
  const count = config.items;
  const concurrency = workload.serial ? 1 : config.concurrency;
  const capacity = workload.serial ? 1 : config.capacity;
  const pipeline = workload.kind === "pipeline";
  const inputs = Array.from({ length: count }, (_, id) => id);
  const expected = inputs
    .map((id) => ({ id, value: id * 2 + (pipeline ? 6 : 0) }))
    .filter(
      ({ id }) =>
        !pipeline ||
        Array.from({ length: 6 }, (_, i) => id * 2 + i + 1).every(
          (value) => value % 97 !== 0,
        ),
    );

  return function execution() {
    const metrics = {
      pulled: 0,
      started: 0,
      completed: 0,
      consumed: 0,
      active: 0,
      maxActive: 0,
      maxAhead: pipeline ? null : 0,
      maxReady: pipeline ? null : 0,
      sourceClosed: false,
      firstItemMs: null,
    };
    const update = () => {
      if (pipeline) return;
      metrics.maxAhead = Math.max(
        metrics.maxAhead,
        metrics.pulled - metrics.consumed,
      );
      metrics.maxReady = Math.max(
        metrics.maxReady,
        metrics.completed - metrics.consumed,
      );
    };
    const source = {
      async *[Symbol.asyncIterator]() {
        try {
          for (const id of inputs) {
            metrics.pulled++;
            update();
            yield id;
          }
        } finally {
          metrics.sourceClosed = true;
        }
      },
    };
    async function mapper(id) {
      metrics.started++;
      metrics.active++;
      metrics.maxActive = Math.max(metrics.maxActive, metrics.active);
      try {
        await Promise.resolve();
        metrics.completed++;
        update();
        return { id, value: id * 2 };
      } finally {
        metrics.active--;
      }
    }
    let start;
    async function consume(iterable) {
      for await (const value of iterable) {
        const wanted = expected[metrics.consumed];
        assert.ok(wanted, "unexpected output");
        assert.equal(value.id, wanted.id, "output order/id");
        assert.equal(value.value, wanted.value, "output value");
        metrics.consumed++;
        metrics.firstItemMs ??= performance.now() - start;
      }
    }
    return {
      source,
      mapper,
      consume,
      metrics,
      concurrency,
      capacity,
      pipelineSteps,
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
        assert.equal(metrics.consumed, expected.length);
        assert.equal(metrics.pulled, count);
        if (!pipeline) {
          assert.ok(
            metrics.maxActive > 0 && metrics.maxActive <= concurrency,
            "configured concurrency was exceeded or mapper was never called",
          );
          assert.equal(metrics.started, count);
          assert.equal(metrics.completed, count);
          // One delivered item may precede the consumer's counter.
          assert.ok(
            metrics.maxAhead <= capacity + 1,
            "bounded stream read too far ahead",
          );
        }
      },
    };
  };
}
