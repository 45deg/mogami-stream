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
  {
    name: "broadcast-balanced",
    description: "Synchronous input shared by eight balanced consumers",
    kind: "broadcast",
    branches: 8,
  },
  {
    name: "broadcast-lagging",
    description:
      "Synchronous input shared by two consumers, one reading in batches",
    kind: "broadcast",
    branches: 2,
    batched: true,
  },
  {
    name: "route-numeric-pair",
    description: "Numeric routing to two consumers",
    kind: "route",
    branches: 2,
  },
  {
    name: "route-keyed-eight",
    description: "Named routing to eight consumers",
    kind: "route",
    branches: 8,
    keyed: true,
  },
  {
    name: "route-guards-eight",
    description: "Predicate routing to eight consumers",
    kind: "route",
    branches: 8,
    guards: true,
  },
  {
    name: "route-async-source",
    description: "Numeric routing with a Promise-returning source",
    kind: "route",
    branches: 2,
    async: true,
  },
  {
    name: "route-async-select",
    description: "Numeric routing with a Promise-returning selector",
    kind: "route",
    branches: 2,
    asyncSelect: true,
  },
  {
    name: "route-ignored",
    description: "Predicate routing to four consumers, ignoring half the input",
    kind: "route",
    branches: 4,
    guards: true,
    ignored: true,
  },
  {
    name: "route-detached-default",
    description: "Named routing to a default after other destinations detach",
    kind: "route",
    branches: 4,
    keyed: true,
    detached: true,
  },
  {
    name: "route-lagging",
    description:
      "Numeric routing with one consumer yielding after each capacity-sized batch",
    kind: "route",
    branches: 2,
    lagging: true,
  },
];

export function prepareWorkload(workload, config) {
  if (workload.kind === "broadcast") return prepareBroadcast(workload, config);
  if (workload.kind === "route") return prepareRoute(workload, config);
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

function prepareBroadcast(workload, config) {
  const { items, capacity } = config;
  const { branches, batched } = workload;
  return function execution() {
    const delivered = Array(branches).fill(0);
    const metrics = {
      pulled: 0,
      consumed: 0,
      maxActive: 0,
      maxAhead: 0,
      maxReady: null,
      sourceClosed: false,
      firstItemMs: null,
    };
    let ended = 0;
    let returned = 0;
    const source = {
      [Symbol.iterator]() {
        return {
          next() {
            // Include the reserved read in every branch's capacity check.
            for (const count of delivered)
              assert.ok(metrics.pulled - count < capacity);
            if (metrics.pulled === items) {
              ended++;
              metrics.sourceClosed = true;
              return { done: true, value: undefined };
            }
            const value = metrics.pulled++;
            metrics.maxAhead = Math.max(
              metrics.maxAhead,
              metrics.pulled - Math.min(...delivered),
            );
            return { done: false, value };
          },
          return() {
            returned++;
            metrics.sourceClosed = true;
            return { done: true, value: undefined };
          },
        };
      },
    };
    let start;
    let report;
    return {
      source,
      capacity,
      metrics,
      begin() {
        start = performance.now();
      },
      async consumeBroadcast(group) {
        const iterators = group.branches.map((branch) => branch.iterate());
        async function read(b) {
          const result = await iterators[b].next();
          if (!result.done) {
            assert.equal(result.value, delivered[b]++, "branch output order");
            metrics.consumed = Math.min(...delivered);
            metrics.firstItemMs ??= performance.now() - start;
          }
          return result.done;
        }
        if (batched) {
          for (let offset = 0; offset < items; offset += capacity) {
            const count = Math.min(capacity, items - offset);
            for (let b = 0; b < branches; b++)
              for (let i = 0; i < count; i++)
                assert.equal(await read(b), false);
          }
          await Promise.all(
            iterators.map(async (_, b) => assert.equal(await read(b), true)),
          );
        } else {
          await Promise.all(
            iterators.map(async (_, b) => {
              while (!(await read(b))) {}
            }),
          );
        }
        report = await group.closed;
      },
      verify() {
        assert.equal(metrics.sourceClosed, true, "source was not closed");
        assert.equal(metrics.pulled, items);
        assert.equal(metrics.consumed, items);
        assert.deepEqual(delivered, Array(branches).fill(items));
        assert.ok(metrics.maxAhead <= capacity, "branch read too far ahead");
        assert.equal(ended, 1);
        assert.equal(returned, 0);
        assert.equal(report.source.kind, "completed");
        assert.deepEqual(report.source.suppressedErrors, []);
        assert.ok(
          report.branches.every((branch) => branch.kind === "completed"),
        );
      },
    };
  };
}

function prepareRoute(workload, config) {
  const { items, capacity } = config;
  const { branches, keyed, guards, ignored, detached, asyncSelect, lagging } =
    workload;
  const keys = Array.from({ length: branches }, (_, b) => `branch${b}`);
  const period = branches * (ignored ? 2 : 1);
  const destination = detached
    ? () => branches - 1
    : (value) => (value % period < branches ? value % period : -1);
  const routeOptions = {
    capacity,
    branches: guards
      ? Object.fromEntries(
          keys.map((key, b) => [key, (value) => value % period === b]),
        )
      : keyed
        ? keys
        : branches,
  };
  if (!guards) {
    routeOptions.select = (value) => {
      const target = value % (detached ? branches - 1 : branches);
      const key = keyed ? keys[target] : target;
      return asyncSelect ? Promise.resolve(key) : key;
    };
  }
  if (detached) routeOptions.default = keys.at(-1);

  return function execution() {
    const delivered = Array(branches).fill(0);
    const produced = Array(branches).fill(0);
    const checksums = Array(branches).fill(0);
    const metrics = {
      pulled: 0,
      consumed: 0,
      maxActive: 0,
      maxAhead: 0,
      maxReady: null,
      sourceClosed: false,
      firstItemMs: null,
      delivered,
      checksums,
    };
    let ended = 0;
    let returned = 0;
    const source = {
      [workload.async ? Symbol.asyncIterator : Symbol.iterator]() {
        return {
          next() {
            // A handed-off item may still await its consumer's continuation.
            for (let b = 0; b < branches; b++)
              assert.ok(produced[b] - delivered[b] <= capacity + 1);
            let result;
            if (metrics.pulled === items) {
              ended++;
              metrics.sourceClosed = true;
              result = { done: true, value: undefined };
            } else {
              const value = metrics.pulled++;
              const target = destination(value);
              if (target >= 0) {
                produced[target]++;
                metrics.maxAhead = Math.max(
                  metrics.maxAhead,
                  produced[target] - delivered[target],
                );
              }
              result = { done: false, value };
            }
            return workload.async ? Promise.resolve(result) : result;
          },
          return() {
            returned++;
            metrics.sourceClosed = true;
            return { done: true, value: undefined };
          },
        };
      },
    };
    let start;
    let report;
    return {
      source,
      routeOptions,
      metrics,
      begin() {
        start = performance.now();
      },
      async consumeRoute(group) {
        const handles =
          keyed || guards
            ? keys.map((key) => group.branches[key])
            : group.branches;
        if (detached)
          for (const branch of handles.slice(0, -1)) branch.detach();
        const iterators = handles.map((branch) => branch.iterate());
        await Promise.all(
          iterators.map(async (iterator, b) => {
            let expected = detached ? 0 : b;
            for (;;) {
              const result = await iterator.next();
              if (result.done) break;
              assert.equal(result.value, expected, "route branch output order");
              expected += detached ? 1 : period;
              delivered[b]++;
              checksums[b] += result.value;
              metrics.consumed++;
              metrics.firstItemMs ??= performance.now() - start;
              if (lagging && b === 1 && delivered[b] % capacity === 0)
                await new Promise((resolve) => setImmediate(resolve));
            }
          }),
        );
        report = await group.closed;
      },
      verify() {
        assert.equal(metrics.sourceClosed, true, "source was not closed");
        assert.equal(metrics.pulled, items);
        assert.equal(ended, 1);
        assert.equal(returned, 0);
        assert.deepEqual(delivered, produced);
        for (let b = 0; b < branches; b++) {
          const count = detached
            ? b === branches - 1
              ? items
              : 0
            : b < items
              ? Math.floor((items - 1 - b) / period) + 1
              : 0;
          const first = detached ? 0 : b;
          const step = detached ? 1 : period;
          assert.equal(delivered[b], count);
          assert.equal(
            checksums[b],
            count ? (count * (2 * first + (count - 1) * step)) / 2 : 0,
          );
        }
        assert.equal(report.source.kind, "completed");
        assert.deepEqual(report.source.suppressedErrors, []);
        for (const branch of Object.values(report.branches))
          assert.equal(branch.kind, "completed");
      },
    };
  };
}
