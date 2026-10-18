import assert from "node:assert/strict";
import * as mogami from "../../dist/index.js";
import {
  poolArray,
  projectRecords,
  serialMap,
  pipelineSteps,
  deepPipeline,
  expandGroup,
  expandGroups,
} from "../lib/workloads.mjs";

const project = (record) => ({ id: record.id, value: record.amount * 2 });
const prefixValue = (id) => ({ id, value: id * 2 });

async function cancelMogami(context) {
  const iterator = mogami
    .from(context.source)
    .pipe(
      mogami.map((id, { signal }) => context.mapper(id, signal), {
        concurrency: context.concurrency,
        capacity: context.capacity,
      }),
    )
    .iterate();
  const reason = new Error("benchmark cancellation");
  const pending = iterator.next();
  const rejected = assert.rejects(pending, (error) => error === reason);
  await context.ready;
  const stopped = performance.now();
  iterator.cancel(reason);
  await rejected;
  const report = await iterator.closed;
  context.metrics.stopToClosedMs = performance.now() - stopped;
  assert.equal(report.kind, "cancelled");
  for (const error of report.suppressedErrors) {
    assert.equal(error.name, "AbortError");
    assert.equal(error.cause, reason);
  }
}

async function cancelNative(context) {
  const pending = poolArray(
    context.source,
    context.mapper,
    context.concurrency,
  );
  const rejected = assert.rejects(pending, { name: "AbortError" });
  await context.ready;
  const stopped = performance.now();
  context.controller.abort();
  await rejected;
  context.metrics.stopToClosedMs = performance.now() - stopped;
}

export async function adaptersFor(suite) {
  const adapters = [
    {
      name: "native",
      version: process.version,
      supports: (w) => !["io-stream", "slow-sink"].includes(w.name),
      async run(w, c) {
        if (w.kind === "cancel") return cancelNative(c);
        if (w.kind === "pipeline") return c.consume(deepPipeline(c.source));
        if (w.kind === "flatten") return c.consume(expandGroups(c.source));
        if (w.kind === "prefix")
          return c.consume(serialMap(c.source, prefixValue));
        if (w.kind === "records") {
          const output =
            w.delivery === "batch"
              ? [...c.source]
                  .map(JSON.parse)
                  .filter((r) => r.active)
                  .map(project)
              : projectRecords(c.source);
          return c.consume(output);
        }
        return c.consume(
          w.delivery === "batch"
            ? await poolArray(c.source, c.mapper, c.concurrency)
            : serialMap(c.source, c.mapper),
        );
      },
    },
    {
      name: "mogami",
      version: await packageVersion(
        new URL("../../package.json", import.meta.url),
      ),
      supports: () => true,
      async run(w, c) {
        if (w.kind === "cancel") return cancelMogami(c);
        let output = mogami.from(c.source);
        if (w.kind === "pipeline") {
          for (const step of pipelineSteps)
            output = output.pipe(
              step.map ? mogami.map(step.map) : mogami.filter(step.filter),
            );
        } else if (w.kind === "flatten") {
          output = output.pipe(mogami.flatMap(expandGroup));
        } else if (w.kind === "records")
          output = output.pipe(
            mogami.map(JSON.parse),
            mogami.filter((r) => r.active),
            mogami.map(project),
          );
        else
          output = output.pipe(
            mogami.map(
              w.kind === "prefix"
                ? prefixValue
                : (id, { signal }) => c.mapper(id, signal),
              {
                concurrency: w.kind === "prefix" ? 1 : c.concurrency,
                capacity: w.kind === "prefix" ? 1 : c.capacity,
              },
            ),
          );
        await c.consume(
          w.delivery === "batch" ? await output.toArray() : output,
        );
        // At the consumer boundary, one delivered item may precede the consumer's counter.
        if (w.kind === "io" && w.delivery === "stream")
          assert.ok(
            c.metrics.maxAhead <= c.capacity + 1,
            "bounded stream read too far ahead",
          );
      },
    },
  ];
  if (suite === "local") return adapters;

  const [
    ix,
    ixOps,
    iter,
    rx,
    pMapModule,
    pLimitModule,
    pQueueModule,
    pAllModule,
  ] = await Promise.all([
    import("ix/Ix.asynciterable"),
    import("ix/Ix.asynciterable.operators"),
    import("iter-tools"),
    import("rxjs"),
    import("p-map"),
    import("p-limit"),
    import("p-queue"),
    import("p-all"),
  ]);
  const serialSupported = (w) =>
    ["records", "prefix", "pipeline", "flatten"].includes(w.kind) || w.serial;
  adapters.push(
    {
      name: "ix",
      version: await dependencyVersion("ix"),
      supports: serialSupported,
      async run(w, c) {
        let output = ix.from(c.source);
        if (w.kind === "pipeline") {
          for (const step of pipelineSteps)
            output = output.pipe(
              step.map ? ixOps.map(step.map) : ixOps.filter(step.filter),
            );
          return c.consume(output);
        }
        if (w.kind === "flatten")
          return c.consume(output.pipe(ixOps.flatMap(expandGroup, 1)));
        output =
          w.kind === "records"
            ? output.pipe(
                ixOps.map(JSON.parse),
                ixOps.filter((r) => r.active),
                ixOps.map(project),
              )
            : output.pipe(
                ixOps.map(
                  w.kind === "prefix" ? prefixValue : (id) => c.mapper(id),
                ),
              );
        await c.consume(
          w.delivery === "batch" ? await ix.toArray(output) : output,
        );
      },
    },
    {
      name: "iter-tools",
      version: await dependencyVersion("iter-tools"),
      supports: serialSupported,
      async run(w, c) {
        if (w.kind === "pipeline") {
          let output = c.source;
          for (const step of pipelineSteps)
            output = step.map
              ? iter.asyncMap(step.map, output)
              : iter.asyncFilter(step.filter, output);
          return c.consume(output);
        }
        if (w.kind === "flatten")
          return c.consume(iter.asyncFlatMap(expandGroup, c.source));
        const output =
          w.kind === "records"
            ? iter.asyncMap(
                project,
                iter.asyncFilter(
                  (r) => r.active,
                  iter.asyncMap(JSON.parse, c.source),
                ),
              )
            : iter.asyncMap(
                w.kind === "prefix" ? prefixValue : (id) => c.mapper(id),
                c.source,
              );
        await c.consume(
          w.delivery === "batch" ? await iter.asyncToArray(output) : output,
        );
      },
    },
    {
      name: "rxjs",
      version: await dependencyVersion("rxjs"),
      supports: (w) => w.name === "records-batch",
      async run(w, c) {
        await c.consume(
          await rx.lastValueFrom(
            rx.from(c.source).pipe(
              rx.map(JSON.parse),
              rx.filter((r) => r.active),
              rx.map(project),
              rx.toArray(),
            ),
          ),
        );
      },
    },
    {
      name: "p-map",
      version: await dependencyVersion("p-map"),
      supports: (w) => w.name === "io-batch",
      async run(w, c) {
        await c.consume(
          await pMapModule.default(c.source, (id) => c.mapper(id), {
            concurrency: c.concurrency,
          }),
        );
      },
    },
    {
      name: "p-map-iterable",
      version: await dependencyVersion("p-map"),
      supports: (w) => ["io-stream", "slow-sink"].includes(w.name),
      async run(w, c) {
        await c.consume(
          pMapModule.pMapIterable(c.source, (id) => c.mapper(id), {
            concurrency: c.concurrency,
            backpressure: c.capacity,
          }),
        );
        assert.ok(
          c.metrics.maxAhead <= c.capacity + 1,
          "bounded iterable read too far ahead",
        );
      },
    },
    {
      name: "p-limit",
      version: await dependencyVersion("p-limit"),
      supports: (w) => w.name === "io-batch",
      async run(w, c) {
        const limit = pLimitModule.default(c.concurrency);
        await c.consume(
          await Promise.all(
            [...c.source].map((id) => limit(() => c.mapper(id))),
          ),
        );
      },
    },
    {
      name: "p-queue",
      version: await dependencyVersion("p-queue"),
      supports: (w) => w.name === "io-batch",
      async run(w, c) {
        const queue = new pQueueModule.default({ concurrency: c.concurrency });
        await c.consume(
          await Promise.all(
            [...c.source].map((id) => queue.add(() => c.mapper(id))),
          ),
        );
      },
    },
    {
      name: "p-all",
      version: await dependencyVersion("p-all"),
      supports: (w) => w.name === "io-batch",
      async run(w, c) {
        await c.consume(
          await pAllModule.default(
            [...c.source].map((id) => () => c.mapper(id)),
            { concurrency: c.concurrency },
          ),
        );
      },
    },
  );
  return adapters;
}

async function packageVersion(url) {
  const { readFile } = await import("node:fs/promises");
  return JSON.parse(await readFile(url, "utf8")).version;
}

async function dependencyVersion(name) {
  return packageVersion(
    new URL(`./node_modules/${name}/package.json`, import.meta.url),
  );
}
