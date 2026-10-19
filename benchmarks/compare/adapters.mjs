import { readFile } from "node:fs/promises";
import * as mogami from "../../dist/index.js";

export async function adaptersFor() {
  const [ix, ixOps, iter, { pMapIterable }] = await Promise.all([
    import("ix/Ix.asynciterable"),
    import("ix/Ix.asynciterable.operators"),
    import("iter-tools"),
    import("p-map"),
  ]);
  const sequential = (w) => w.kind === "pipeline" || w.serial;
  return [
    {
      name: "mogami",
      version: await packageVersion(
        new URL("../../package.json", import.meta.url),
      ),
      supports: () => true,
      async run(w, c) {
        let output = mogami.from(c.source);
        if (w.kind === "pipeline") {
          for (const step of c.pipelineSteps)
            output = output.pipe(
              step.map ? mogami.map(step.map) : mogami.filter(step.filter),
            );
        } else {
          output = output.pipe(
            mogami.map(c.mapper, {
              concurrency: c.concurrency,
              capacity: c.capacity,
            }),
          );
        }
        await c.consume(output);
      },
    },
    {
      name: "ix",
      version: await dependencyVersion("ix"),
      supports: sequential,
      async run(w, c) {
        let output = ix.from(c.source);
        if (w.kind === "pipeline") {
          for (const step of c.pipelineSteps)
            output = output.pipe(
              step.map ? ixOps.map(step.map) : ixOps.filter(step.filter),
            );
        } else output = output.pipe(ixOps.map(c.mapper));
        await c.consume(output);
      },
    },
    {
      name: "iter-tools",
      version: await dependencyVersion("iter-tools"),
      supports: sequential,
      async run(w, c) {
        let output = c.source;
        if (w.kind === "pipeline") {
          for (const step of c.pipelineSteps)
            output = step.map
              ? iter.asyncMap(step.map, output)
              : iter.asyncFilter(step.filter, output);
        } else output = iter.asyncMap(c.mapper, output);
        await c.consume(output);
      },
    },
    {
      name: "p-map-iterable",
      version: await dependencyVersion("p-map"),
      supports: (w) => w.kind === "async-map",
      async run(w, c) {
        await c.consume(
          pMapIterable(c.source, c.mapper, {
            concurrency: c.concurrency,
            backpressure: c.capacity,
          }),
        );
      },
    },
  ];
}

async function packageVersion(url) {
  return JSON.parse(await readFile(url, "utf8")).version;
}

function dependencyVersion(name) {
  return packageVersion(
    new URL(`./node_modules/${name}/package.json`, import.meta.url),
  );
}
