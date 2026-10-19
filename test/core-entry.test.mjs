import assert from "node:assert/strict";
import { test } from "node:test";
import { readFile } from "node:fs/promises";
import * as core from "mogami-stream/core";
import * as fluent from "mogami-stream";

test("core exposes the same functions without fluent methods or prototype mutation", async () => {
  assert.deepEqual(Object.keys(core).sort(), Object.keys(fluent).sort());
  const source = core.of(1, 2);
  assert.equal("map" in source, false);
  assert.equal("take" in source, false);
  assert.equal("map" in Object.getPrototypeOf(source), false);
  assert.deepEqual(
    await source.pipe(core.map(String), core.take(1)).toArray(),
    ["1"],
  );
  assert.equal("map" in core.map(String)(source), false);
  assert.equal("map" in core.pipe(source, core.take(1)), false);
  assert.equal(typeof fluent.from(source).map, "function");
  assert.equal("map" in source, false);
});

test("every source factory from the default entry returns a fluent stream", async () => {
  const sources = [
    fluent.from([1]),
    fluent.defer(() => [1]),
    fluent.of(1),
    fluent.empty(),
    fluent.range(1, 2),
    fluent.chain([1]),
    fluent.merge([1]),
    fluent.zip([1]),
    fluent.zipLatest([[1]]),
    fluent.fromTimeline([{ at: 0, kind: "value", value: 1 }]),
    fluent.fromCallback(
      (observer) => {
        observer.next(1);
        observer.complete();
      },
      { capacity: 1 },
    ),
  ];
  for (const source of sources) {
    assert.equal(typeof source.map, "function");
    assert.deepEqual(
      await source
        .map(() => "ok")
        .take(1)
        .toArray(),
      source === sources[3] ? [] : ["ok"],
    );
  }
  const target = new EventTarget();
  const iterator = fluent
    .fromEvent(target, "value", { capacity: 1 })
    .map((event) => event.type)
    .take(1)
    .iterate();
  const pending = iterator.next();
  target.dispatchEvent(new Event("value"));
  assert.deepEqual(await pending, { done: false, value: "value" });
  await iterator.return();
});

test("core and fluent streams compose while keeping one execution and cleanup", async () => {
  let opened = 0;
  let closed = 0;
  const source = core.defer(() => {
    opened++;
    return (async function* () {
      try {
        yield 1;
        yield 2;
      } finally {
        closed++;
      }
    })();
  });
  const wrapped = fluent.from(source);
  assert.equal(fluent.from(wrapped), wrapped);
  const pipeline = wrapped
    .map((value) => value * 2)
    .pipe(core.map(String), core.take(1))
    .map((value) => `${value}!`);
  assert.equal(opened, 0);
  assert.deepEqual(await pipeline.toArray(), ["2!"]);
  assert.equal(opened, 1);
  assert.equal(closed, 1);
  assert.deepEqual(
    await core.from(fluent.of(3)).pipe(core.map(String)).toArray(),
    ["3"],
  );
  assert.deepEqual(await fluent.map(String)(source).take(1).toArray(), ["1"]);
  assert.deepEqual(
    await fluent.pipe(source, core.map(String)).take(1).toArray(),
    ["1"],
  );
});

test("core module graph cannot reach the fluent layer", async () => {
  const pending = [new URL("../dist/core/index.js", import.meta.url)];
  const visited = new Set();
  while (pending.length) {
    const url = pending.pop();
    if (visited.has(url.href)) continue;
    visited.add(url.href);
    assert.ok(!url.pathname.includes("/fluent/"), url.href);
    const source = await readFile(url, "utf8");
    for (const match of source.matchAll(
      /(?:from\s*|import\s*)["'](\.[^"']+)["']/g,
    )) {
      pending.push(new URL(match[1], url));
    }
  }
  assert.ok(visited.size > 10);
});
