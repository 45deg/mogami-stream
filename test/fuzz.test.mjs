import { test } from "node:test";
import assert from "node:assert/strict";
import * as stream from "../dist/index.js";
import { iterateWithScheduler } from "../dist/terminal/iterate.js";
import { controlled, flush, turn, VirtualScheduler } from "./helpers.mjs";
import { forSeeds } from "./fuzz-helpers.mjs";

const values = [
  undefined,
  null,
  false,
  0,
  -0,
  NaN,
  "value",
  { id: 1 },
  Promise.resolve(2),
];

// Use the async protocol without awaiting Promise-valued elements.
function asyncSource(items, delay) {
  return {
    [Symbol.asyncIterator]() {
      let index = 0;
      return {
        async next() {
          for (let turn = 0; turn < delay; turn++) await Promise.resolve();
          return index < items.length
            ? { value: items[index++], done: false }
            : { done: true };
        },
        return() {
          return { done: true };
        },
      };
    },
  };
}

test("seeded fuzz: sequential composition matches array operations", async () => {
  await forSeeds(async (pick) => {
    const children = Array.from({ length: pick(9) }, () =>
      Array.from({ length: pick(9) }, () => values[pick(values.length)]),
    );
    const sources = children.map((child) =>
      pick(2) ? child : asyncSource(child, pick(4)),
    );
    const name = ["flatten", "concat", "flatMap", "concatMap"][pick(4)];
    const operator = name.endsWith("Map")
      ? stream[name]((child) => child)
      : stream[name]();
    const skip = pick(5);
    const count = pick(children.flat().length + 3);
    const size = pick(8) + 1;
    const expected = children.flat().slice(skip).slice(0, count);
    const chunks = [];
    for (let index = 0; index < expected.length; index += size)
      chunks.push(expected.slice(index, index + size));
    const pipeline = stream
      .from(asyncSource(sources, pick(4)))
      .pipe(
        operator,
        stream.skip(0),
        stream.skip(skip),
        stream.take(count),
        stream.chunks(size),
      );
    assert.deepEqual(await stream.toArray(pipeline), chunks);
    assert.deepEqual(await stream.toArray(pipeline), chunks);
  });
});

test("seeded fuzz: bufferTime on a frozen clock matches array chunks", async () => {
  await forSeeds(async (pick) => {
    const input = Array.from(
      { length: pick(33) },
      () => values[pick(values.length)],
    );
    const size = pick(8) + 1;
    const expected = [];
    for (let index = 0; index < input.length; index += size)
      expected.push(input.slice(index, index + size));
    const clock = new VirtualScheduler();
    const iterator = iterateWithScheduler(
      stream
        .from(asyncSource(input, pick(4)))
        .pipe(stream.bufferTime(10, { maxItems: size })),
      {},
      clock,
    );
    const actual = [];
    for await (const batch of iterator) actual.push(batch);
    assert.deepEqual(actual, expected);
    assert.equal((await iterator.closed).kind, "completed");
    assert.equal(clock.timers.size, 0);
  });
});

test("seeded fuzz: bufferTime preserves values across timer and arrival interleavings", async () => {
  await forSeeds(async (pick) => {
    const input = Array.from(
      { length: pick(17) },
      () => values[pick(values.length)],
    );
    const size = pick(6) + 1;
    const source = controlled();
    const clock = new VirtualScheduler();
    const iterator = iterateWithScheduler(
      stream
        .from(source.source)
        .pipe(stream.bufferTime(10, { maxItems: size })),
      {},
      clock,
    );
    const batches = [];
    const collecting = (async () => {
      for await (const batch of iterator) batches.push(batch);
    })();
    for (let index = 0; index <= input.length; index++) {
      await flush();
      await clock.advance(pick(16));
      await turn();
      assert.equal(source.reads.length, index + 1);
      source.reads[index].resolve(
        index < input.length ? { value: input[index] } : { done: true },
      );
    }
    await collecting;
    assert.deepEqual(batches.flat(), input);
    assert.ok(
      batches.every((batch) => batch.length > 0 && batch.length <= size),
    );
    assert.equal(clock.timers.size, 0);
    assert.equal((await iterator.closed).kind, "completed");
  });
});

test("seeded fuzz: stopping sequential children observes late results without refilling", async () => {
  await forSeeds(async (pick) => {
    const name = ["flatten", "concat", "flatMap", "concatMap"][pick(4)];
    const operator = name.endsWith("Map")
      ? stream[name]((child) => child)
      : stream[name]();
    const child = controlled();
    let accepted = 0;
    const source = {
      *[Symbol.iterator]() {
        accepted++;
        yield child.source;
        accepted++;
        yield ["unrequested"];
      },
    };
    const controller = new AbortController();
    const iterator = stream.iterate(stream.from(source).pipe(operator), {
      signal: controller.signal,
    });
    const count = pick(9);
    for (let index = 0; index < count; index++) {
      const reading = iterator.next();
      await flush();
      child.reads[index].resolve({ value: index });
      assert.equal((await reading).value, index);
      await flush();
      assert.equal(child.reads.length, index + 1);
    }
    const reading = iterator.next().then(
      (value) => ({ value }),
      (error) => ({ error }),
    );
    await flush();
    const mode = pick(3);
    const reason = pick(2) ? NaN : new Error("fuzz cancellation");
    let returning;
    if (mode === 0)
      returning = iterator.return().then(
        (value) => ({ value }),
        (error) => ({ error }),
      );
    else if (mode === 1) iterator.cancel(reason);
    else controller.abort(reason);
    const result = await reading;
    if (mode === 0) assert.equal(result.value.done, true);
    else assert.ok(Object.is(result.error, reason));
    let closed = false;
    iterator.closed.then(() => {
      closed = true;
    });
    await flush();
    assert.equal(closed, false);
    assert.equal(child.returns, 1);
    assert.equal(accepted, 1);
    const late = new Error("late fuzz failure");
    const fails = pick(2);
    if (fails) child.reads[count].reject(late);
    else child.reads[count].resolve({ value: "late value" });
    const report = await iterator.closed;
    if (returning) {
      const returned = await returning;
      assert.equal(report.kind, fails ? "failed" : "completed");
      if (fails) assert.equal(returned.error, late);
      else assert.equal(returned.value.done, true);
    } else assert.equal(report.kind, "cancelled");
    assert.deepEqual(report.suppressedErrors, fails ? [late] : []);
    assert.equal(accepted, 1);
  });
});
