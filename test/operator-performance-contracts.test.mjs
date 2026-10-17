import { test } from "node:test";
import assert from "node:assert/strict";
import * as stream from "../dist/index.js";
import { flush, controlled } from "./helpers.mjs";

test("bufferTime preserves Promise and falsy values in full and partial batches", async () => {
  const value = Promise.resolve(1);
  assert.deepEqual(
    await stream.toArray(
      stream
        .of(undefined, value, false, null, 0)
        .pipe(stream.bufferTime(60_000, { maxItems: 3 })),
    ),
    [
      [undefined, value, false],
      [null, 0],
    ],
  );
});

test("skip(0) stays lazy and preserves Promise values and input cleanup", async () => {
  let opened = 0;
  let reads = 0;
  let returned = 0;
  const value = Promise.resolve(42);
  const source = {
    [Symbol.iterator]() {
      opened++;
      return {
        next() {
          reads++;
          return { value };
        },
        return() {
          returned++;
          return { done: true };
        },
      };
    },
  };
  const iterator = stream.iterate(stream.skip(0)(source));
  assert.equal(opened, 0);
  assert.equal((await iterator.next()).value, value);
  await iterator.return();
  assert.equal(opened, 1);
  assert.equal(reads, 1);
  assert.equal(returned, 1);
});

for (const name of ["flatten", "concat"]) {
  test(`${name} with one child preserves values and does not refill while idle`, async () => {
    let accepted = 0;
    const value = Promise.resolve(1);
    const source = {
      *[Symbol.iterator]() {
        accepted++;
        yield [value];
        accepted++;
        yield [2];
      },
    };
    const pipeline = stream.from(source).pipe(stream[name]());
    const iterator = stream.iterate(pipeline);
    assert.equal(accepted, 0);
    assert.equal((await iterator.next()).value, value);
    await flush();
    assert.equal(accepted, 1);
    assert.equal((await iterator.next()).value, 2);
    assert.equal((await iterator.next()).done, true);
    assert.equal((await iterator.closed).kind, "completed");
    assert.deepEqual(await stream.toArray(pipeline), [value, 2]);
  });

  test(`${name} cancellation observes a pending child and waits for cleanup`, async () => {
    const child = controlled();
    const iterator = stream.iterate(
      stream.of(child.source).pipe(stream[name]()),
    );
    const reading = assert.rejects(
      iterator.next(),
      (error) => error === "stop",
    );
    await flush();
    assert.equal(child.reads.length, 1);
    iterator.cancel("stop");
    await reading;
    assert.equal(child.returns, 1);
    let closed = false;
    iterator.closed.then(() => {
      closed = true;
    });
    await flush();
    assert.equal(closed, false);
    const late = new Error("late child failure");
    child.reads[0].reject(late);
    const report = await iterator.closed;
    assert.equal(report.kind, "cancelled");
    assert.deepEqual(report.suppressedErrors, [late]);
  });

  test(`${name} stops before accepting another child when cleanup fails`, async () => {
    let accepted = 0;
    const failure = new Error("child cleanup failed");
    const source = {
      *[Symbol.iterator]() {
        accepted++;
        yield stream.empty().pipe(
          stream.finalize(() => {
            throw failure;
          }),
        );
        accepted++;
        yield [1];
      },
    };
    await assert.rejects(
      stream.toArray(stream.from(source).pipe(stream[name]())),
      (error) => error === failure,
    );
    assert.equal(accepted, 1);
  });
}

for (const name of ["flatMap", "concatMap"]) {
  test(`${name} keeps mapper indexes and signals local to each sequential execution`, async () => {
    const contexts = [];
    const source = stream.of("a", "b").pipe(
      stream[name]((value, context) => {
        contexts.push(context);
        return [value, context.index];
      }),
    );
    assert.deepEqual(await stream.toArray(source), ["a", 0, "b", 1]);
    assert.deepEqual(await stream.toArray(source), ["a", 0, "b", 1]);
    assert.deepEqual(
      contexts.map(({ index }) => index),
      [0, 1, 0, 1],
    );
    assert.equal(contexts[0].signal, contexts[1].signal);
    assert.notEqual(contexts[0].signal, contexts[2].signal);
  });
}
