import { test } from "node:test";
import assert from "node:assert/strict";
import { from, map, range, filter, iterate, toArray } from "../dist/index.js";
import { iterateWithScheduler } from "../dist/terminal/iterate.js";
import { deferred, flush, VirtualScheduler } from "./helpers.mjs";

test("one-slot mapping awaits thenables and reserves input only on demand", async () => {
  const gates = [deferred(), deferred()];
  const contexts = [];
  let reads = 0;
  const source = {
    *[Symbol.iterator]() {
      for (const value of [1, 2]) {
        reads++;
        yield value;
      }
    },
  };
  const iterator = iterate(
    from(source).pipe(
      map(
        (value, context) => {
          contexts.push(context);
          return {
            then(resolve) {
              gates[context.index].promise.then(() => resolve(value * 2));
            },
          };
        },
        { concurrency: 1, capacity: 1, ordered: false },
      ),
    ),
  );
  const first = iterator.next();
  await flush();
  assert.equal(reads, 1);
  gates[0].resolve();
  assert.equal((await first).value, 2);
  await flush();
  assert.equal(reads, 1);

  const second = iterator.next();
  await flush();
  assert.equal(reads, 2);
  const closing = iterator.return();
  assert.equal(contexts[1].signal.aborted, true);
  gates[1].resolve();
  assert.equal((await second).done, true);
  await closing;
  assert.deepEqual(
    contexts.map((context) => context.index),
    [0, 1],
  );
});

test("synchronous filtering observes callback cancellation and closes its input", async () => {
  const controller = new AbortController();
  const reason = new Error("callback cancellation");
  let reads = 0;
  let closed = false;
  const source = {
    *[Symbol.iterator]() {
      try {
        for (;;) yield 1;
      } finally {
        closed = true;
      }
    },
  };
  await assert.rejects(
    toArray(
      from(source).pipe(
        filter(() => {
          if (++reads === 1_000) controller.abort(reason);
          return false;
        }),
      ),
      {
        signal: controller.signal,
      },
    ),
    (error) => error === reason,
  );
  assert.equal(closed, true);
  assert.equal(reads, 1_000);
});

test("plain pipelines do not consult the scheduler even across many reads and elapsed callback work", async () => {
  const scheduler = new VirtualScheduler();
  scheduler.now = () => {
    throw new Error("Plain pipelines must not read the scheduler clock.");
  };
  scheduler.sleep = () => {
    throw new Error("Plain pipelines must not schedule a wait.");
  };
  const iterator = iterateWithScheduler(
    range(0, 2_000).pipe(
      map((value) => {
        scheduler.time += 1;
        return value;
      }),
    ),
    {},
    scheduler,
  );
  let sum = 0;
  for await (const value of iterator) sum += value;
  assert.equal(sum, 1_999_000);
  assert.equal((await iterator.closed).kind, "completed");
});
