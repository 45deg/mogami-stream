import { test } from "node:test";
import assert from "node:assert/strict";
import * as stream from "../dist/index.js";
import { iterateWithScheduler } from "../dist/terminal/iterate.js";
import { controlled, deferred, flush, VirtualScheduler } from "./helpers.mjs";

test("zip stops siblings as soon as an input ends, before its finalizer finishes", async () => {
  const cleanup = deferred();
  const sibling = controlled();
  const iterator = stream.iterate(
    stream.zip(
      stream.empty().pipe(stream.finalize(() => cleanup.promise)),
      sibling.source,
    ),
  );
  const next = iterator.next();
  await flush();
  const returnsBeforeCleanup = sibling.returns;
  cleanup.resolve();
  for (const read of sibling.reads) read.resolve({ done: true });
  await next;
  await iterator.closed;
  assert.equal(returnsBeforeCleanup, 1);
});

test("synchronous cleanup failure does not reenter finalizers during zip completion", async () => {
  let finalized = 0;
  const failure = new Error("return failed");
  const sibling = {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      return { value: 1, done: false };
    },
    return() {
      throw failure;
    },
  };
  const iterator = stream.iterate(
    stream.zip([], sibling).pipe(
      stream.finalize(() => {
        finalized++;
      }),
    ),
  );
  await assert.rejects(iterator.next(), (error) => error === failure);
  await iterator.closed;
  assert.equal(finalized, 1);
});

test("bufferTime retains an arrival observed beyond the old window for the next batch", async () => {
  const clock = new VirtualScheduler();
  const input = controlled();
  const iterator = iterateWithScheduler(
    stream.from(input.source).pipe(stream.bufferTime(10, { maxItems: 3 })),
    {},
    clock,
  );
  const first = iterator.next();
  await flush();
  input.reads[0].resolve({ value: "first" });
  await flush();
  // Simulate a busy host: the clock advances before timer callbacks run.
  clock.time = 11;
  input.reads[1].resolve({ value: "next window" });
  const firstBatch = (await first).value;
  const second = iterator.next();
  await flush();
  input.reads.at(-1).resolve({ done: true });
  const secondBatch = (await second).value;
  await iterator.return();
  assert.deepEqual(firstBatch, ["first"]);
  assert.deepEqual(secondBatch, ["next window"]);
});

test("deadline checks elapsed time before starting another read even when its timer is delayed", async () => {
  const clock = new VirtualScheduler();
  let reads = 0;
  const source = {
    *[Symbol.iterator]() {
      for (let i = 0; i < 3; i++) {
        reads++;
        yield i;
      }
    },
  };
  const iterator = iterateWithScheduler(
    stream.from(source).pipe(stream.deadline(10)),
    {},
    clock,
  );
  await iterator.next();
  clock.time = 11;
  const result = await iterator.next().then(
    (value) => ({ value }),
    (error) => ({ error }),
  );
  const readsAfterDeadline = reads;
  await iterator.return().catch(() => {});
  assert.ok(result.error instanceof stream.DeadlineExceededError);
  assert.equal(readsAfterDeadline, 1);
});

test("a known NaN cancellation reason is not recorded as another failure", async () => {
  const iterator = stream.iterate(
    stream.defer(({ signal }) => ({
      [Symbol.asyncIterator]() {
        return this;
      },
      next() {
        return new Promise((resolve, reject) =>
          signal.addEventListener("abort", () => reject(signal.reason), {
            once: true,
          }),
        );
      },
      return() {
        return { done: true };
      },
    })),
  );
  const next = iterator.next();
  const rejected = assert.rejects(next, Number.isNaN);
  await flush();
  iterator.cancel(NaN);
  await rejected;
  const report = await iterator.closed;
  assert.equal(report.kind, "cancelled");
  assert.deepEqual(report.suppressedErrors, []);
});

test("an iterator return without a done flag is reported as incomplete cleanup", async () => {
  const source = {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      return { value: 1 };
    },
    return() {
      return { value: 2 };
    },
  };
  const iterator = stream.iterate(source);
  await iterator.next();
  const result = await iterator.return().then(
    (value) => ({ value }),
    (error) => ({ error }),
  );
  assert.ok(result.error instanceof stream.IteratorCleanupError);
  assert.equal((await iterator.closed).kind, "failed");
});

test("throw after completion cannot change the completed close report or later return", async () => {
  const iterator = stream.iterate(stream.empty());
  await iterator.next();
  const report = await iterator.closed;
  const error = new Error("after completion");
  await assert.rejects(iterator.throw(error), (thrown) => thrown === error);
  const returned = await iterator.return().then(
    (value) => ({ value }),
    (error) => ({ error }),
  );
  assert.deepEqual(returned.value, { done: true, value: undefined });
  assert.equal((await iterator.closed).kind, "completed");
  assert.equal(await iterator.closed, report);
});

test("scope cleanup remains once-only when a cleanup failure reenters close", async () => {
  const { Scope } = await import("../dist/core/scope.js");
  const failure = new Error("cleanup");
  let finalized = 0;
  let reentered;
  const errors = [];
  const host = {
    scheduler: new VirtualScheduler(),
    fail(error) {
      throw error;
    },
    suppress(error) {
      errors.push(error);
      reentered = root.close();
    },
  };
  const root = new Scope(host);
  const child = new Scope(host, root);
  child.setCleanup(() => {
    throw failure;
  });
  root.finalizers.push(() => {
    finalized++;
  });
  await root.close();
  await reentered;
  assert.equal(finalized, 1);
  assert.deepEqual(errors, [failure]);
});

test("opening a child without reading it does not start its factory or finalizer", async () => {
  let opened = 0;
  let finalized = 0;
  const failure = new Error("could not open sibling");
  const unread = stream
    .defer(() => {
      opened++;
      return [1];
    })
    .pipe(
      stream.finalize(() => {
        finalized++;
      }),
    );
  const broken = {
    [Symbol.asyncIterator]() {
      throw failure;
    },
  };
  await assert.rejects(
    stream.toArray(stream.merge(unread, broken)),
    (error) => error === failure,
  );
  assert.equal(opened, 0);
  assert.equal(finalized, 0);
});

test("from accepts primitive strings and falls back from an absent async iterator method", async () => {
  assert.deepEqual(await stream.toArray(stream.from("ab")), ["a", "b"]);
  const source = {
    [Symbol.asyncIterator]: undefined,
    *[Symbol.iterator]() {
      yield 1;
      yield 2;
    },
  };
  assert.deepEqual(await stream.toArray(stream.from(source)), [1, 2]);
});

test("deadline rejects a value arriving after its budget even before the timer callback runs", async () => {
  const clock = new VirtualScheduler();
  const input = controlled();
  const iterator = iterateWithScheduler(
    stream.from(input.source).pipe(stream.deadline(10)),
    {},
    clock,
  );
  const rejected = assert.rejects(
    iterator.next(),
    stream.DeadlineExceededError,
  );
  await flush();
  clock.time = 11;
  input.reads[0].resolve({ value: 1 });
  await rejected;
  const report = await iterator.closed;
  assert.equal(report.kind, "failed");
  assert.equal(input.returns, 1);
  assert.equal(clock.timers.size, 0);
});

test("chain waits for the previous input's cleanup before opening another input", async () => {
  const cleanup = deferred();
  let opened = false;
  const iterator = stream.iterate(
    stream.chain(
      stream.empty().pipe(stream.finalize(() => cleanup.promise)),
      stream.defer(() => {
        opened = true;
        return [1];
      }),
    ),
  );
  const next = iterator.next();
  await flush();
  const openedBeforeCleanup = opened;
  cleanup.resolve();
  assert.equal((await next).value, 1);
  await iterator.return();
  assert.equal(openedBeforeCleanup, false);
});

test("higher-order operators retain a child's concurrency slot until cleanup completes", async () => {
  for (const operator of [stream.flatten, stream.concat]) {
    const cleanup = deferred();
    let accepted = 0;
    const children = {
      *[Symbol.iterator]() {
        accepted++;
        yield stream.empty().pipe(stream.finalize(() => cleanup.promise));
        accepted++;
        yield [1];
      },
    };
    const iterator = stream.iterate(
      stream.from(children).pipe(operator({ concurrency: 1 })),
    );
    const next = iterator.next();
    await flush();
    const acceptedBeforeCleanup = accepted;
    cleanup.resolve();
    assert.equal((await next).value, 1);
    await iterator.return();
    assert.equal(acceptedBeforeCleanup, 1);
  }
});

test("the external protocol distinguishes missing return from malformed operation results", async () => {
  const withoutReturn = {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      return { value: 1 };
    },
  };
  const iterator = stream.iterate(withoutReturn);
  await iterator.next();
  await iterator.return();
  assert.equal((await iterator.closed).kind, "completed");

  const invalidNext = {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      return null;
    },
  };
  await assert.rejects(stream.toArray(invalidNext), TypeError);

  const invalidReturn = {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      return { value: 1 };
    },
    return() {
      return undefined;
    },
  };
  const invalid = stream.iterate(invalidReturn);
  await invalid.next();
  await assert.rejects(invalid.return(), TypeError);
  assert.equal((await invalid.closed).kind, "failed");
});

test("the completed outcome is sealed before closed observers run", async () => {
  const iterator = stream.iterate(stream.empty());
  const failure = new Error("after close report");
  const afterClosed = iterator.closed.then(() =>
    assert.rejects(iterator.throw(failure), (error) => error === failure),
  );
  await Promise.all([iterator.next(), afterClosed]);
  assert.equal((await iterator.closed).kind, "completed");
  await iterator.return();
});

test("cancellation rejects a pending next promptly even after input exhaustion during finalization", async () => {
  for (const external of [false, true]) {
    const controller = new AbortController();
    const started = deferred();
    const cleanup = deferred();
    const iterator = stream.iterate(
      stream.empty().pipe(
        stream.finalize(() => {
          started.resolve();
          return cleanup.promise;
        }),
      ),
      { signal: controller.signal },
    );
    let delivered;
    const observed = iterator.next().then(
      (value) => {
        delivered = { value };
      },
      (error) => {
        delivered = { error };
      },
    );
    await started.promise;
    await flush();
    if (external) controller.abort("stop during cleanup");
    else iterator.cancel("stop during cleanup");
    await flush();
    const deliveredBeforeCleanup = delivered;
    cleanup.resolve();
    await observed;
    const report = await iterator.closed;
    assert.deepEqual(deliveredBeforeCleanup, { error: "stop during cleanup" });
    assert.equal(report.kind, "cancelled");
    assert.equal((await iterator.next()).done, true);
  }
});

test("cancellation during iterator acquisition closes the acquired input without reading it", async () => {
  const controller = new AbortController();
  const reason = new Error("cancelled during acquisition");
  let reads = 0;
  let returns = 0;
  const source = {
    [Symbol.iterator]() {
      controller.abort(reason);
      return {
        next() {
          reads++;
          return { value: 1 };
        },
        return() {
          returns++;
          return { done: true };
        },
      };
    },
  };
  const iterator = stream.iterate(stream.from(source), {
    signal: controller.signal,
  });
  await assert.rejects(iterator.next(), (error) => error === reason);
  await iterator.closed;
  assert.equal(reads, 0);
  assert.equal(returns, 1);
});
