import { test } from "node:test";
import assert from "node:assert/strict";
import * as stream from "../dist/index.js";
import { Scope } from "../dist/core/scope.js";
import { controlled, flush, VirtualScheduler } from "./helpers.mjs";

test("an implicit stop reason is shared across every descendant scope", async () => {
  const errors = [];
  const root = new Scope({
    scheduler: new VirtualScheduler(),
    checkpoint: () => undefined,
    fail: (error) => errors.push(error),
    suppress: (error) => errors.push(error),
  });
  const child = new Scope(root.host, root);
  const grandchild = new Scope(root.host, child);
  root.stop(undefined);
  assert.equal(root.signal.reason.name, "AbortError");
  assert.equal(child.signal.reason, root.signal.reason);
  assert.equal(grandchild.signal.reason, root.signal.reason);
  await root.close();
  assert.deepEqual(errors, []);
});

test("zip completion does not mistake an abort-aware sibling's stop response for failure", async () => {
  const shortest = controlled();
  let started = false;
  let returned = 0;
  const sibling = stream.defer(({ signal }) => ({
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      started = true;
      return new Promise((resolve, reject) =>
        signal.addEventListener("abort", () => reject(signal.reason), {
          once: true,
        }),
      );
    },
    return() {
      returned++;
      return { done: true };
    },
  }));
  const iterator = stream.iterate(stream.zip(shortest.source, sibling));
  const reading = iterator.next();
  await flush();
  assert.equal(started, true);
  shortest.reads[0].resolve({ done: true });
  assert.equal((await reading).done, true);
  const report = await iterator.closed;
  assert.equal(report.kind, "completed");
  assert.deepEqual(report.suppressedErrors, []);
  assert.equal(returned, 1);
  assert.equal(shortest.returns, 0);
});
