import { getEventListeners } from "node:events";
import test from "node:test";
import assert from "node:assert/strict";
import { defer, iterate, of } from "../dist/index.js";

test("execution keeps its initial signal when options are reassigned", async () => {
  const original = new AbortController();
  const replacement = new AbortController();
  const reason = new Error("original abort");
  const options = { signal: original.signal };
  const iterator = iterate(of(1), options);

  options.signal = replacement.signal;
  replacement.abort(new Error("replacement abort"));
  assert.equal(getEventListeners(original.signal, "abort").length, 1);
  assert.equal(getEventListeners(replacement.signal, "abort").length, 0);

  original.abort(reason);
  await assert.rejects(iterator.next(), (error) => error === reason);
  assert.equal((await iterator.closed).reason, reason);
  assert.equal(getEventListeners(original.signal, "abort").length, 0);
});

for (const ending of ["completed", "failed", "cancelled"]) {
  test(`${ending} execution removes the initial signal listener`, async () => {
    const original = new AbortController();
    const replacement = new AbortController();
    const reason = new Error(ending);
    const options = { signal: original.signal };
    const source =
      ending === "failed"
        ? defer(() => {
            throw reason;
          })
        : of(1);
    const iterator = iterate(source, options);
    options.signal = replacement.signal;

    if (ending === "completed") {
      assert.equal((await iterator.next()).value, 1);
      assert.equal((await iterator.next()).done, true);
    } else if (ending === "failed") {
      await assert.rejects(iterator.next(), (error) => error === reason);
    } else {
      iterator.cancel(reason);
      await assert.rejects(iterator.next(), (error) => error === reason);
    }

    assert.equal((await iterator.closed).kind, ending);
    assert.equal(getEventListeners(original.signal, "abort").length, 0);
    assert.equal(getEventListeners(replacement.signal, "abort").length, 0);
  });
}

test("signal-free and initially aborted executions keep their behavior", async () => {
  const withoutSignal = iterate(of(1));
  assert.equal((await withoutSignal.next()).value, 1);
  assert.equal((await withoutSignal.next()).done, true);

  const original = new AbortController();
  const replacement = new AbortController();
  const reason = new Error("already aborted");
  let opened = false;
  original.abort(reason);
  const options = { signal: original.signal };
  const iterator = iterate(
    defer(() => {
      opened = true;
      return of(1);
    }),
    options,
  );
  options.signal = replacement.signal;

  await assert.rejects(iterator.next(), (error) => error === reason);
  assert.equal(opened, false);
  assert.equal((await iterator.closed).reason, reason);
  assert.equal(getEventListeners(original.signal, "abort").length, 0);
});
