import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { fromCallback, broadcast } from "../../dist/index.js";

// JSC's concurrent compiler can retain execution values through JITWorkList.
// Match memory-retention.mjs: keep JIT enabled but finish compilation inline.
if (process.versions.bun && process.env.BUN_JSC_useConcurrentJIT !== "false") {
  const result = spawnSync(
    process.execPath,
    ["--expose-gc", fileURLToPath(import.meta.url), ...process.argv.slice(2)],
    {
      env: { ...process.env, BUN_JSC_useConcurrentJIT: "false" },
      stdio: "inherit",
    },
  );
  if (result.error) throw result.error;
  process.exit(result.status ?? 1);
}

assert.equal(typeof globalThis.gc, "function");
const consumed = [];
const dropped = [];
const queued = [];
const retained = [];
function payload(references) {
  const value = { bytes: new Uint8Array(1024) };
  references.push(new WeakRef(value));
  if (
    (references === consumed && process.argv.includes("--retain-consumed")) ||
    (references === dropped && process.argv.includes("--retain-dropped"))
  )
    retained.push(value);
  return value;
}

async function push(capacity, overflow) {
  let observer;
  let cleaned = 0;
  const buffered = [];
  const iterator = fromCallback(
    (sink) => {
      observer = sink;
      sink.next("ready");
      return () => {
        cleaned++;
      };
    },
    { capacity, overflow },
  ).iterate();
  await iterator.next();
  for (let i = 0; i < 3; i++) {
    observer.next(payload(consumed));
    await iterator.next();
  }
  for (let i = 0; i < capacity + 3; i++) {
    const discard = overflow === "drop-oldest" ? i < 3 : i >= capacity;
    observer.next(payload(discard ? dropped : queued));
    if (!discard) buffered.push(queued.at(-1));
  }
  return {
    checkOpen() {
      assert.equal(observer.signal.aborted, false);
      assert.equal(cleaned, 0);
    },
    async resume() {
      for (const reference of buffered) {
        const result = await iterator.next();
        assert.equal(result.done, false);
        assert.equal(result.value, reference.deref());
      }
      const next = iterator.next();
      observer.next("resumed");
      assert.deepEqual(await next, { done: false, value: "resumed" });
      observer.complete();
      assert.equal((await iterator.next()).done, true);
      assert.equal((await iterator.closed).kind, "completed");
      assert.equal(cleaned, 1);
    },
  };
}

async function fanout(capacity) {
  const consumedCount = Math.floor(capacity / 2);
  const buffered = [];
  let returned = 0;
  const group = broadcast(
    {
      *[Symbol.iterator]() {
        try {
          for (let i = 0; i < capacity; i++) {
            yield payload(i < consumedCount ? consumed : queued);
          }
          for (;;) yield "resumed";
        } finally {
          returned++;
        }
      },
    },
    { capacity, branches: 2 },
  );
  const [fast, slow] = group.branches.map((branch) => branch.iterate());
  for (let i = 0; i < capacity; i++) {
    await fast.next();
    if (i >= consumedCount) buffered.push(queued.at(-1));
  }
  for (let i = 0; i < consumedCount; i++) await slow.next();
  let closed = false;
  void group.closed.then(() => {
    closed = true;
  });
  return {
    checkOpen() {
      assert.equal(closed, false);
      assert.equal(returned, 0);
    },
    async resume() {
      for (const reference of buffered) {
        const result = await slow.next();
        assert.equal(result.done, false);
        assert.equal(result.value, reference.deref());
      }
      assert.deepEqual(await fast.next(), { done: false, value: "resumed" });
      assert.deepEqual(await slow.next(), { done: false, value: "resumed" });
      assert.equal((await group.close()).source.kind, "completed");
      assert.equal(returned, 1);
    },
  };
}

const handles = [];
for (const capacity of [256, 257]) {
  for (const overflow of ["drop-oldest", "drop-newest"])
    handles.push(await push(capacity, overflow));
  handles.push(await fanout(capacity));
}
for (const handle of handles) handle.checkOpen();
for (let i = 0; i < 5; i++) {
  // Collect outside promise reactions, which can still hold the last delivery.
  await new Promise((resolve) => {
    setImmediate(() => {
      globalThis.gc();
      resolve();
    });
  });
}
for (const [name, references] of [
  ["consumed", consumed],
  ["dropped", dropped],
]) {
  assert.ok(references.length > 0);
  assert.equal(
    references.filter((reference) => reference.deref()).length,
    0,
    `${name} payloads retained while queues are open`,
  );
}
assert.ok(queued.length > 0);
assert.ok(queued.every((reference) => reference.deref() !== undefined));
for (const handle of handles) {
  handle.checkOpen();
  await handle.resume();
}
assert.equal(retained.length, 0);
