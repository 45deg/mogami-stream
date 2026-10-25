import assert from "node:assert/strict";
import { fromCallback, broadcast, route } from "../../dist/index.js";
assert.equal(typeof globalThis.gc, "function");
const handles = [];
const references = [];
const retained = [];
const payload = () => {
  const value = { bytes: new Uint8Array(64 * 1024) };
  references.push(new WeakRef(value));
  if (process.argv.includes("--retain-payloads")) retained.push(value);
  return value;
};
for (const capacity of [1, 16, 256, 257, 1024, Number.MAX_SAFE_INTEGER]) {
  for (const overflow of ["drop-oldest", "drop-newest"]) {
    let observer;
    const iterator = fromCallback(
      (sink) => {
        observer = sink;
        // Large-capacity checks use three occupied slots and never preallocate.
        for (let i = 0; i < Math.min(capacity + 1, 4); i++)
          sink.next(payload());
      },
      { capacity, overflow },
    ).iterate();
    await iterator.next();
    await iterator.return();
    handles.push({ iterator, observer });
  }
  for (const kind of ["broadcast", "route"]) {
    const options = { capacity, branches: 1, select: () => 0 };
    const group = (kind === "broadcast" ? broadcast : route)(
      {
        *[Symbol.iterator]() {
          for (let i = 0; i < 4; i++) yield payload();
        },
      },
      options,
    );
    const iterator = group.branches[0].iterate();
    while (!(await iterator.next()).done) {}
    await group.closed;
    handles.push({ group, iterator });
  }
}
assert.ok(references.length > 0);
for (let i = 0; i < 5; i++) {
  // Collect outside promise reactions, which can still hold the last delivery.
  await new Promise((resolve) => {
    setImmediate(() => {
      globalThis.gc();
      resolve();
    });
  });
}
assert.equal(
  references.filter((ref) => ref.deref()).length,
  0,
  "queue payloads retained after close",
);
for (const { iterator, observer, group } of handles) {
  assert.equal((await iterator.next()).done, true);
  if (observer) assert.equal(observer.signal.aborted, true);
  if (group) assert.equal((await group.closed).source.kind, "completed");
}
assert.equal(retained.length, 0);
assert.equal(handles.length, 24);
console.log(
  "capacity boundaries and close passed; WeakRef release checked:",
  Boolean(globalThis.gc),
);
