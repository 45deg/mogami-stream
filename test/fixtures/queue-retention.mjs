import assert from "node:assert/strict";
import { fromCallback, broadcast, route } from "../../dist/index.js";
assert.equal(typeof globalThis.gc, "function");
const handles = [];
const references = [];
const payload = () => {
  const value = { bytes: new Uint8Array(64 * 1024) };
  references.push(new WeakRef(value));
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
if (globalThis.gc) {
  for (let i = 0; i < 5; i++) {
    await new Promise((resolve) => setImmediate(resolve));
    globalThis.gc();
  }
  assert.equal(references.filter((ref) => ref.deref()).length, 0);
}
assert.equal(handles.length, 24);
console.log(
  "capacity boundaries and close passed; WeakRef release checked:",
  Boolean(globalThis.gc),
);
