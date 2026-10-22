import assert from "node:assert/strict";
import { fork } from "../../dist/index.js";

const references = [];
const retained = [];
function payload() {
  const value = { bytes: new Uint8Array(64 * 1024) };
  references.push(new WeakRef(value));
  if (process.argv.includes("--retain-payloads")) retained.push(value);
  return value;
}

async function exercise() {
  const group = fork(
    {
      *[Symbol.iterator]() {
        for (let i = 0; i < 4; i++) yield payload();
      },
    },
    { branches: 2, capacity: 5 },
  );
  const iterator = group.branches[0].iterate();
  while (!(await iterator.next()).done) {}
  group.branches[1].detach();
  await group.closed;
  return { group, iterator };
}
const handles = [];
for (let i = 0; i < 10; i++) handles.push(await exercise());
for (let i = 0; i < 5; i++) {
  await new Promise((resolve) =>
    setImmediate(() => {
      global.gc();
      resolve();
    }),
  );
}
assert.equal(
  references.filter((ref) => ref.deref()).length,
  0,
  "fork retained delivered or detached payloads",
);
for (const { group, iterator } of handles) {
  assert.equal((await group.closed).source.kind, "completed");
  assert.equal((await iterator.next()).done, true);
}
assert.equal(retained.length, 0);
