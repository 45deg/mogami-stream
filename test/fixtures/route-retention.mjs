import assert from "node:assert/strict";
import { route } from "../../dist/index.js";

const references = [];
const retained = [];
function payload() {
  const value = { bytes: new Uint8Array(64 * 1024) };
  references.push(new WeakRef(value));
  if (process.argv.includes("--retain-payloads")) retained.push(value);
  return value;
}

async function queuedAndDelivered() {
  const group = route(
    {
      *[Symbol.iterator]() {
        for (let i = 0; i < 4; i++) yield payload();
      },
    },
    { branches: 2, capacity: 4, select: (_value, { index }) => index % 2 },
  );
  const iterator = group.branches[0].iterate();
  while (!(await iterator.next()).done) {}
  group.branches[1].detach();
  await group.closed;
  return { group, iterator };
}

async function blockedCoordinator() {
  const group = route(
    {
      *[Symbol.iterator]() {
        for (;;) yield payload();
      },
    },
    { branches: 2, capacity: 1, select: () => 1 },
  );
  const iterator = group.branches[0].iterate();
  const pending = iterator.next();
  await new Promise((resolve) => setImmediate(resolve));
  await group.close();
  await pending;
  return { group, iterator };
}

const handles = [];
for (let i = 0; i < 10; i++) {
  handles.push(await queuedAndDelivered(), await blockedCoordinator());
}
for (let i = 0; i < 5; i++) {
  await new Promise((resolve) =>
    setImmediate(() => {
      global.gc();
      resolve();
    }),
  );
}
assert.equal(
  references.filter((reference) => reference.deref()).length,
  0,
  "route retained delivered, queued, or coordinator payloads after close",
);
for (const { group, iterator } of handles) {
  assert.equal((await group.closed).source.kind, "completed");
  assert.equal((await iterator.next()).done, true);
}
assert.equal(retained.length, 0);
