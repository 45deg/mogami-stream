import assert from "node:assert/strict";
import { createHook } from "node:async_hooks";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

if (process.argv.length > 3)
  throw new Error(
    "Usage: node benchmarks/profile-sync.mjs [path/to/dist/index.js]",
  );
const api = await import(
  process.argv[2]
    ? pathToFileURL(resolve(process.argv[2])).href
    : new URL("../dist/index.js", import.meta.url).href
);
const inputs = Array.from({ length: 10_000 }, (_, id) =>
  JSON.stringify({ id, amount: (id % 100) * 7, active: id % 3 !== 0 }),
);
const run = () =>
  api
    .from(inputs)
    .pipe(
      api.map(JSON.parse),
      api.filter((record) => record.active),
      api.map((record) => ({ id: record.id, value: record.amount * 2 })),
    )
    .toArray();

// Warm up without hooks; --cpu-prof captures these repeated library executions.
for (let round = 0; round < 80; round++) await run();
let promiseInitializations = 0;
const hook = createHook({
  init(_id, type) {
    if (type === "PROMISE") promiseInitializations++;
  },
});
let output;
try {
  hook.enable();
  output = await run();
} finally {
  hook.disable();
}
assert.equal(output.length, 6666);
console.log(
  JSON.stringify(
    {
      inputs: inputs.length,
      outputs: output.length,
      promiseInitializations,
      note: "Separate diagnostic pass; async_hooks must not be enabled for benchmark timings.",
    },
    null,
    2,
  ),
);
