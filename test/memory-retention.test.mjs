import { test } from "node:test";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";
import { fromTimeline, take, tap } from "../dist/index.js";

test("closed iterators release execution payloads while still retained", async () => {
  await promisify(execFile)(
    process.execPath,
    [
      "--expose-gc",
      fileURLToPath(
        new URL("./fixtures/memory-retention.mjs", import.meta.url),
      ),
    ],
    { timeout: 8_000 },
  );
});

test("timeline input remains replayable after failed and early executions close", async () => {
  const values = [{ id: 1 }, { id: 2 }];
  const source = fromTimeline(
    values.map((value) => ({ at: 0, kind: "value", value })),
  );
  const failure = new Error("downstream failure");
  await assert.rejects(
    source
      .pipe(
        tap(() => {
          throw failure;
        }),
      )
      .toArray(),
    (error) => error === failure,
  );
  assert.deepEqual(await source.toArray(), values);
  assert.deepEqual(await source.pipe(take(1)).toArray(), [values[0]]);
  assert.deepEqual(await source.toArray(), values);
});
