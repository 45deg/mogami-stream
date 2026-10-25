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

test("memory retention check rejects a closure-held payload leak", async () => {
  await assert.rejects(
    promisify(execFile)(
      process.execPath,
      [
        "--expose-gc",
        fileURLToPath(
          new URL("./fixtures/memory-retention.mjs", import.meta.url),
        ),
        "--retain-payloads",
      ],
      { timeout: 8_000 },
    ),
    (error) =>
      error.code === 1 &&
      /mapReturn: payloads retained after closed/.test(error.stderr),
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

test("queue storage releases payloads at small and large capacity boundaries", async () => {
  await promisify(execFile)(
    process.execPath,
    [
      "--expose-gc",
      fileURLToPath(new URL("./fixtures/queue-retention.mjs", import.meta.url)),
    ],
    { timeout: 8_000 },
  );
});

test("queue retention check rejects explicitly retained payloads", async () => {
  await assert.rejects(
    promisify(execFile)(
      process.execPath,
      [
        "--expose-gc",
        fileURLToPath(
          new URL("./fixtures/queue-retention.mjs", import.meta.url),
        ),
        "--retain-payloads",
      ],
      { timeout: 8_000 },
    ),
    (error) =>
      error.code === 1 &&
      /queue payloads retained after close/.test(error.stderr),
  );
});
