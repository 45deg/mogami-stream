import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import ts from "typescript";
import { finalize, from, map, TimeoutError } from "../dist/index.js";
import { deferred, flush } from "./helpers.mjs";

// Execute the published example itself, while retaining Node 22 compatibility.
const source = await readFile(
  new URL("../docs/examples/terminal-report.ts", import.meta.url),
  "utf8",
);
const { outputText } = ts.transpileModule(source, {
  compilerOptions: {
    target: ts.ScriptTarget.ES2022,
    module: ts.ModuleKind.ESNext,
  },
});
const executable = outputText.replaceAll(
  '"mogami-stream"',
  JSON.stringify(new URL("../dist/index.js", import.meta.url).href),
);
const { collectWithReport, nonCooperativeTimeout } = await import(
  `data:text/javascript;base64,${Buffer.from(executable).toString("base64")}`
);

test("collection recipe retains values and waits for successful cleanup", async () => {
  const cleanup = deferred();
  const started = deferred();
  let settled = false;
  const result = collectWithReport(
    from([1, 2]).pipe(
      finalize(() => {
        started.resolve();
        return cleanup.promise;
      }),
    ),
  ).then((value) => {
    settled = true;
    return value;
  });
  await started.promise;
  assert.equal(settled, false);
  cleanup.resolve();
  assert.deepEqual(await result, {
    status: "fulfilled",
    value: [1, 2],
    report: { kind: "completed", suppressedErrors: [] },
  });
});

for (const primary of [new Error("processing failed"), undefined]) {
  test(`collection recipe preserves ${String(primary)} and awaits secondary failure`, async () => {
    const cleanup = deferred();
    const started = deferred();
    const secondary = new Error("cleanup failed");
    let settled = false;
    const result = collectWithReport(
      from([1]).pipe(
        map(() => {
          throw primary;
        }),
        finalize(() => {
          started.resolve();
          return cleanup.promise;
        }),
      ),
    ).then((value) => {
      settled = true;
      return value;
    });
    await started.promise;
    await flush();
    assert.equal(settled, false);
    cleanup.reject(secondary);
    assert.deepEqual(await result, {
      status: "rejected",
      reason: primary,
      report: {
        kind: "failed",
        reason: primary,
        suppressedErrors: [secondary],
      },
    });
  });
}

test("collection recipe exposes cleanup-only failure as primary", async () => {
  const failure = new Error("cleanup failed");
  const result = await collectWithReport(
    from([1]).pipe(
      finalize(() => {
        throw failure;
      }),
    ),
  );
  assert.deepEqual(result, {
    status: "rejected",
    reason: failure,
    report: { kind: "failed", reason: failure, suppressedErrors: [] },
  });
});

test("collection recipe waits for noncooperative work after external cancellation", async () => {
  const work = deferred();
  const started = deferred();
  const controller = new AbortController();
  const reason = new Error("cancelled");
  const lateError = new Error("late work failure");
  let cleanups = 0;
  let settled = false;
  const result = collectWithReport(
    from([1]).pipe(
      map(() => {
        started.resolve();
        return work.promise;
      }),
      finalize(() => {
        cleanups++;
      }),
    ),
    { signal: controller.signal },
  ).then((value) => {
    settled = true;
    return value;
  });
  await started.promise;
  controller.abort(reason);
  await flush();
  assert.equal(settled, false);
  assert.equal(cleanups, 0);
  work.reject(lateError);
  assert.deepEqual(await result, {
    status: "rejected",
    reason,
    report: { kind: "cancelled", reason, suppressedErrors: [lateError] },
  });
  assert.equal(cleanups, 1);
});

test("collection recipe does not open a source with an already aborted signal", async () => {
  const controller = new AbortController();
  const reason = new Error("cancelled before starting");
  controller.abort(reason);
  const result = await collectWithReport(
    {
      [Symbol.iterator]() {
        assert.fail("source must not open");
      },
    },
    { signal: controller.signal },
  );
  assert.deepEqual(result, {
    status: "rejected",
    reason,
    report: { kind: "cancelled", reason, suppressedErrors: [] },
  });
});

test("timeout recipe observes the stop before work settles and reports both later failures", async () => {
  const { notification, cleanupPending, report } =
    await nonCooperativeTimeout();
  assert.equal(notification.status, "rejected");
  assert.ok(notification.reason instanceof TimeoutError);
  assert.equal(cleanupPending, true);
  assert.equal(report.kind, "failed");
  assert.equal(report.reason, notification.reason);
  assert.deepEqual(
    report.suppressedErrors.map((error) => error.message),
    ["work failed after timeout", "cleanup failed"],
  );
});
