import { test } from "node:test";
import assert from "node:assert/strict";
import * as stream from "../dist/index.js";
import { create } from "../dist/core/stream.js";
import { deferred, flush } from "./helpers.mjs";

for (const name of ["flatten", "concat", "flatMap", "concatMap"]) {
  test(`${name} composed delivery preserves Promise identity and drains cleanup after downstream abort`, async () => {
    const controller = new AbortController();
    const cleanup = deferred();
    const reason = new Error("abort in downstream callback");
    const value = Promise.resolve(42);
    let accepted = 0;
    let finalized = false;
    const seen = [];
    const child = create((scope) => {
      let delivered = false;
      scope.setCleanup(async () => {
        await cleanup.promise;
        finalized = true;
      });
      const read = (receive) => {
        if (delivered) receive({ done: true });
        else {
          delivered = true;
          receive({ done: false, value });
        }
      };
      return { read, next: () => new Promise(read) };
    });
    function* outer() {
      accepted++;
      yield child;
      accepted++;
      yield [99];
    }
    const combine = name.endsWith("Map")
      ? stream[name]((source) => source)
      : stream[name]();
    const iterator = stream
      .from(outer())
      .pipe(
        combine,
        stream.tap((item) => {
          seen.push(item);
          controller.abort(reason);
        }),
      )
      .iterate({ signal: controller.signal });
    await assert.rejects(iterator.next(), (error) => error === reason);
    assert.equal(seen.length, 1);
    assert.equal(seen[0], value);
    assert.equal(accepted, 1);
    let closed = false;
    iterator.closed.then(() => {
      closed = true;
    });
    await flush();
    assert.equal(closed, false);
    assert.equal(finalized, false);
    cleanup.resolve();
    const report = await iterator.closed;
    assert.equal(finalized, true);
    assert.equal(report.kind, "cancelled");
    assert.equal(report.reason, reason);
    assert.deepEqual(report.suppressedErrors, []);
  });
}
