import { test } from "node:test";
import assert from "node:assert/strict";
import * as s from "../dist/index.js";
import { deferred, flush } from "./helpers.mjs";

const upstreamFailure = new Error("upstream");
const setups = {
  defer: (setup) => s.defer(setup),
  catchError: (setup) =>
    s
      .defer(() => {
        throw upstreamFailure;
      })
      .pipe(
        s.catchError((error, context) => {
          assert.equal(error, upstreamFailure);
          return setup(context);
        }),
      ),
};

for (const [name, setup] of Object.entries(setups)) {
  test(`${name}: no resources are acquired before demand or after an initial abort`, async () => {
    const source = setup(() => assert.fail("setup must not start"));
    assert.deepEqual(await source.pipe(s.take(0)).toArray(), []);
    const reason = new Error("already cancelled");
    const controller = new AbortController();
    controller.abort(reason);
    const iterator = source.iterate({ signal: controller.signal });
    await assert.rejects(iterator.next(), (error) => error === reason);
    assert.equal((await iterator.closed).reason, reason);
  });

  for (const timing of ["before", "during", "at resolution", "after"]) {
    test(`${name}: cancellation ${timing} acquisition releases only owned resources`, async () => {
      const started = deferred(),
        gate = deferred(),
        cleanup = deferred();
      const reason = new Error("cancel setup");
      const resource = { id: 1 };
      const controller = new AbortController();
      let acquisitions = 0,
        releases = 0,
        opened = 0,
        setupSignal;
      const iterator = setup(async ({ signal, acquire }) => {
        setupSignal = signal;
        if (timing === "before") {
          started.resolve();
          await gate.promise;
        }
        await acquire(
          async (acquisitionSignal) => {
            assert.equal(acquisitionSignal, signal);
            acquisitions++;
            if (timing === "during") {
              started.resolve();
              await gate.promise;
            }
            if (timing === "at resolution") {
              controller.abort(reason);
              started.resolve();
            }
            return resource;
          },
          async (owned) => {
            assert.equal(owned, resource);
            releases++;
            await cleanup.promise;
          },
        );
        if (timing === "after") {
          started.resolve();
          await gate.promise;
        }
        return {
          [Symbol.iterator]() {
            opened++;
            return [1][Symbol.iterator]();
          },
        };
      }).iterate({ signal: controller.signal });
      const pending = iterator.next();
      const rejected = assert.rejects(pending, (error) => error === reason);
      await started.promise;
      controller.abort(reason);
      await rejected;
      assert.equal(setupSignal.aborted, true);
      let closed = false;
      void iterator.closed.then(() => {
        closed = true;
      });
      await flush();
      assert.equal(closed, false);
      gate.resolve();
      await flush();
      assert.equal(opened, 0);
      assert.equal(acquisitions, timing === "before" ? 0 : 1);
      assert.equal(releases, timing === "before" ? 0 : 1);
      if (timing !== "before") assert.equal(closed, false);
      cleanup.resolve();
      const report = await iterator.closed;
      assert.equal(report.kind, "cancelled");
      assert.equal(report.reason, reason);
      assert.deepEqual(report.suppressedErrors, []);
      await iterator.return();
      assert.equal(releases, timing === "before" ? 0 : 1);
    });
  }

  test(`${name}: late ordinary sources are not inspected or opened`, async () => {
    const started = deferred(),
      gate = deferred();
    const iterator = setup(async () => {
      started.resolve();
      await gate.promise;
      return {
        get [Symbol.asyncIterator]() {
          assert.fail("must not inspect source");
        },
        get [Symbol.iterator]() {
          assert.fail("must not inspect source");
        },
        get return() {
          assert.fail("must not assume iterator ownership");
        },
      };
    }).iterate();
    const pending = iterator.next();
    await started.promise;
    const reason = new Error("cancel");
    iterator.cancel(reason);
    await assert.rejects(pending, (error) => error === reason);
    gate.resolve();
    assert.deepEqual(await iterator.closed, {
      kind: "cancelled",
      reason,
      suppressedErrors: [],
    });
  });

  for (const ending of ["exhaustion", "take", "return", "read failure"]) {
    test(`${name}: ${ending} closes child reads before releasing resources once`, async () => {
      const events = [],
        childCleanup = deferred();
      const failure = new Error("read failure");
      const source = setup(async ({ acquire }) => {
        const first = await acquire(
          () => 1,
          (value) => {
            events.push(`release ${value}`);
          },
        );
        await acquire(
          () => 2,
          (value) => {
            events.push(`release ${value}`);
          },
        );
        return (async function* () {
          try {
            yield first;
            if (ending === "read failure") throw failure;
          } finally {
            events.push("child cleanup");
            await childCleanup.promise;
            events.push("child closed");
          }
        })();
      });
      const iterator = (
        ending === "take" ? source.pipe(s.take(1)) : source
      ).iterate();
      assert.deepEqual(await iterator.next(), { done: false, value: 1 });
      const finish = ending === "return" ? iterator.return() : iterator.next();
      const result =
        ending === "read failure"
          ? assert.rejects(finish, (error) => error === failure)
          : finish;
      await flush();
      assert.deepEqual(events, ["child cleanup"]);
      childCleanup.resolve();
      await result;
      const report = await iterator.closed;
      assert.equal(
        report.kind,
        ending === "read failure" ? "failed" : "completed",
      );
      assert.deepEqual(events, [
        "child cleanup",
        "child closed",
        "release 2",
        "release 1",
      ]);
      await iterator.return().catch(() => {});
      assert.equal(events.length, 4);
    });
  }

  for (const late of [false, true]) {
    test(`${name}: ${late ? "late" : "initial"} acquisition failure preserves the primary reason`, async () => {
      const started = deferred(),
        gate = deferred();
      const failure = new Error("acquire"),
        cleanupFailure = new Error("release");
      const reason = new Error("cancel");
      let released = 0;
      const iterator = setup(async ({ acquire }) => {
        await acquire(
          () => "first",
          () => {
            released++;
            throw cleanupFailure;
          },
        );
        await acquire(
          async () => {
            started.resolve();
            await gate.promise;
            throw failure;
          },
          () => assert.fail("failed acquisition has no resource"),
        );
        return [];
      }).iterate();
      const pending = iterator.next();
      const rejected = assert.rejects(
        pending,
        (error) => error === (late ? reason : failure),
      );
      await started.promise;
      if (late) iterator.cancel(reason);
      gate.resolve();
      await rejected;
      const report = await iterator.closed;
      assert.equal(report.kind, late ? "cancelled" : "failed");
      assert.equal(report.reason, late ? reason : failure);
      assert.ok(report.suppressedErrors.includes(cleanupFailure));
      assert.equal(report.suppressedErrors.includes(failure), late);
      assert.equal(released, 1);
    });
  }

  test(`${name}: late successful acquisition reports release failure without replacing cancellation`, async () => {
    const started = deferred(),
      gate = deferred();
    const reason = new Error("cancel"),
      failure = new Error("release");
    const iterator = setup(async ({ acquire }) => {
      await acquire(
        async () => {
          started.resolve();
          return gate.promise;
        },
        async () => {
          throw failure;
        },
      );
      return [];
    }).iterate();
    const pending = iterator.next();
    await started.promise;
    iterator.cancel(reason);
    await assert.rejects(pending, (error) => error === reason);
    gate.resolve({});
    assert.deepEqual(await iterator.closed, {
      kind: "cancelled",
      reason,
      suppressedErrors: [failure],
    });
  });

  test(`${name}: setup failure releases resources and remains primary`, async () => {
    const failure = new Error("setup"),
      cleanupFailure = new Error("release");
    let releases = 0;
    const iterator = setup(async ({ acquire }) => {
      await acquire(
        () => ({}),
        () => {
          releases++;
          throw cleanupFailure;
        },
      );
      throw failure;
    }).iterate();
    await assert.rejects(iterator.next(), (error) => error === failure);
    assert.deepEqual(await iterator.closed, {
      kind: "failed",
      reason: failure,
      suppressedErrors: [cleanupFailure],
    });
    assert.equal(releases, 1);
  });

  test(`${name}: contexts cannot acquire after setup and resources are fresh per run`, async () => {
    let saved,
      acquisitions = 0,
      releases = 0;
    const source = setup(async (context) => {
      saved = context;
      const value = await context.acquire(
        () => ++acquisitions,
        () => {
          releases++;
        },
      );
      return [value];
    });
    for (let run = 1; run <= 2; run++) {
      const iterator = source.iterate();
      assert.deepEqual(await iterator.next(), { done: false, value: run });
      await assert.rejects(
        saved.acquire(
          () => assert.fail(),
          () => assert.fail(),
        ),
        TypeError,
      );
      await iterator.return();
      assert.equal(releases, run);
      await assert.rejects(
        saved.acquire(
          () => assert.fail(),
          () => assert.fail(),
        ),
        TypeError,
      );
    }
  });

  test(`${name}: a release-only failure fails terminal consumption`, async () => {
    const failure = new Error("release");
    let released = 0;
    await assert.rejects(
      setup(async ({ acquire }) => {
        await acquire(
          () => undefined,
          async () => {
            released++;
            throw failure;
          },
        );
        return [];
      }).toArray(),
      (error) => error === failure,
    );
    assert.equal(released, 1);
  });
}

test("catchError waits for owned resources to release before recovering acquisition failures", async () => {
  for (const failure of [new Error("acquire"), undefined]) {
    const cleanup = deferred();
    const events = [];
    const iterator = s
      .defer(async ({ acquire }) => {
        await acquire(
          () => ({}),
          async () => {
            events.push("release");
            await cleanup.promise;
          },
        );
        await acquire(
          () => {
            throw failure;
          },
          () => assert.fail(),
        );
        return [];
      })
      .pipe(
        s.catchError((error) => {
          assert.equal(error, failure);
          events.push("recover");
          return [42];
        }),
      )
      .iterate();
    const pending = iterator.next();
    await flush();
    assert.deepEqual(events, ["release"]);
    cleanup.resolve();
    assert.deepEqual(await pending, { done: false, value: 42 });
    await iterator.return();
    assert.deepEqual(events, ["release", "recover"]);
    assert.equal((await iterator.closed).kind, "completed");
  }
});
