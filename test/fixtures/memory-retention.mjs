import assert from "node:assert/strict";
import {
  setImmediate as turn,
  setTimeout as sleep,
} from "node:timers/promises";
import {
  range,
  map,
  dedupe,
  scan,
  defer,
  merge,
  zipLatest,
  flatten,
  concat,
  flatMap,
  concatMap,
  bufferTime,
  fromTimeline,
  tap,
} from "../../dist/index.js";

function fail() {
  throw new Error("expected failure");
}

function source(payload, fails = false) {
  return defer(() => ({
    [Symbol.iterator]() {
      let index = 0;
      return {
        next() {
          if (fails && index === 2) fail();
          return index === 6
            ? { done: true }
            : { done: false, value: payload(index++) };
        },
        return: () => ({ done: true }),
      };
    },
  }));
}

const cases = {
  mapReturn: (payload) => range(0, 100).pipe(map(payload, { concurrency: 8 })),
  dedupeComplete: (payload) => range(0, 4).pipe(map(payload), dedupe()),
  scanComplete: (payload) => range(0, 4).pipe(scan(payload, null)),
  deferComplete: (payload) => defer(() => [payload()]),
  mapCancel: (payload) =>
    range(0, 100).pipe(
      map(
        async () => {
          await turn();
          return payload();
        },
        { concurrency: 8 },
      ),
    ),
  mapFailure: (payload) =>
    range(0, 100).pipe(
      map(
        async (value) => {
          if (value === 0) {
            await turn();
            throw new Error("expected failure");
          }
          return payload();
        },
        { concurrency: 8 },
      ),
    ),
  dedupeUpstreamFailure: (payload) => source(payload, true).pipe(dedupe()),
  dedupeCallbackFailure: (payload) =>
    source(payload).pipe(
      dedupe((value, { index }) => {
        if (index === 2) fail();
        return value;
      }),
    ),
  scanUpstreamFailure: (payload) =>
    source(payload, true).pipe(scan((_acc, value) => value, null)),
  scanCallbackFailure: (payload) =>
    source(payload).pipe(
      scan((_acc, value, { index }) => {
        if (index === 2) fail();
        return value;
      }, null),
    ),
  mergeDownstreamFailure: (payload) =>
    merge(source(payload), source(payload)).pipe(tap(fail)),
  zipLatestDownstreamFailure: (payload) =>
    zipLatest([source(payload), source(payload)]).pipe(tap(fail)),
  zipLatestPartialDownstreamFailure: (payload) =>
    zipLatest([source(payload), source(payload)], { partial: true }).pipe(
      tap(fail),
    ),
  bufferTimeDownstreamFailure: (payload) =>
    defer(() => ({
      [Symbol.asyncIterator]() {
        let index = 0;
        return {
          async next() {
            if (index > 0) await sleep(5);
            return index === 4
              ? { done: true }
              : { done: false, value: payload(index++) };
          },
          return: () => ({ done: true }),
        };
      },
    })).pipe(
      bufferTime(1, { maxItems: 3 }),
      tap(async () => {
        await sleep(15);
        fail();
      }),
    ),
  timelineFailure: (payload) =>
    defer(() =>
      fromTimeline([
        ...Array.from({ length: 6 }, () => ({
          at: 0,
          kind: "value",
          value: payload(),
        })),
        { at: 0, kind: "error", error: new Error("expected failure") },
      ]),
    ),
  timelineDownstreamFailure: (payload) =>
    defer(() =>
      fromTimeline(
        Array.from({ length: 6 }, () => ({
          at: 0,
          kind: "value",
          value: payload(),
        })),
      ),
    ).pipe(tap(fail)),
};

for (const [name, combine] of Object.entries({
  flatten,
  concat,
  flatMap,
  concatMap,
})) {
  cases[`${name}DownstreamFailure`] = (payload) =>
    (name.endsWith("Map")
      ? range(0, 4).pipe(combine(() => source(payload), { concurrency: 4 }))
      : range(0, 4).pipe(
          map(() => source(payload)),
          combine({ concurrency: 4 }),
        )
    ).pipe(tap(fail));
}

async function closeOne(name, create, payload) {
  const iterator = create(payload).iterate();
  if (name === "mapReturn") {
    await iterator.next();
    await turn();
    await iterator.return();
  } else if (name === "mapCancel") {
    const pending = iterator.next();
    // Let non-cooperative mapper work start before cancellation.
    await Promise.resolve();
    iterator.cancel(new Error("expected cancellation"));
    await assert.rejects(pending, /expected cancellation/);
  } else if (name.endsWith("Failure")) {
    await assert.rejects(async () => {
      while (!(await iterator.next()).done) {
        // Upstream and callback failures can follow successful outputs.
      }
    }, /expected failure/);
  } else {
    while (!(await iterator.next()).done) {
      // Discard results so only the closed iterator can retain them.
    }
  }
  await iterator.closed;
  return iterator;
}

async function check(name, create) {
  const references = [];
  const payload = () => {
    const value = { bytes: new Uint8Array(64 * 1024) };
    references.push(new WeakRef(value));
    return value;
  };
  const iterators = [];
  for (let i = 0; i < 10; i++) {
    iterators.push(await closeOne(name, create, payload));
  }
  assert.ok(references.length > 0, `${name}: must create payloads`);
  for (let i = 0; i < 5; i++) {
    // WeakRef targets stay alive until the end of the current job.
    await turn();
    global.gc();
  }
  const retained = references.filter((reference) => reference.deref()).length;
  assert.equal(retained, 0, `${name}: payloads retained after closed`);
  // Use the iterators after GC to keep them strongly reachable during the check.
  for (const iterator of iterators) {
    assert.equal((await iterator.next()).done, true);
  }
}

for (const [name, create] of Object.entries(cases)) await check(name, create);
