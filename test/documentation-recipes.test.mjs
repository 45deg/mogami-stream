import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import ts from "typescript";
import { flush } from "./helpers.mjs";

async function example(name) {
  const source = await readFile(
    new URL(`../docs/examples/${name}.ts`, import.meta.url),
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
  return import(
    `data:text/javascript;base64,${Buffer.from(executable).toString("base64")}`
  );
}

test("lifecycle example reads the close report after processing failure", async (t) => {
  const failure = new Error("fetch failed");
  t.mock.method(globalThis, "fetch", async () => {
    throw failure;
  });
  const logs = t.mock.method(console, "log", () => {});
  const errors = t.mock.method(console, "error", () => {});
  const { readOne } = await example("lifecycle");
  await assert.rejects(
    readOne(new AbortController().signal),
    (error) => error === failure,
  );
  assert.equal(logs.mock.callCount(), 1);
  assert.equal(errors.mock.calls[0].arguments[0], failure);
});

test("lifecycle example reports cleanup-only failure and cancels HTTP error bodies", async (t) => {
  const failure = new Error("finalizer failed");
  t.mock.method(globalThis, "fetch", async () => new Response("ok"));
  const log = t.mock.method(console, "log", () => {
    throw failure;
  });
  const errors = t.mock.method(console, "error", () => {});
  const { readOne } = await example("lifecycle");
  await assert.rejects(
    readOne(new AbortController().signal),
    (error) => error === failure,
  );
  assert.equal(errors.mock.calls[0].arguments[0], failure);
  log.mock.mockImplementation(() => {});
  let cancelled = false;
  globalThis.fetch = async () =>
    new Response(
      new ReadableStream({
        cancel() {
          cancelled = true;
        },
      }),
      { status: 503 },
    );
  await assert.rejects(readOne(new AbortController().signal), /HTTP 503/);
  assert.equal(cancelled, true);
});

test("timeline example assertions execute", async () => {
  await example("testing");
});

test("value examples isolate mutable state and preserve Promise items", async (t) => {
  t.mock.method(console, "log", () => {});
  const { totals, pending, preserved, resolved } =
    await example("value-semantics");
  assert.equal(preserved[0], pending);
  assert.deepEqual(resolved, [2]);
  assert.deepEqual(await Promise.all([totals.toArray(), totals.toArray()]), [
    [1, 3],
    [1, 3],
  ]);
});

test("quick start fetches and consumes its URLs", async (t) => {
  const fetch = t.mock.method(globalThis, "fetch", async (url, { signal }) => {
    assert.ok(signal instanceof AbortSignal);
    return new Response(url);
  });
  const log = t.mock.method(console, "log", () => {});
  await example("quick-start");
  assert.equal(fetch.mock.callCount(), 2);
  assert.deepEqual([...log.mock.calls[0].arguments[0]].sort(), [
    "https://example.com/a",
    "https://example.com/b",
  ]);
});

test("WebSocket adapter delivers messages, completes, and removes listeners", async (t) => {
  const { websocketMessages } = await example("events");
  const socket = new EventTarget();
  const removed = t.mock.method(socket, "removeEventListener");
  const iterator = websocketMessages(socket, new AbortController().signal);
  const next = iterator.next();
  await flush();
  socket.dispatchEvent(new MessageEvent("message", { data: "hello" }));
  assert.equal((await next).value.data, "hello");
  socket.dispatchEvent(new Event("close"));
  assert.equal((await iterator.next()).done, true);
  assert.equal((await iterator.closed).kind, "completed");
  assert.deepEqual(removed.mock.calls.map((call) => call.arguments[0]).sort(), [
    "close",
    "error",
    "message",
  ]);
});

test("SSE usage closes its connection and removes listeners on cancellation", async (t) => {
  let connection;
  class FakeEventSource extends EventTarget {
    closed = false;
    constructor() {
      super();
      connection = this;
    }
    close() {
      this.closed = true;
    }
  }
  const previous = Object.getOwnPropertyDescriptor(globalThis, "EventSource");
  Object.defineProperty(globalThis, "EventSource", {
    configurable: true,
    value: FakeEventSource,
  });
  t.after(() => {
    if (previous) Object.defineProperty(globalThis, "EventSource", previous);
    else delete globalThis.EventSource;
  });
  const log = t.mock.method(console, "log", () => {});
  const { receiveEvents } = await example("events");
  const controller = new AbortController();
  const failure = new Error("stop");
  const running = receiveEvents(
    "https://example.com/events",
    controller.signal,
  );
  const rejected = assert.rejects(running, (error) => error === failure);
  const removed = t.mock.method(connection, "removeEventListener");
  await flush();
  connection.dispatchEvent(new MessageEvent("message", { data: "hello" }));
  await flush();
  controller.abort(failure);
  await rejected;
  assert.equal(log.mock.calls[0].arguments[0], "hello");
  assert.equal(connection.closed, true);
  assert.equal(removed.mock.calls[0].arguments[0], "message");
});

test("cursor example opens and closes a fresh cursor for each execution", async () => {
  const { readFirstRows } = await example("resources");
  let opened = 0;
  let closed = 0;
  const result = await readFirstRows(async ({ signal }) => {
    assert.ok(signal instanceof AbortSignal);
    opened++;
    return {
      async *rows() {
        for (let i = 0; i < 20; i++) yield i;
      },
      async close() {
        closed++;
      },
    };
  }, new AbortController().signal);
  const expected = Array.from({ length: 10 }, (_, i) => i);
  assert.deepEqual(result, { runA: expected, runB: expected });
  assert.equal(opened, 2);
  assert.equal(closed, 2);
});
