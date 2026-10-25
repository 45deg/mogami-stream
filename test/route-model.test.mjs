import { test } from "node:test";
import assert from "node:assert/strict";
import * as core from "../dist/core/index.js";
import * as fluent from "../dist/index.js";
import { deferred, flush } from "./helpers.mjs";
import { forSeeds } from "./fuzz-helpers.mjs";

const unavailable = Symbol("unavailable destination");
const done = { done: true, value: undefined };
const completed = () => ({ kind: "completed", suppressedErrors: [] });
const stopped = (kind, reason) => ({ kind, reason, suppressedErrors: [] });

// This model uses unbounded arrays and explicit input/classification steps;
// it shares neither the hub's queue representation nor its scheduling code.
class RouteModel {
  constructor(count, capacity, backup, fallback, predicates) {
    Object.assign(this, { count, capacity, backup, fallback, predicates });
    this.branches = Array.from({ length: count }, () => ({
      queue: [],
      pending: false,
      notified: false,
      output: [],
    }));
    this.reads = 0;
    this.calls = [];
    this.started = false;
    this.phase = "running";
    this.nextIndex = 0;
    this.suppressed = [];
  }
  end(index, report) {
    const branch = this.branches[index];
    if (branch.report) return;
    branch.report = report;
    branch.queue = [];
    if (branch.pending) this.finishRead(branch);
  }
  finishRead(branch) {
    branch.pending = false;
    if (branch.queue.length) {
      branch.output.push({
        result: { done: false, value: branch.queue.shift() },
      });
    } else if (!branch.notified && branch.report.kind !== "completed") {
      branch.notified = true;
      branch.output.push({ error: branch.report.reason });
    } else branch.output.push({ result: done });
  }
  pull(index) {
    const branch = this.branches[index];
    branch.pending = true;
    if (branch.queue.length || branch.report) this.finishRead(branch);
  }
  depart(index, kind, reason) {
    if (this.branches[index].report) return;
    this.end(index, kind === "completed" ? completed() : stopped(kind, reason));
    if (this.branches.every((b) => b.report) && this.phase === "running")
      this.stop(kind === "completed" ? "completed" : "cancelled", reason);
  }
  stop(kind, reason) {
    if (this.phase === "stopping" || this.groupReport) return;
    this.phase = "stopping";
    this.held = undefined;
    this.kind ??= kind;
    if (this.kind !== "completed") this.reason ??= reason;
    for (let i = 0; i < this.count; i++)
      this.end(i, kind === "completed" ? completed() : stopped(kind, reason));
  }
  fail(error) {
    if (this.kind === "failed" || this.kind === "cancelled") {
      if (error !== this.reason && !this.suppressed.includes(error))
        this.suppressed.push(error);
      return;
    }
    this.kind = "failed";
    this.reason = error;
    this.phase = "stopping";
    this.held = undefined;
    for (let i = 0; i < this.count; i++) this.end(i, stopped("failed", error));
  }
  input(value, error) {
    this.reading = false;
    if (error) this.fail(error);
    else if (this.phase === "running") {
      if (value === undefined) {
        this.phase = "draining";
      } else {
        this.classifying = { value, predicate: 0, index: this.nextIndex++ };
        this.call();
      }
    }
  }
  call() {
    const { value, predicate, index } = this.classifying;
    this.calls.push([value.id, index, this.predicates ? predicate : -1]);
  }
  classify(error) {
    const current = this.classifying;
    if (error) {
      this.classifying = undefined;
      this.fail(error);
    } else if (this.phase !== "running") this.classifying = undefined;
    else if (
      this.predicates &&
      current.value.target !== current.predicate &&
      current.predicate + 1 < this.count
    ) {
      current.predicate++;
      this.call();
    } else {
      this.classifying = undefined;
      const target =
        this.predicates && current.value.target !== current.predicate
          ? -1
          : current.value.target;
      this.held = { value: current.value, target, redirected: false };
    }
  }
  advance() {
    for (let i = 0; i < this.count; i++) {
      const branch = this.branches[i];
      if (this.phase === "draining" && !branch.queue.length)
        this.end(i, completed());
    }
    if (this.held) {
      const item = this.held;
      if (item.target < 0 || this.branches[item.target].report) {
        if (!item.redirected) {
          item.target = this.backup;
          item.redirected = true;
        }
        if (item.target < 0 || this.branches[item.target].report) {
          this.held = undefined;
          if (this.fallback === "error") this.fail(unavailable);
        }
      }
      if (this.held) {
        const branch = this.branches[item.target];
        if (branch.pending || branch.queue.length < this.capacity) {
          branch.queue.push(item.value);
          this.held = undefined;
          if (branch.pending) this.finishRead(branch);
        }
      }
    }
    if (
      this.phase === "running" &&
      !this.reading &&
      !this.classifying &&
      !this.held &&
      this.branches.some((b) => b.pending && !b.report)
    ) {
      this.reading = true;
      this.started = true;
      this.reads++;
    }
    if (
      this.phase !== "running" &&
      !this.reading &&
      !this.classifying &&
      (!this.started || this.cleanupSettled) &&
      !this.sourceReport
    ) {
      this.kind ??= "completed";
      this.sourceReport =
        this.kind === "completed"
          ? completed()
          : stopped(this.kind, this.reason);
      this.sourceReport.suppressedErrors = [...this.suppressed];
    }
    if (this.sourceReport && this.branches.every((b) => b.report))
      this.groupReport = {
        source: this.sourceReport,
        branches: this.branches.map((b) => b.report),
      };
  }
}

function observe(promise) {
  const state = { settled: false };
  void promise.then(
    (result) => Object.assign(state, { settled: true, result }),
    (error) => Object.assign(state, { settled: true, error }),
  );
  return state;
}
function canonical(report) {
  if (!report) return report;
  return {
    ...report,
    ...(report.kind === "failed" || report.kind === "cancelled"
      ? {
          reason:
            report.reason instanceof core.RouteError
              ? unavailable
              : report.reason,
        }
      : {}),
  };
}

async function scenario(api, pick) {
  const count = 3;
  const capacity = 1 + pick(3);
  const backup = pick(count + 1) - 1;
  const fallback = pick(2) ? "error" : "ignore";
  const predicates = Boolean(pick(2));
  const model = new RouteModel(count, capacity, backup, fallback, predicates);
  const reads = [];
  const calls = [];
  const gates = [];
  const cleanup = deferred();
  const controller = new AbortController();
  const errors = Array.from({ length: 4 }, (_, i) => new Error(`reason ${i}`));
  let acquired = 0;
  let returned = 0;
  let finalized = 0;
  let readIndex = 0;
  let gateIndex = 0;
  let valueIndex = 0;
  const source = {
    [Symbol.asyncIterator]() {
      acquired++;
      return this;
    },
    next() {
      const gate = deferred();
      reads.push(gate);
      return gate.promise;
    },
    return() {
      returned++;
      return Promise.resolve(done);
    },
  };
  const choose = (value, { index }, predicate = -1) => {
    calls.push([value.id, index, predicate]);
    const gate = deferred();
    gates.push({ ...gate, value, predicate });
    return gate.promise;
  };
  const group = api.route(
    api.from(source).pipe(
      api.finalize(() => {
        finalized++;
        return cleanup.promise;
      }),
    ),
    {
      branches: predicates
        ? Object.fromEntries(
            Array.from({ length: count }, (_, i) => [
              String(i),
              (value, context) => choose(value, context, i),
            ]),
          )
        : count,
      ...(predicates ? {} : { select: choose }),
      capacity,
      ...(backup < 0 ? {} : { default: predicates ? String(backup) : backup }),
      fallback,
      signal: controller.signal,
    },
  );
  const branches = Object.values(group.branches);
  const iterators = branches.map((branch) => branch.iterate());
  const outputs = branches.map(() => []);
  const reports = branches.map((branch) => observe(branch.closed));
  const closed = observe(group.closed);
  const saved = new Map();
  const trace = [];

  async function check() {
    model.advance();
    await flush();
    assert.equal(reads.length, model.reads, "source demand");
    assert.deepEqual(calls, model.calls, "classification order/index");
    const delivered = [];
    for (let i = 0; i < count; i++) {
      const expected = model.branches[i];
      const actual = outputs[i]
        .filter((entry) => entry.settled)
        .map((entry) =>
          "error" in entry
            ? {
                error:
                  entry.error instanceof core.RouteError
                    ? unavailable
                    : entry.error,
              }
            : { result: entry.result },
        );
      assert.deepEqual(actual, expected.output, `branch ${i} delivery`);
      assert.equal(
        reports[i].settled,
        Boolean(expected.report),
        `branch ${i} completion`,
      );
      if (expected.report) {
        assert.deepEqual(canonical(reports[i].result), expected.report);
        if (!saved.has(i))
          saved.set(i, {
            ...canonical(reports[i].result),
            suppressedErrors: [...reports[i].result.suppressedErrors],
          });
        assert.deepEqual(canonical(reports[i].result), saved.get(i));
        // Compare the report itself as well as a snapshot across later failures.
        assert.equal(reports[i].result, await branches[i].closed);
        assert.deepEqual(canonical(reports[i].result), expected.report);
      }
      delivered.push(
        ...actual.flatMap((entry) =>
          entry.result?.done === false ? [entry.result.value.id] : [],
        ),
      );
      assert.ok(expected.queue.length <= capacity);
    }
    assert.equal(
      new Set(delivered).size,
      delivered.length,
      "no duplicate delivery",
    );
    const buffered = model.branches.reduce((sum, b) => sum + b.queue.length, 0);
    assert.ok(
      buffered +
        Number(Boolean(model.reading || model.classifying || model.held)) <=
        count * capacity + 1,
    );
    assert.equal(
      closed.settled,
      Boolean(model.groupReport),
      "shared cleanup completion",
    );
    if (closed.settled) {
      assert.deepEqual(
        {
          source: canonical(closed.result.source),
          branches: Object.values(closed.result.branches).map(canonical),
        },
        model.groupReport,
      );
    }
    assert.ok(acquired <= 1 && returned <= 1 && finalized <= 1);
  }
  async function step(op, index = 0, error) {
    trace.push([op, index, error?.message]);
    switch (op) {
      case "pull":
        if (model.branches[index].pending) return;
        model.pull(index);
        outputs[index].push(observe(iterators[index].next()));
        break;
      case "input":
        if (!model.reading) return;
        {
          const value = {
            id: valueIndex++,
            target: pick(predicates ? count + 1 : count),
          };
          model.input(value, error);
          if (error) reads[readIndex++].reject(error);
          else reads[readIndex++].resolve({ done: false, value });
        }
        break;
      case "eof":
        if (!model.reading) return;
        model.input();
        reads[readIndex++].resolve(done);
        break;
      case "classify":
        if (!model.classifying) return;
        {
          const gate = gates[gateIndex++];
          model.classify(error);
          if (error) gate.reject(error);
          else
            gate.resolve(
              predicates
                ? gate.value.target === gate.predicate
                : gate.value.target,
            );
        }
        break;
      case "detach":
        model.depart(index, "completed");
        branches[index].detach();
        break;
      case "local":
        model.depart(index, "cancelled", error);
        branches[index].cancel(error);
        break;
      case "cancel":
        model.stop("cancelled", error);
        group.cancel(error);
        break;
      case "abort":
        if (controller.signal.aborted) return;
        model.stop("cancelled", error);
        controller.abort(error);
        break;
      case "close":
        model.stop("completed");
        void group.close();
        break;
      case "cleanup":
        if (model.cleanupSettled) return;
        model.cleanupSettled = true;
        if (error) {
          model.fail(error);
          cleanup.reject(error);
        } else cleanup.resolve();
        break;
    }
    await check();
  }
  try {
    // Keep one consumer waiting while other destinations fill their queues.
    await step("pull", count - 1);
    for (let i = 0; i < 60; i++) {
      const ops = ["pull", "pull", "input", "input", "classify", "classify"];
      if (i > 15) ops.push("detach", "local");
      if (i > 35) ops.push("cancel", "abort", "close", "eof");
      const op = ops[pick(ops.length)];
      await step(
        op,
        pick(count),
        ["local", "cancel", "abort"].includes(op) ||
          (["input", "classify"].includes(op) && i > 25 && pick(9) === 0)
          ? errors[pick(errors.length)]
          : undefined,
      );
    }
    await step("close");
    if (model.reading) await step("input", 0, pick(2) ? errors[2] : undefined);
    if (model.classifying)
      await step("classify", 0, pick(2) ? errors[2] : undefined);
    await step("cleanup", 0, pick(2) ? errors[3] : undefined);
    await check();
    assert.equal(closed.settled, true);
    assert.equal(finalized, Number(model.started));
  } catch (error) {
    throw new Error(`Route operations: ${JSON.stringify(trace)}`, {
      cause: error,
    });
  }
}

for (const [name, api] of [
  ["core", core],
  ["fluent", fluent],
]) {
  test(`${name}: seeded route operations match a bounded reference model`, async () => {
    await forSeeds((pick) => scenario(api, pick));
  });
}

for (const [name, api] of [
  ["core", core],
  ["fluent", fluent],
]) {
  test(`${name}: cancellation after EOF preserves completed endpoints while cleanup is pending`, async () => {
    const cleanup = deferred();
    const finalizing = deferred();
    const reason = new Error("cancel during cleanup");
    const late = new Error("cleanup failed");
    const group = api.route(
      api.from([0]).pipe(
        api.finalize(() => {
          finalizing.resolve();
          return cleanup.promise;
        }),
      ),
      { branches: 2, capacity: 1, select: () => 0 },
    );
    const empty = group.branches[1].iterate();
    assert.deepEqual(await empty.next(), done);
    await finalizing.promise;
    const completedEndpoint = await empty.closed;
    group.cancel(reason);
    assert.equal(await empty.closed, completedEndpoint);
    assert.deepEqual(completedEndpoint, completed());
    assert.deepEqual(
      await group.branches[0].closed,
      stopped("cancelled", reason),
    );
    cleanup.reject(late);
    const report = await group.closed;
    assert.equal(report.source.kind, "cancelled");
    assert.equal(report.source.reason, reason);
    assert.deepEqual(report.source.suppressedErrors, [late]);
    assert.equal(report.branches[1], completedEndpoint);
  });
}
