import * as stream from "../../dist/index.js";

function equal(actual, expected) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(
      `Expected ${JSON.stringify(expected)}, received ${JSON.stringify(actual)}.`,
    );
  }
}

/** The same public-package contracts run in every host, without Node APIs. */
export async function runContracts() {
  const passed = [];
  async function check(name, run) {
    await run();
    passed.push(name);
  }

  await check("sequential pipeline", async () => {
    equal(
      await stream.toArray(
        stream.range(0, 10).pipe(
          stream.filter((x) => x % 2 === 0),
          stream.map((x) => x * 2),
          stream.take(3),
        ),
      ),
      [0, 4, 8],
    );
  });
  await check("recovery and sequential selection", async () => {
    const failure = new Error("upstream");
    let finalized = false;
    const source = stream
      .defer(() => {
        throw failure;
      })
      .pipe(
        stream.finalize(() => {
          finalized = true;
        }),
        stream.catchError(async (error, { signal }) => {
          if (error !== failure || signal.aborted || !finalized)
            throw new Error(
              "Recovery ran before cleanup or lost failure context.",
            );
          return [null, 0, 0, 1, 1, 2, 1, undefined];
        }),
        stream.filterMap(async (value) => value),
        stream.dedupe(async (value) => value),
        stream.skipWhile(async (value) => value < 1),
      );
    equal(await source.toArray(), [1, 2, 1]);
  });
  await check("bounded ordered map", async () => {
    equal(
      await stream.toArray(
        stream
          .range(0, 8)
          .pipe(
            stream.map(async (x) => x * 2, { concurrency: 3, capacity: 4 }),
          ),
      ),
      [0, 2, 4, 6, 8, 10, 12, 14],
    );
  });
  await check("unordered map", async () => {
    equal(
      (
        await stream.toArray(
          stream
            .of(1, 2, 3)
            .pipe(
              stream.map(async (x) => x, { concurrency: 2, ordered: false }),
            ),
        )
      ).sort(),
      [1, 2, 3],
    );
  });
  await check("lazy creation and pre-abort", async () => {
    let opened = false;
    const source = stream.defer(() => {
      opened = true;
      return [1];
    });
    equal(await stream.toArray(source.pipe(stream.take(0))), []);
    const controller = new AbortController();
    controller.abort("stop");
    try {
      await stream.toArray(source, { signal: controller.signal });
      throw new Error("Expected cancellation.");
    } catch (error) {
      if (error !== "stop") throw error;
    }
    equal(opened, false);
  });
  await check("short-circuit signal isolation and finalization", async () => {
    let upstreamSignal,
      downstreamSignal,
      finalized = 0;
    const source = stream
      .defer(({ signal }) => {
        upstreamSignal = signal;
        return [1, 2];
      })
      .pipe(
        stream.finalize(() => {
          finalized++;
        }),
        stream.take(1),
        stream.map((value, { signal }) => {
          downstreamSignal = signal;
          return value;
        }),
      );
    const iterator = stream.iterate(source);
    equal((await iterator.next()).value, 1);
    equal(upstreamSignal.aborted, true);
    equal(downstreamSignal.aborted, false);
    await iterator.return();
    equal(finalized, 1);
  });
  await check("merge and zip", async () => {
    equal(
      await stream.toArray(stream.merge([], [1], [], [2, 3], [])),
      [1, 2, 3],
    );
    equal(
      (await stream.toArray(stream.merge([1, 3], [2, 4]))).sort(),
      [1, 2, 3, 4],
    );
    equal(await stream.toArray(stream.zip([1, 2], ["a"])), [[1, "a"]]);
  });
  await check(
    "zip completion recognizes a deferred sibling's abort response",
    async () => {
      let finishShortest;
      let startSibling;
      let returned = 0;
      const started = new Promise((resolve) => {
        startSibling = resolve;
      });
      const shortest = {
        [Symbol.asyncIterator]() {
          return this;
        },
        next() {
          return new Promise((resolve) => {
            finishShortest = resolve;
          });
        },
      };
      const sibling = stream.defer(({ signal }) => ({
        [Symbol.asyncIterator]() {
          return this;
        },
        next() {
          const pending = new Promise((resolve, reject) =>
            signal.addEventListener("abort", () => reject(signal.reason), {
              once: true,
            }),
          );
          startSibling();
          return pending;
        },
        return() {
          returned++;
          return { done: true };
        },
      }));
      const iterator = stream.iterate(stream.zip(shortest, sibling));
      const reading = iterator.next();
      await started;
      finishShortest({ done: true });
      equal((await reading).done, true);
      const report = await iterator.closed;
      equal(report.kind, "completed");
      equal(report.suppressedErrors, []);
      equal(returned, 1);
    },
  );
  await check("zipLatest", async () => {
    equal(
      await stream.toArray(stream.zipLatest([stream.of(1), stream.of("a")])),
      [[1, "a"]],
    );
    equal(
      await stream.toArray(
        stream.zipLatest([stream.empty(), stream.of(1)], { partial: true }),
      ),
      [[undefined, 1]],
    );
  });
  await check("higher order streams", async () => {
    for (const concurrency of [1, 2]) {
      for (const operator of [stream.concat, stream.flatten]) {
        equal(
          (
            await stream.toArray(
              stream.of([1, 2], [], [3]).pipe(operator({ concurrency })),
            )
          ).sort(),
          [1, 2, 3],
        );
      }
      for (const operator of [stream.flatMap, stream.concatMap]) {
        equal(
          (
            await stream.toArray(
              stream.of(1, 2).pipe(operator((x) => [x, x], { concurrency })),
            )
          ).sort(),
          [1, 1, 2, 2],
        );
      }
    }
  });
  await check("cleanup failure reports", async () => {
    const failure = new Error("cleanup");
    const iterator = stream.iterate(
      stream.empty().pipe(
        stream.finalize(() => {
          throw failure;
        }),
      ),
    );
    try {
      await iterator.next();
      throw new Error("Expected a cleanup failure.");
    } catch (error) {
      if (error !== failure) throw error;
    }
    equal((await iterator.closed).kind, "failed");
  });
  await check("timer connection", async () => {
    equal(
      await stream
        .toArray(
          stream
            .of(1, 2)
            .pipe(
              stream.delayStart(1),
              stream.spaceout(1),
              stream.rateLimit({ count: 1, perMs: 1 }),
              stream.bufferTime(1, { maxItems: 2 }),
            ),
        )
        .then((values) => values.flat()),
      [1, 2],
    );
  });
  await check("abortable timer", async () => {
    const iterator = stream.iterate(
      stream.of(1).pipe(stream.delayStart(60_000)),
    );
    const result = iterator.next().then(
      () => false,
      (error) => error === "stop",
    );
    iterator.cancel("stop");
    equal(await result, true);
    equal((await iterator.closed).kind, "cancelled");
  });
  await check("deadline zero", async () => {
    try {
      await stream.toArray(stream.of(1).pipe(stream.deadline(0)));
      throw new Error("Expected deadline failure.");
    } catch (error) {
      if (!(error instanceof stream.DeadlineExceededError)) throw error;
    }
  });
  await check("Promise values and reduce", async () => {
    const promise = Promise.resolve(1);
    if ((await stream.toArray(stream.from([promise])))[0] !== promise)
      throw new Error("Promise identity changed.");
    equal(await stream.reduce(stream.of(1, 2, 3), (sum, x) => sum + x, 0), 6);
  });
  await check("primitive iterable protocol", async () => {
    equal(await stream.toArray(stream.from("ab")), ["a", "b"]);
    const source = {
      [Symbol.asyncIterator]: undefined,
      *[Symbol.iterator]() {
        yield 1;
      },
    };
    equal(await stream.toArray(stream.from(source)), [1]);
  });
  await check("zip stops siblings before waiting for finalizers", async () => {
    let finishCleanup;
    const cleanup = new Promise((resolve) => {
      finishCleanup = resolve;
    });
    let returned = false;
    const sibling = {
      [Symbol.asyncIterator]() {
        return this;
      },
      next() {
        return { value: 1 };
      },
      return() {
        returned = true;
        finishCleanup();
        return { done: true };
      },
    };
    equal(
      await stream.toArray(
        stream.zip(
          stream.empty().pipe(stream.finalize(() => cleanup)),
          sibling,
        ),
      ),
      [],
    );
    equal(returned, true);
  });
  await check("unread stages do not finalize", async () => {
    let finalized = false;
    const failure = new Error("sibling acquisition");
    const unread = stream.of(1).pipe(
      stream.finalize(() => {
        finalized = true;
      }),
    );
    const broken = {
      [Symbol.asyncIterator]() {
        throw failure;
      },
    };
    try {
      await stream.toArray(stream.merge(unread, broken));
      throw new Error("Expected failure.");
    } catch (error) {
      if (error !== failure) throw error;
    }
    equal(finalized, false);
  });
  await check("incomplete cleanup without a done flag fails", async () => {
    const source = {
      [Symbol.asyncIterator]() {
        return this;
      },
      next() {
        return { value: 1 };
      },
      return() {
        return { value: 2 };
      },
    };
    const iterator = stream.iterate(source);
    await iterator.next();
    try {
      await iterator.return();
      throw new Error("Expected cleanup failure.");
    } catch (error) {
      if (!(error instanceof stream.IteratorCleanupError)) throw error;
    }
    equal((await iterator.closed).kind, "failed");
  });
  await check(
    "callback cancellation closes a synchronous infinite filter",
    async () => {
      const controller = new AbortController();
      const reason = new Error("callback cancellation");
      let reads = 0;
      let closed = false;
      const source = {
        *[Symbol.iterator]() {
          try {
            for (;;) yield 1;
          } finally {
            closed = true;
          }
        },
      };
      try {
        await stream.toArray(
          stream.from(source).pipe(
            stream.filter(() => {
              if (++reads === 1_000) controller.abort(reason);
              return false;
            }),
          ),
          { signal: controller.signal },
        );
        throw new Error("Expected timer cancellation.");
      } catch (error) {
        if (error !== reason) throw error;
      }
      equal(closed, true);
      equal(reads, 1_000);
    },
  );
  await check("consumption and running aggregates", async () => {
    const seen = [];
    await stream.forEach([1, 2, 3], async (value, { index }) => {
      seen.push([value, index]);
    });
    equal(seen, [
      [1, 0],
      [2, 1],
      [3, 2],
    ]);
    equal(
      await stream
        .of(1, 2, 3)
        .pipe(stream.scan(async (sum, value) => sum + value, 0))
        .toArray(),
      [1, 3, 6],
    );
    equal(await stream.find([1, 2, 3], async (value) => value > 1), 2);
    equal(await stream.of(1, 2).some(async (value) => value === 2), true);
    equal(await stream.of(1, 2).every((value) => value < 2), false);
    equal(await stream.empty().every(() => false), true);
  });
  await check("timeline source completion and failure", async () => {
    equal(
      await stream
        .fromTimeline([
          { at: 0, kind: "value", value: "A" },
          { at: 1, kind: "value", value: "B" },
          { at: 2, kind: "complete" },
        ])
        .toArray(),
      ["A", "B"],
    );
    const failure = new Error("timeline");
    try {
      await stream
        .fromTimeline([{ at: 0, kind: "error", error: failure }])
        .toArray();
      throw new Error("Expected timeline failure.");
    } catch (error) {
      if (error !== failure) throw error;
    }
  });
  await check("item retries and settled outcomes", async () => {
    const attempts = [];
    equal(
      await stream
        .of(1, 2)
        .pipe(
          stream.map(
            (value, { attempt, index }) => {
              attempts.push([index, attempt]);
              if (attempt === 1) throw new Error("transient");
              return Promise.resolve(value * 2);
            },
            { concurrency: 2, retry: { retries: 1, delayMs: 1 } },
          ),
        )
        .toArray(),
      [2, 4],
    );
    equal(attempts.length, 4);
    const failure = new Error("item");
    const values = await stream
      .of(1, 2)
      .pipe(
        stream.mapSettled(
          async (value) => {
            if (value === 1) throw failure;
            return value * 2;
          },
          { concurrency: 2 },
        ),
      )
      .toArray();
    equal(values[0].status, "rejected");
    if (values[0].reason !== failure)
      throw new Error("Mapper failure identity changed.");
    equal(values[1], { status: "fulfilled", value: 4 });
    let settledAttempts = 0;
    const final = await stream
      .of(1)
      .pipe(
        stream.mapSettled(
          () => {
            settledAttempts++;
            throw failure;
          },
          { retry: { retries: 2, delayMs: 1 } },
        ),
      )
      .toArray();
    equal(settledAttempts, 3);
    equal(final[0].status, "rejected");
    if (final[0].reason !== failure)
      throw new Error("Final retry failure identity changed.");
    if ("mapRetry" in stream)
      throw new Error("Removed mapRetry export is still present.");

    const iterator = stream
      .of(1)
      .pipe(
        stream.map(
          () => {
            throw failure;
          },
          { retry: { retries: 2, delayMs: 60_000 } },
        ),
      )
      .iterate();
    const pending = iterator.next().then(
      () => false,
      (error) => error === "stop",
    );
    iterator.cancel("stop");
    equal(await pending, true);
    equal((await iterator.closed).kind, "cancelled");
  });
  return passed;
}
