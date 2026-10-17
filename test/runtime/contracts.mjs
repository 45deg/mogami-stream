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
      (await stream.toArray(stream.merge([1, 3], [2, 4]))).sort(),
      [1, 2, 3, 4],
    );
    equal(await stream.toArray(stream.zip([1, 2], ["a"])), [[1, "a"]]);
  });
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
    equal(
      await stream.toArray(
        stream.of([1, 2], [], [3]).pipe(stream.concat({ concurrency: 2 })),
      ),
      [1, 2, 3],
    );
    equal(
      (
        await stream.toArray(
          stream
            .of(1, 2)
            .pipe(stream.flatMap((x) => [x, x], { concurrency: 2 })),
        )
      ).sort(),
      [1, 1, 2, 2],
    );
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
  return passed;
}
