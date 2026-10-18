import type { Scheduler } from "../core/scheduler.js";
import { duration } from "./errors.js";

/** Internal test utility; not exported by the package entry point. */
export interface TestClock extends Scheduler {
  advanceBy(ms: number): Promise<void>;
  advanceTo(timeMs: number): Promise<void>;
  readonly pendingTimers: number;
}

export function createTestClock(): TestClock {
  let time = 0;
  let advancing = false;
  const timers = new Set<{ at: number; resolve(): void }>();
  // A host turn lets promise continuations install their next virtual timers.
  const settle = () => new Promise<void>((resolve) => setTimeout(resolve, 0));
  return {
    now: () => time,
    get pendingTimers() {
      return timers.size;
    },
    sleep(ms, signal) {
      duration(ms, "ms");
      return new Promise<void>((resolve, reject) => {
        if (signal.aborted) {
          reject(signal.reason);
          return;
        }
        const at = time + ms;
        if (!Number.isFinite(at)) {
          reject(new RangeError("Timer time must be finite."));
          return;
        }
        const timer = {
          at,
          resolve() {
            timers.delete(timer);
            signal.removeEventListener("abort", abort);
            resolve();
          },
        };
        const abort = () => {
          timers.delete(timer);
          signal.removeEventListener("abort", abort);
          reject(signal.reason);
        };
        signal.addEventListener("abort", abort, { once: true });
        timers.add(timer);
      });
    },
    advanceBy(ms) {
      duration(ms, "ms");
      return this.advanceTo(time + ms);
    },
    async advanceTo(timeMs) {
      duration(timeMs, "timeMs");
      if (timeMs < time)
        throw new RangeError("The test clock cannot move backwards.");
      if (advancing)
        throw new TypeError("Only one clock advance may be pending.");
      advancing = true;
      try {
        await settle();
        for (;;) {
          let next = Infinity;
          for (const timer of timers) next = Math.min(next, timer.at);
          if (next > timeMs) break;
          time = next;
          for (const timer of [...timers])
            if (timer.at <= time) timer.resolve();
          await settle();
        }
        time = timeMs;
      } finally {
        advancing = false;
      }
    },
  };
}
