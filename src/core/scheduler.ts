export interface Scheduler {
  now(): number;
  sleep(ms: number, signal: AbortSignal): Promise<void>;
}

const MAX_TIMER_MS = 2_147_483_647;

function wait(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(signal.reason);
      return;
    }
    const abort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", abort);
      resolve();
    }, ms);
    signal.addEventListener("abort", abort, { once: true });
  });
}

export const defaultScheduler: Scheduler = {
  now() {
    if (typeof globalThis.performance?.now !== "function") {
      throw new Error("A monotonic performance.now() clock is required.");
    }
    return globalThis.performance.now();
  },
  async sleep(ms, signal) {
    const end = this.now() + ms;
    do {
      await wait(Math.min(MAX_TIMER_MS, Math.max(0, end - this.now())), signal);
    } while (this.now() < end);
  },
};
