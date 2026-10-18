import { setImmediate } from "node:timers/promises";

export function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}
export async function flush() {
  for (let i = 0; i < 100; i++) await Promise.resolve();
}
export const turn = () => setImmediate();

export class VirtualScheduler {
  time = 0;
  timers = new Set();
  now() {
    return this.time;
  }
  sleep(ms, signal) {
    return new Promise((resolve, reject) => {
      if (signal.aborted) {
        reject(signal.reason);
        return;
      }
      const timer = {
        at: this.time + ms,
        resolve: () => {
          this.timers.delete(timer);
          signal.removeEventListener("abort", abort);
          resolve();
        },
      };
      const abort = () => {
        this.timers.delete(timer);
        signal.removeEventListener("abort", abort);
        reject(signal.reason);
      };
      signal.addEventListener("abort", abort, { once: true });
      this.timers.add(timer);
    });
  }
  async advance(ms) {
    this.time += ms;
    for (const timer of [...this.timers])
      if (timer.at <= this.time) timer.resolve();
    await flush();
  }
}

export function controlled() {
  const reads = [];
  let returns = 0;
  const source = {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      const read = deferred();
      reads.push(read);
      return read.promise;
    },
    return() {
      returns++;
      return Promise.resolve({ done: true });
    },
  };
  return {
    source,
    reads,
    get returns() {
      return returns;
    },
  };
}
