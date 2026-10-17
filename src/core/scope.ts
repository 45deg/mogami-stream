import type { Scheduler } from "./scheduler.js";

export interface ExecutionHost {
  readonly scheduler: Scheduler;
  fail(error: unknown): void;
  suppress(error: unknown): void;
  checkpoint(signal: AbortSignal): Promise<void>;
}

/** Owns one stage, its outstanding operations, and its upstream stages. */
export class Scope {
  readonly controller = new AbortController();
  readonly children = new Set<Scope>();
  readonly operations = new Set<Promise<unknown>>();
  readonly finalizers: Array<() => unknown> = [];
  private cleanup: (() => void | PromiseLike<void>) | undefined;
  private closing: Promise<void> | undefined;
  private stopped = false;
  private finished = false;

  constructor(
    readonly host: ExecutionHost,
    readonly parent?: Scope,
  ) {
    parent?.check();
    parent?.children.add(this);
  }

  get signal(): AbortSignal {
    return this.controller.signal;
  }
  get scheduler(): Scheduler {
    return this.host.scheduler;
  }

  check(): void {
    if (this.stopped) throw this.signal.reason;
  }

  checkpoint(): Promise<void> {
    this.check();
    return this.host.checkpoint(this.signal);
  }

  setCleanup(cleanup: () => void | PromiseLike<void>): void {
    this.cleanup = cleanup;
  }
  markDone(): void {
    this.finished = true;
  }

  observe<T>(work: PromiseLike<T> | T, isCleanup = false): Promise<Awaited<T>> {
    const promise = Promise.resolve(work);
    this.operations.add(promise);
    // Attach both handlers immediately, including for operations that lose a race.
    void promise.then(
      () => {
        this.operations.delete(promise);
      },
      (error) => {
        this.operations.delete(promise);
        if (this.signal.aborted && Object.is(error, this.signal.reason)) return;
        if (isCleanup || this.stopped) this.host.suppress(error);
        else this.host.fail(error);
      },
    );
    return promise;
  }

  /** Broadcast first. No await may postpone a sibling's stop request. */
  stop(reason: unknown): void {
    if (this.stopped) return;
    this.stopped = true;
    this.controller.abort(reason);
    for (const child of this.children) child.stop(reason);
    if (!this.finished && this.cleanup) {
      const cleanup = this.cleanup;
      this.cleanup = undefined;
      try {
        this.observe(cleanup(), true);
      } catch (error) {
        this.host.suppress(error);
      }
    }
  }

  close(): Promise<void> {
    if (this.closing) return this.closing;
    // Store the promise before cleanup can call back into close().
    this.closing = Promise.resolve().then(() => this.finish());
    return this.closing;
  }

  private async finish(): Promise<void> {
    for (const child of this.children) {
      if (!child.finished) child.stop(this.signal.reason);
    }
    await Promise.all([...this.children].map((child) => child.close()));
    while (this.operations.size) await Promise.allSettled([...this.operations]);
    for (const finalize of this.finalizers.reverse()) {
      try {
        await finalize();
      } catch (error) {
        this.host.suppress(error);
      }
    }
    this.parent?.children.delete(this);
  }
}
