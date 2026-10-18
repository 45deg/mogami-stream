import type { Scheduler } from "./scheduler.js";

export interface ExecutionHost {
  readonly scheduler: Scheduler;
  fail(error: unknown): void;
  suppress(error: unknown): void;
}

/** Owns one stage, its outstanding operations, and its upstream stages. */
export class Scope {
  readonly controller = new AbortController();
  readonly children = new Set<Scope>();
  private pendingOperations = 0;
  private operationsDrained: (() => void) | undefined;
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

  setCleanup(cleanup: () => void | PromiseLike<void>): void {
    this.cleanup = cleanup;
    // An external iterator factory can synchronously abort during acquisition.
    if (this.stopped) this.startCleanup();
  }
  markDone(): void {
    this.finished = true;
  }

  observe<T>(work: PromiseLike<T> | T, isCleanup = false): Promise<Awaited<T>> {
    const promise = Promise.resolve(work);
    this.operationStarted();
    // Attach both handlers immediately, including for operations that lose a race.
    void promise.then(this.operationFinished, (error) => {
      this.operationFinished();
      this.reportError(error, isCleanup);
    });
    return promise;
  }

  /** Pair with operationFinished in finally when the caller already awaits work. */
  operationStarted(): void {
    this.pendingOperations++;
  }

  reportError(error: unknown, isCleanup = false): void {
    if (this.signal.aborted && Object.is(error, this.signal.reason)) return;
    if (isCleanup || this.stopped) this.host.suppress(error);
    else this.host.fail(error);
  }

  readonly operationFinished = (): void => {
    if (--this.pendingOperations === 0) {
      this.operationsDrained?.();
      this.operationsDrained = undefined;
    }
  };

  /** Broadcast first. No await may postpone a sibling's stop request. */
  stop(reason: unknown): void {
    if (this.stopped) return;
    this.stopped = true;
    this.controller.abort(reason);
    // abort(undefined) creates a reason; share that same object with descendants.
    const stopReason = this.signal.reason;
    for (const child of this.children) child.stop(stopReason);
    this.startCleanup();
  }

  private startCleanup(): void {
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
    while (this.pendingOperations) {
      await new Promise<void>((resolve) => {
        this.operationsDrained = resolve;
      });
    }
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
