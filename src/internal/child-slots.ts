import type { Scope } from "../core/scope.js";
import { open, type Source } from "../core/stream.js";
import { deferred } from "./deferred.js";

export function notifications(scope?: Scope) {
  let event = deferred<void>();
  const notify = () => {
    const previous = event;
    event = deferred();
    previous.resolve();
  };
  scope?.signal.addEventListener("abort", notify, { once: true });
  scope?.finalizers.push(() =>
    scope.signal.removeEventListener("abort", notify),
  );
  return {
    get promise() {
      return event.promise;
    },
    notify,
  };
}

/** Each input has exactly one pending read or one ready result. */
export class ChildSlot<T> {
  readonly reader;
  result: IteratorResult<T> | undefined;
  pending = false;
  failure: { error: unknown } | undefined;
  constructor(
    source: Source<T>,
    scope: Scope,
    private readonly notify: () => void,
  ) {
    this.reader = open(source, scope);
  }
  start(): void {
    if (this.pending || this.result || this.failure) return;
    this.pending = true;
    void this.reader.next().then(
      (result) => {
        this.pending = false;
        this.result = result;
        this.notify();
      },
      (error: unknown) => {
        this.pending = false;
        this.failure = { error };
        this.notify();
      },
    );
  }
  consume(): IteratorResult<T> {
    if (this.failure) throw this.failure.error;
    const result = this.result!;
    this.result = undefined;
    return result;
  }
}
