import type { Scope } from "../core/scope.js";
import { open, type Source } from "../core/stream.js";

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
    this.reader.read(
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
