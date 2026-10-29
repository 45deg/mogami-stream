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
    this.reader.read(this.receive, this.reject);
  }
  private readonly receive = (result: IteratorResult<T>): void => {
    this.pending = false;
    this.result = result;
    this.notify();
  };
  private readonly reject = (error: unknown): void => {
    this.pending = false;
    this.failure = { error };
    this.notify();
  };

  consume(): IteratorResult<T> {
    if (this.failure) throw this.failure.error;
    const result = this.result!;
    this.result = undefined;
    return result;
  }
}

/** Circular links preserve input order when a finished child leaves rotation. */
export class LinkedChildSlot<T> extends ChildSlot<T> {
  next: LinkedChildSlot<T> = this;
  previous: LinkedChildSlot<T> = this;
}

export class ChildList<T> {
  first: LinkedChildSlot<T> | undefined;
  size = 0;

  append(slot: LinkedChildSlot<T>): void {
    const first = this.first;
    if (first) {
      slot.previous = first.previous;
      slot.next = first;
      first.previous.next = slot;
      first.previous = slot;
    } else this.first = slot;
    this.size++;
  }

  remove(slot: LinkedChildSlot<T>): LinkedChildSlot<T> | undefined {
    this.size--;
    const next = this.size ? slot.next : undefined;
    slot.previous.next = slot.next;
    slot.next.previous = slot.previous;
    if (this.first === slot) this.first = next;
    // A pending refill or cleanup must not retain the other children.
    slot.next = slot.previous = slot;
    return next;
  }
}
