export class IteratorCleanupError extends Error {
  constructor() {
    super("Iterator.return() did not finish the iterator (done was not true).");
    this.name = "IteratorCleanupError";
  }
}

export class TimeoutError extends Error {
  constructor(readonly durationMs: number) {
    super(`The next value did not arrive within ${durationMs} ms.`);
    this.name = "TimeoutError";
  }
}

export class DeadlineExceededError extends Error {
  constructor(readonly durationMs: number) {
    super(`The execution exceeded its ${durationMs} ms time budget.`);
    this.name = "DeadlineExceededError";
  }
}

export function cancellationReason(): Error {
  const error = new Error("The stream execution was cancelled.");
  error.name = "AbortError";
  return error;
}

export function integer(value: number, name: string, minimum = 1): void {
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new RangeError(`${name} must be a safe integer >= ${minimum}.`);
  }
}

export function duration(value: number, name: string, positive = false): void {
  if (!Number.isFinite(value) || (positive ? value <= 0 : value < 0)) {
    throw new RangeError(
      `${name} must be finite and ${positive ? "positive" : "non-negative"}.`,
    );
  }
}
