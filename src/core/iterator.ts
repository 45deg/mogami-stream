import { IteratorCleanupError } from "../internal/errors.js";
import { done, item } from "../internal/deferred.js";
import type { ExecutionHost, Scope } from "./scope.js";
import type { Reader, Source } from "./stream.js";

type ExternalIterator<T> = {
  next(): IteratorResult<T> | PromiseLike<IteratorResult<T>>;
  return?(): IteratorResult<T> | PromiseLike<IteratorResult<T>>;
};

type IteratorSource<T> = {
  [Symbol.iterator]?: () => Iterator<T>;
  [Symbol.asyncIterator]?: () => AsyncIterator<T>;
};

const owners = new WeakMap<ExecutionHost, WeakSet<object>>();

function validateResult<T>(result: IteratorResult<T>): IteratorResult<T> {
  if (
    result === null ||
    (typeof result !== "object" && typeof result !== "function")
  ) {
    throw new TypeError("An iterator operation must return an object.");
  }
  return result;
}

function normalizeResult<T>(
  result: IteratorResult<T>,
  scope: Scope,
): IteratorResult<T> {
  validateResult(result);
  scope.check();
  const completed = result.done;
  scope.check();
  if (completed) return done();
  // Snapshot external getters inside the owned read, before downstream delivery.
  const value = result.value;
  scope.check();
  return item(value);
}

function readNext<T>(iterator: ExternalIterator<T>, scope: Scope) {
  const method = iterator.next;
  // A method getter can stop and clean up the iterator before invocation.
  scope.check();
  return method.call(iterator);
}

/** Adapts the external protocol without awaiting values stored in its results. */
export function acquireIterator<T>(source: Source<T>, scope: Scope): Reader<T> {
  // Boxing lets primitive iterables (notably strings) use the same protocol.
  const iterable: IteratorSource<T> = Object(source);
  let getIterator: (() => ExternalIterator<T>) | undefined =
    iterable[Symbol.asyncIterator];
  // Method getters can cancel before the fallback lookup or factory starts.
  scope.check();
  if (getIterator == null) {
    getIterator = iterable[Symbol.iterator];
    scope.check();
  }
  if (typeof getIterator !== "function") {
    throw new TypeError("The source must be iterable or async iterable.");
  }
  const iterator = getIterator.call(source);
  // Recovery boundaries have their own host but share execution ownership.
  let executionScope = scope;
  while (executionScope.parent) executionScope = executionScope.parent;
  const executionHost = executionScope.host;
  let claimed = owners.get(executionHost);
  if (!claimed) {
    claimed = new WeakSet();
    owners.set(executionHost, claimed);
  }
  if (claimed.has(iterator)) {
    throw new TypeError("An iterator cannot be shared within one execution.");
  }
  claimed.add(iterator);

  scope.setCleanup(async () => {
    const close = iterator.return;
    if (close == null) return;
    const result = validateResult(await close.call(iterator));
    if (result.done !== true) throw new IteratorCleanupError();
  });
  return {
    next: () =>
      Promise.resolve(readNext(iterator, scope)).then((result) =>
        normalizeResult(result, scope),
      ),
    read(receive, reject) {
      void Promise.resolve(readNext(iterator, scope)).then((result) => {
        try {
          receive(normalizeResult(result, scope));
        } catch (error) {
          reject(error);
        }
      }, reject);
    },
  };
}
