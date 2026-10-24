import { done } from "../internal/deferred.js";
import { acquireIterator } from "./iterator.js";
import { Scope } from "./scope.js";
import {
  iterate,
  type ExecutionOptions,
  type StreamIterator,
} from "../terminal/iterate.js";
import { toArray } from "../terminal/to-array.js";
import { reduce } from "../terminal/reduce.js";
import { forEach, find, some, every } from "../terminal/consume.js";

/**
 * A synchronous or asynchronous iterable accepted as a stream input.
 */
export type Source<T> = Iterable<T> | AsyncIterable<T>;
/**
 * A function that transforms an `AsyncIterable` into a {@link Stream}.
 */
export type Operator<T, U> = (source: AsyncIterable<T>) => Stream<U>;
/**
 * Per-item context passed to operator callbacks.
 */
export interface OperatorContext {
  /** Zero-based item index for the current operator execution. */
  readonly index: number;
  /** Abort signal that triggers when the owning stage stops. Pass this to cancellable I/O. */
  readonly signal: AbortSignal;
}
/** Internal delivery path: callbacks may run synchronously and must not throw. */
export type Read<T> = (
  receive: (result: IteratorResult<T>) => void,
  reject: (error: unknown) => void,
) => void;
export type Reader<T> = {
  next(): Promise<IteratorResult<T>>;
  // Optional fast path for measured per-item overhead, not a requirement for operators.
  // Preserve cancellation before callbacks, bounded synchronous reentry, and cleanup ordering.
  // next() remains suitable for batching, predicate loops, and source switching.
  read?: Read<T>;
};
export type Builder<T> = (scope: Scope) => Reader<T>;
const openStream = Symbol("openStream");

/**
 * A lazy `AsyncIterable` pipeline with composable operators and terminal methods.
 *
 * Create streams with {@link from}, {@link defer}, or another source factory. Each iteration runs an independent execution; whether a stream can be iterated more than once depends on its underlying source.
 */
export class Stream<T> implements AsyncIterable<T> {
  /** @internal */
  constructor(private readonly builder: Builder<T>) {}

  /** Lets a fluent facade reuse the execution builder without adding a stage. @internal */
  protected static builderFor<T>(source: Stream<T>): Builder<T> {
    return source.builder;
  }

  /** @internal */
  [openStream](scope: Scope): Reader<T> {
    let reader: Reader<T> | undefined;
    const getReader = () => {
      if (!reader) {
        reader = this.builder(scope);
        scope.check();
      }
      return reader;
    };
    return {
      // Acquiring an internal iterator must not start its stage yet.
      next: () => getReader().next(),
      read: (receive, reject) => readInto(getReader(), receive, reject),
    };
  }
  /** Creates a new execution that starts reading on the first `next()` call. */
  [Symbol.asyncIterator](): StreamIterator<T> {
    return iterate(this);
  }
  /** Returns a controlled iterator for this stream. Equivalent to {@link iterate | `iterate(this, options)`}. */
  iterate(options: ExecutionOptions = {}): StreamIterator<T> {
    return iterate(this, options);
  }
  /** Collects all emitted values into an array and waits for cleanup before resolving. */
  toArray(options: ExecutionOptions = {}): Promise<T[]> {
    return toArray(this, options);
  }
  /** Reduces values sequentially from `initial` and returns the final accumulator after cleanup. */
  reduce<A>(
    reducer: (
      accumulator: Awaited<A>,
      value: T,
      context: OperatorContext,
    ) => Awaited<A> | PromiseLike<Awaited<A>>,
    initial: A,
    options: ExecutionOptions = {},
  ): Promise<Awaited<A>> {
    return reduce(this, reducer, initial, options);
  }
  /** Runs `effect` sequentially for each value and waits for cleanup without collecting results. */
  forEach(
    effect: (value: T, context: OperatorContext) => unknown,
    options: ExecutionOptions = {},
  ): Promise<void> {
    return forEach(this, effect, options);
  }
  /** Returns the first value matching `predicate` (or `undefined` if none match) and closes upstream. */
  find<S extends T>(
    predicate: (value: T, context: OperatorContext) => value is S,
    options?: ExecutionOptions,
  ): Promise<Awaited<S> | undefined>;
  find(
    predicate: (
      value: T,
      context: OperatorContext,
    ) => boolean | PromiseLike<boolean>,
    options?: ExecutionOptions,
  ): Promise<Awaited<T> | undefined>;
  find(
    predicate: (
      value: T,
      context: OperatorContext,
    ) => boolean | PromiseLike<boolean>,
    options: ExecutionOptions = {},
  ): Promise<Awaited<T> | undefined> {
    return find(this, predicate, options);
  }
  /** Returns `true` on the first matching value and closes upstream early, or `false` if no values match. */
  some(
    predicate: (
      value: T,
      context: OperatorContext,
    ) => boolean | PromiseLike<boolean>,
    options: ExecutionOptions = {},
  ): Promise<boolean> {
    return some(this, predicate, options);
  }
  /** Returns `false` on the first non-matching value and closes upstream early, or `true` if all values match. */
  every(
    predicate: (
      value: T,
      context: OperatorContext,
    ) => boolean | PromiseLike<boolean>,
    options: ExecutionOptions = {},
  ): Promise<boolean> {
    return every(this, predicate, options);
  }
  /** Composes up to eight operators from left to right without starting execution. */
  pipe(): Stream<T>;
  pipe<A>(op1: Operator<T, A>): Stream<A>;
  pipe<A, B>(op1: Operator<T, A>, op2: Operator<A, B>): Stream<B>;
  pipe<A, B, C>(
    op1: Operator<T, A>,
    op2: Operator<A, B>,
    op3: Operator<B, C>,
  ): Stream<C>;
  pipe<A, B, C, D>(
    op1: Operator<T, A>,
    op2: Operator<A, B>,
    op3: Operator<B, C>,
    op4: Operator<C, D>,
  ): Stream<D>;
  pipe<A, B, C, D, E>(
    op1: Operator<T, A>,
    op2: Operator<A, B>,
    op3: Operator<B, C>,
    op4: Operator<C, D>,
    op5: Operator<D, E>,
  ): Stream<E>;
  pipe<A, B, C, D, E, F>(
    op1: Operator<T, A>,
    op2: Operator<A, B>,
    op3: Operator<B, C>,
    op4: Operator<C, D>,
    op5: Operator<D, E>,
    op6: Operator<E, F>,
  ): Stream<F>;
  pipe<A, B, C, D, E, F, G>(
    op1: Operator<T, A>,
    op2: Operator<A, B>,
    op3: Operator<B, C>,
    op4: Operator<C, D>,
    op5: Operator<D, E>,
    op6: Operator<E, F>,
    op7: Operator<F, G>,
  ): Stream<G>;
  pipe<A, B, C, D, E, F, G, H>(
    op1: Operator<T, A>,
    op2: Operator<A, B>,
    op3: Operator<B, C>,
    op4: Operator<C, D>,
    op5: Operator<D, E>,
    op6: Operator<E, F>,
    op7: Operator<F, G>,
    op8: Operator<G, H>,
  ): Stream<H>;
  pipe(...operators: Array<(source: any) => any>): Stream<any> {
    return operators.reduce((source, operator) => operator(source), this);
  }
}

export function create<T>(builder: Builder<T>): Stream<T> {
  return new Stream(builder);
}

function readInto<T>(
  reader: Reader<T>,
  receive: Parameters<Read<T>>[0],
  reject: Parameters<Read<T>>[1],
): void {
  if (reader.read) reader.read(receive, reject);
  else void reader.next().then(receive, reject);
}

/** Opens only when an operator has demand; preserves Promise values in sync inputs. */
export function open<T>(source: Source<T>, parent: Scope): OwnedReader<T> {
  const scope = new Scope(parent.host, parent);
  let reader: Reader<T> | undefined;
  scope.finalizers.push(() => {
    // Error stacks can retain this wrapper after the stage has closed.
    reader = undefined;
  });
  try {
    if (source instanceof Stream) {
      reader = source[openStream](scope);
    } else {
      reader = acquireIterator(source, scope);
    }
  } catch (error) {
    parent.host.fail(error);
    throw error;
  }
  let ended = false;
  const accept = (result: IteratorResult<T>) => {
    scope.check();
    if (result.done) {
      ended = true;
      scope.markDone();
      // Expose exhaustion before cleanup finishes so siblings can stop promptly.
      void scope.close();
    }
    return result;
  };
  return {
    scope,
    read(receive, reject) {
      if (ended) {
        receive(done());
        return;
      }
      scope.operationStarted();
      const failed = (error: unknown) => {
        try {
          scope.reportError(error);
          reject(error);
        } finally {
          scope.operationFinished();
        }
      };
      try {
        scope.check();
        readInto(
          reader!,
          (result) => {
            try {
              const accepted = accept(result);
              receive(accepted);
            } catch (error) {
              scope.reportError(error);
              reject(error);
            } finally {
              scope.operationFinished();
            }
          },
          failed,
        );
      } catch (error) {
        failed(error);
      }
    },
    async next() {
      if (ended) return done();
      scope.check();
      scope.operationStarted();
      try {
        const result = await reader!.next();
        return accept(result);
      } catch (error) {
        scope.reportError(error);
        throw error;
      } finally {
        scope.operationFinished();
      }
    },
  };
}

export interface OwnedReader<T> extends Reader<T> {
  readonly scope: Scope;
  read: Read<T>;
}
