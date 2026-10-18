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

export type Source<T> = Iterable<T> | AsyncIterable<T>;
export type Operator<T, U> = (source: AsyncIterable<T>) => Stream<U>;
export interface OperatorContext {
  readonly index: number;
  readonly signal: AbortSignal;
}
/** Internal delivery path: callbacks may run synchronously and must not throw. */
export type Read<T> = (
  receive: (result: IteratorResult<T>) => void,
  reject: (error: unknown) => void,
) => void;
export type Reader<T> = {
  next(): Promise<IteratorResult<T>>;
  read?: Read<T>;
};
export type Builder<T> = (scope: Scope) => Reader<T>;
const openStream = Symbol("openStream");

export class Stream<T> implements AsyncIterable<T> {
  constructor(private readonly builder: Builder<T>) {}

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
  [Symbol.asyncIterator](): StreamIterator<T> {
    return iterate(this);
  }
  iterate(options: ExecutionOptions = {}): StreamIterator<T> {
    return iterate(this, options);
  }
  toArray(options: ExecutionOptions = {}): Promise<T[]> {
    return toArray(this, options);
  }
  reduce<A>(
    reducer: (
      accumulator: A,
      value: T,
      context: OperatorContext,
    ) => A | PromiseLike<A>,
    initial: A,
    options: ExecutionOptions = {},
  ): Promise<A> {
    return reduce(this, reducer, initial, options);
  }
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
  let reader: Reader<T>;
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
          reader,
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
        const result = await reader.next();
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
