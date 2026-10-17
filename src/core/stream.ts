import { done } from "../internal/deferred.js";
import { acquireIterator } from "./iterator.js";
import { Scope } from "./scope.js";
import { iterate, type StreamIterator } from "../terminal/iterate.js";

export type Source<T> = Iterable<T> | AsyncIterable<T>;
export type Operator<T, U> = (source: AsyncIterable<T>) => Stream<U>;
export interface OperatorContext {
  readonly index: number;
  readonly signal: AbortSignal;
}
export type Reader<T> = { next(): Promise<IteratorResult<T>> };
export type Builder<T> = (scope: Scope) => Reader<T>;
const openStream = Symbol("openStream");

export class Stream<T> implements AsyncIterable<T> {
  constructor(private readonly builder: Builder<T>) {}

  [openStream](scope: Scope): Reader<T> {
    let reader: Reader<T> | undefined;
    return {
      next: () => {
        // Acquiring an internal iterator must not start its stage yet.
        if (!reader) {
          reader = this.builder(scope);
          scope.check();
        }
        return reader.next();
      },
    };
  }
  [Symbol.asyncIterator](): StreamIterator<T> {
    return iterate(this);
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
  return {
    scope,
    async next() {
      if (ended) return done();
      scope.check();
      const pause = scope.checkpoint();
      if (pause) await scope.observe(pause);
      scope.check();
      let operation: Promise<IteratorResult<T>>;
      try {
        operation = scope.observe(reader.next());
      } catch (error) {
        parent.host.fail(error);
        throw error;
      }
      const result = await operation;
      scope.check();
      if (result.done) {
        ended = true;
        scope.markDone();
        // Expose exhaustion before cleanup finishes so combinators can stop
        // siblings promptly. The owning scope still waits for this cleanup.
        void scope.close();
      }
      return result;
    },
  };
}

export interface OwnedReader<T> extends Reader<T> {
  readonly scope: Scope;
}
