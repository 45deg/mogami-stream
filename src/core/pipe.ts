import { from } from "../operators/creation.js";
import type { Source, Stream, Operator } from "./stream.js";

/**
 * Wraps `source` in a {@link Stream} and applies operators from left to right without starting execution.
 *
 * Type inference supports up to eight operators; chain additional `.pipe()` calls for longer pipelines.
 * @category Composition
 */
export function pipe<T>(source: Source<T>): Stream<T>;
export function pipe<T, A>(source: Source<T>, op1: Operator<T, A>): Stream<A>;
export function pipe<T, A, B>(
  source: Source<T>,
  op1: Operator<T, A>,
  op2: Operator<A, B>,
): Stream<B>;
export function pipe<T, A, B, C>(
  source: Source<T>,
  op1: Operator<T, A>,
  op2: Operator<A, B>,
  op3: Operator<B, C>,
): Stream<C>;
export function pipe<T, A, B, C, D>(
  source: Source<T>,
  op1: Operator<T, A>,
  op2: Operator<A, B>,
  op3: Operator<B, C>,
  op4: Operator<C, D>,
): Stream<D>;
export function pipe<T, A, B, C, D, E>(
  source: Source<T>,
  op1: Operator<T, A>,
  op2: Operator<A, B>,
  op3: Operator<B, C>,
  op4: Operator<C, D>,
  op5: Operator<D, E>,
): Stream<E>;
export function pipe<T, A, B, C, D, E, F>(
  source: Source<T>,
  op1: Operator<T, A>,
  op2: Operator<A, B>,
  op3: Operator<B, C>,
  op4: Operator<C, D>,
  op5: Operator<D, E>,
  op6: Operator<E, F>,
): Stream<F>;
export function pipe<T, A, B, C, D, E, F, G>(
  source: Source<T>,
  op1: Operator<T, A>,
  op2: Operator<A, B>,
  op3: Operator<B, C>,
  op4: Operator<C, D>,
  op5: Operator<D, E>,
  op6: Operator<E, F>,
  op7: Operator<F, G>,
): Stream<G>;
export function pipe<T, A, B, C, D, E, F, G, H>(
  source: Source<T>,
  op1: Operator<T, A>,
  op2: Operator<A, B>,
  op3: Operator<B, C>,
  op4: Operator<C, D>,
  op5: Operator<D, E>,
  op6: Operator<E, F>,
  op7: Operator<F, G>,
  op8: Operator<G, H>,
): Stream<H>;
export function pipe(
  source: Source<any>,
  ...operators: Array<(source: any) => any>
): Stream<any> {
  return operators.reduce((stream, operator) => operator(stream), from(source));
}
