import { from } from "../operators/creation.js";
import type { Source, Stream, Operator } from "./stream.js";

/**
 * Wraps `source` in a {@link Stream} and applies operators from left to right without starting execution.
 *
 * Type inference supports up to sixteen operators; chain additional `.pipe()` calls for longer pipelines.
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
export function pipe<T, A, B, C, D, E, F, G, H, I>(
  source: Source<T>,
  op1: Operator<T, A>,
  op2: Operator<A, B>,
  op3: Operator<B, C>,
  op4: Operator<C, D>,
  op5: Operator<D, E>,
  op6: Operator<E, F>,
  op7: Operator<F, G>,
  op8: Operator<G, H>,
  op9: Operator<H, I>,
): Stream<I>;
export function pipe<T, A, B, C, D, E, F, G, H, I, J>(
  source: Source<T>,
  op1: Operator<T, A>,
  op2: Operator<A, B>,
  op3: Operator<B, C>,
  op4: Operator<C, D>,
  op5: Operator<D, E>,
  op6: Operator<E, F>,
  op7: Operator<F, G>,
  op8: Operator<G, H>,
  op9: Operator<H, I>,
  op10: Operator<I, J>,
): Stream<J>;
export function pipe<T, A, B, C, D, E, F, G, H, I, J, K>(
  source: Source<T>,
  op1: Operator<T, A>,
  op2: Operator<A, B>,
  op3: Operator<B, C>,
  op4: Operator<C, D>,
  op5: Operator<D, E>,
  op6: Operator<E, F>,
  op7: Operator<F, G>,
  op8: Operator<G, H>,
  op9: Operator<H, I>,
  op10: Operator<I, J>,
  op11: Operator<J, K>,
): Stream<K>;
export function pipe<T, A, B, C, D, E, F, G, H, I, J, K, L>(
  source: Source<T>,
  op1: Operator<T, A>,
  op2: Operator<A, B>,
  op3: Operator<B, C>,
  op4: Operator<C, D>,
  op5: Operator<D, E>,
  op6: Operator<E, F>,
  op7: Operator<F, G>,
  op8: Operator<G, H>,
  op9: Operator<H, I>,
  op10: Operator<I, J>,
  op11: Operator<J, K>,
  op12: Operator<K, L>,
): Stream<L>;
export function pipe<T, A, B, C, D, E, F, G, H, I, J, K, L, M>(
  source: Source<T>,
  op1: Operator<T, A>,
  op2: Operator<A, B>,
  op3: Operator<B, C>,
  op4: Operator<C, D>,
  op5: Operator<D, E>,
  op6: Operator<E, F>,
  op7: Operator<F, G>,
  op8: Operator<G, H>,
  op9: Operator<H, I>,
  op10: Operator<I, J>,
  op11: Operator<J, K>,
  op12: Operator<K, L>,
  op13: Operator<L, M>,
): Stream<M>;
export function pipe<T, A, B, C, D, E, F, G, H, I, J, K, L, M, N>(
  source: Source<T>,
  op1: Operator<T, A>,
  op2: Operator<A, B>,
  op3: Operator<B, C>,
  op4: Operator<C, D>,
  op5: Operator<D, E>,
  op6: Operator<E, F>,
  op7: Operator<F, G>,
  op8: Operator<G, H>,
  op9: Operator<H, I>,
  op10: Operator<I, J>,
  op11: Operator<J, K>,
  op12: Operator<K, L>,
  op13: Operator<L, M>,
  op14: Operator<M, N>,
): Stream<N>;
export function pipe<T, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O>(
  source: Source<T>,
  op1: Operator<T, A>,
  op2: Operator<A, B>,
  op3: Operator<B, C>,
  op4: Operator<C, D>,
  op5: Operator<D, E>,
  op6: Operator<E, F>,
  op7: Operator<F, G>,
  op8: Operator<G, H>,
  op9: Operator<H, I>,
  op10: Operator<I, J>,
  op11: Operator<J, K>,
  op12: Operator<K, L>,
  op13: Operator<L, M>,
  op14: Operator<M, N>,
  op15: Operator<N, O>,
): Stream<O>;
export function pipe<T, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P>(
  source: Source<T>,
  op1: Operator<T, A>,
  op2: Operator<A, B>,
  op3: Operator<B, C>,
  op4: Operator<C, D>,
  op5: Operator<D, E>,
  op6: Operator<E, F>,
  op7: Operator<F, G>,
  op8: Operator<G, H>,
  op9: Operator<H, I>,
  op10: Operator<I, J>,
  op11: Operator<J, K>,
  op12: Operator<K, L>,
  op13: Operator<L, M>,
  op14: Operator<M, N>,
  op15: Operator<N, O>,
  op16: Operator<O, P>,
): Stream<P>;
export function pipe(
  source: Source<any>,
  ...operators: Array<(source: any) => any>
): Stream<any> {
  return operators.reduce((stream, operator) => operator(stream), from(source));
}
