import {
  create,
  open,
  type Operator,
  type OperatorContext,
} from "../core/stream.js";

export function tap<T>(
  effect: (value: T, context: OperatorContext) => unknown,
): Operator<T, T> {
  return (source) =>
    create((scope) => {
      const upstream = open(source, scope);
      let index = 0;
      return {
        async next() {
          const result = await upstream.next();
          if (!result.done)
            await effect(result.value, {
              index: index++,
              signal: scope.signal,
            });
          return result;
        },
      };
    });
}

export function finalize<T>(effect: () => unknown): Operator<T, T> {
  return (source) =>
    create((scope) => {
      scope.finalizers.push(effect);
      const upstream = open(source, scope);
      return {
        next: () => upstream.next(),
        read: (receive, reject) => upstream.read(receive, reject),
      };
    });
}
