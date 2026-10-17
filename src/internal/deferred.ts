export function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((settle) => {
    resolve = settle;
  });
  return { promise, resolve };
}

export const done = (): IteratorReturnResult<undefined> => ({
  done: true,
  value: undefined,
});
export const item = <T>(value: T): IteratorYieldResult<T> => ({
  done: false,
  value,
});
