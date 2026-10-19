import type { Scope } from "./scope.js";

/** Context for source setup in {@link defer} and {@link catchError}. */
export interface ResourceContext {
  /** Pass this signal to cancellable setup and reads. */
  readonly signal: AbortSignal;
  /**
   * Acquires a resource and gives this stage responsibility for releasing it.
   *
   * Call and await this method during the factory/handler invocation. `acquire`
   * is skipped if the stage has stopped. Once it succeeds, `release` runs once
   * after child sources and pending work finish, even if cancellation occurred
   * during acquisition. Cancellation rejects this promise after registering
   * release. Acquisition failures fail the stage; partial acquisition cleanup
   * belongs to `acquire`. Release failures follow the normal cleanup contract.
   * Do not also release the same resource from the returned source.
   */
  acquire<R>(
    acquire: (signal: AbortSignal) => R | PromiseLike<R>,
    release: (resource: R) => unknown,
  ): Promise<R>;
}

/** Keeps registration open only while the source callback is running. */
export async function withResourceContext<T>(
  scope: Scope,
  callback: (context: ResourceContext) => T | PromiseLike<T>,
): Promise<T> {
  let accepting = true;
  try {
    scope.check();
    return await callback({
      signal: scope.signal,
      acquire<R>(
        acquire: (signal: AbortSignal) => R | PromiseLike<R>,
        release: (resource: R) => unknown,
      ): Promise<R> {
        if (!accepting)
          return Promise.reject(
            new TypeError("Resources must be acquired during source setup."),
          );
        return scope.observe(
          Promise.resolve().then(async () => {
            scope.check();
            const resource = await acquire(scope.signal);
            // Register before checking cancellation: a late result is owned too.
            scope.finalizers.push(() => release(resource));
            scope.check();
            return resource;
          }),
        );
      },
    });
  } finally {
    accepting = false;
  }
}
