import { Scope, type ExecutionHost } from "../core/scope.js";
import { withResourceContext, type ResourceContext } from "../core/resource.js";
import { create, open, type Source, type Stream } from "../core/stream.js";

/**
 * Recovers from an upstream failure by switching once to a fallback source returned by `handler`.
 *
 * Waits for the failed upstream stage to finish cleaning up before calling `handler`. Execution cancellation, cleanup errors, downstream errors, and errors thrown by `handler` or the replacement source are not caught. `handler` may return a source or a promise of one, and receives the active stage's `AbortSignal`. Use `context.acquire(acquire, release)` for resources opened during recovery; plain fallback sources returned after cancellation are never opened or closed by the library.
 * @category Lifecycle
 */
export function catchError<U>(
  handler: (
    error: unknown,
    context: ResourceContext,
  ) => Source<U> | PromiseLike<Source<U>>,
): <T>(source: AsyncIterable<T>) => Stream<T | U> {
  return <T>(source: AsyncIterable<T>) =>
    create<T | U>((scope) => {
      let failure: { error: unknown } | undefined;
      // An upstream failure stops this subtree without failing the execution.
      // Cleanup and late failures still belong to the execution's close report.
      const host: ExecutionHost = {
        scheduler: scope.scheduler,
        fail(error) {
          if (scope.signal.aborted) {
            scope.reportError(error);
          } else if (!failure) {
            failure = { error };
            boundary.stop(error);
            void boundary.close();
          } else if (!Object.is(error, failure.error)) {
            scope.host.suppress(error);
          }
        },
        suppress(error) {
          if (failure && Object.is(error, failure.error)) return;
          scope.host.suppress(error);
        },
      };
      const boundary = new Scope(host, scope);
      let upstream: ReturnType<typeof open<T>> | undefined;
      let replacement: ReturnType<typeof open<U>> | undefined;
      return {
        async next() {
          if (replacement) return replacement.next();
          try {
            upstream ??= open(source, boundary);
            const result = await upstream.next();
            if (failure) throw failure.error;
            if (result.done) await boundary.close();
            scope.check();
            return result;
          } catch (error) {
            scope.check();
            // Acquisition failures also pass through the boundary host.
            host.fail(error);
            await boundary.close();
            scope.check();
            const fallback = await withResourceContext(scope, (context) =>
              handler(failure!.error, context),
            );
            scope.check();
            replacement = open(fallback, scope);
            return replacement.next();
          }
        },
      };
    });
}
