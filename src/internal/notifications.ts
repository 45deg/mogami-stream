import type { Scope } from "../core/scope.js";
import { deferred } from "./deferred.js";

export function notifications(scope: Scope) {
  let event: ReturnType<typeof deferred<void>> | undefined;
  const notify = () => {
    const previous = event;
    event = undefined;
    previous?.resolve();
  };
  scope.signal.addEventListener("abort", notify, { once: true });
  scope.finalizers.push(() =>
    scope.signal.removeEventListener("abort", notify),
  );
  return {
    get promise() {
      event ??= deferred<void>();
      return event.promise;
    },
    notify,
  };
}
