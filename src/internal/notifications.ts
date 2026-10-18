import type { Scope } from "../core/scope.js";
import { deferred } from "./deferred.js";

export function notifications(scope: Scope) {
  let event: ReturnType<typeof deferred<void>> | undefined;
  let version = 0;
  const notify = () => {
    version++;
    const previous = event;
    event = undefined;
    previous?.resolve();
  };
  scope.signal.addEventListener("abort", notify, { once: true });
  scope.finalizers.push(() =>
    scope.signal.removeEventListener("abort", notify),
  );
  return {
    get version() {
      return version;
    },
    get promise() {
      event ??= deferred<void>();
      return event.promise;
    },
    notify,
  };
}
