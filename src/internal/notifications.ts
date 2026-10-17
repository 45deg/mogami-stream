import type { Scope } from "../core/scope.js";
import { deferred } from "./deferred.js";

export function notifications(scope: Scope) {
  let event = deferred<void>();
  const notify = () => {
    const previous = event;
    event = deferred();
    previous.resolve();
  };
  scope.signal.addEventListener("abort", notify, { once: true });
  scope.finalizers.push(() =>
    scope.signal.removeEventListener("abort", notify),
  );
  return {
    get promise() {
      return event.promise;
    },
    notify,
  };
}
