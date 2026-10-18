import { create, type Stream } from "../core/stream.js";
import { done, item } from "../internal/deferred.js";
import { duration } from "../internal/errors.js";

export type TimelineEvent<T> =
  | { readonly at: number; readonly kind: "value"; readonly value: T }
  | { readonly at: number; readonly kind: "complete" }
  | { readonly at: number; readonly kind: "error"; readonly error: unknown };

export function fromTimeline<T>(
  events: readonly TimelineEvent<T>[],
): Stream<T> {
  // Snapshot the schedule so later caller edits cannot invalidate its ordering.
  const timeline = events.map((event) => ({ ...event }));
  let previous = 0;
  for (let index = 0; index < timeline.length; index++) {
    const event = timeline[index]!;
    duration(event.at, "at");
    if (event.at < previous)
      throw new RangeError("Timeline times must be non-decreasing.");
    previous = event.at;
    if (
      event.kind !== "value" &&
      event.kind !== "complete" &&
      event.kind !== "error"
    ) {
      throw new TypeError("Unknown timeline event kind.");
    }
    if (event.kind !== "value" && index !== timeline.length - 1) {
      throw new RangeError("A terminal timeline event must be last.");
    }
  }
  return create((scope) => {
    const start = scope.scheduler.now();
    let index = 0;
    let ended = false;
    return {
      async next() {
        if (ended) return done();
        const event = timeline[index++];
        if (!event) return done();
        const remaining = event.at - (scope.scheduler.now() - start);
        if (remaining > 0) await scope.scheduler.sleep(remaining, scope.signal);
        scope.check();
        switch (event.kind) {
          case "value":
            return item(event.value);
          case "complete":
            ended = true;
            return done();
          case "error":
            ended = true;
            throw event.error;
        }
      },
    };
  });
}
