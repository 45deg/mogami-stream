# Branching a stream

`broadcast` and `route` create multiple branches from one source execution, preserving input order within each branch.

| API                       | Destination                                   |
| ------------------------- | --------------------------------------------- |
| [`broadcast`](#broadcast) | Send the same value to every branch           |
| [`route`](#route)         | Send each item to at most one selected branch |

A numeric `branches` creates an array; an array of string keys creates an object keyed by branch name. Both APIs support function and fluent forms.

## broadcast

<<< @/examples/operators/broadcast.ts#example

Values share the same references without copying. Do not mutate shared values.

## route

`select` returns the destination's number or key. Routing runs one item at a time in input order and may be asynchronous.

<<< @/examples/operators/route.ts#example

### Predicates and type guards

Instead of `select`, pass predicates as `branches`. The first match in `Object.keys(branches)` order chooses the destination.

```ts
const group = from(events).route({
  branches: {
    image: (event): event is ImageEvent => event.kind === "image",
    text: (event): event is TextEvent => event.kind === "text",
  },
  capacity: 8,
});
```

A synchronous type guard narrows its branch. Ordinary boolean or Promise-returning predicates and the `default` branch keep the input type.

### default and fallback

No match or a departed destination sends the item to `default`. A match on a departed branch does not try the next predicate. If `default` is also unavailable, `fallback` applies.

| Setting                            | Behavior                         |
| ---------------------------------- | -------------------------------- |
| `default: "other"`                 | Send the item to `other` instead |
| `fallback: "ignore"` (the default) | Discard the item and continue    |
| `fallback: "error"`                | Fail the group with `RouteError` |

Invalid destinations and callback errors or rejections fail the group regardless of `fallback`. An item already sent is never rerouted after departure.

`route` also passes values without copying. If the input contains the same object more than once, branches can still share references.

## Capacity and demand

`capacity` bounds the number of items per branch.

| API         | What counts toward the bound                                                                    |
| ----------- | ----------------------------------------------------------------------------------------------- |
| `broadcast` | Queued values + read reservations                                                               |
| `route`     | Queued values, plus one separate slot across the group for reading, classification, or delivery |

Any full branch stops upstream in `broadcast`; a full selected destination stops it in `route`. Routing waits rather than using an empty sibling, blocking later input items too. No new reads start without demand.

Read all branches concurrently and detach unused ones. Even unread branches can stop upstream. Capacity does not bound bytes or downstream work.

## Completion and cancellation

Each branch can be consumed once. Branches from `mogami-stream` support fluent operators; wrap branches from `mogami-stream/core` with `from(branch)`.

- `branch.detach()` and a `for await` break end that branch and discard unread items. Siblings continue.
- `branch.cancel(reason)` cancels one branch; `group.cancel(reason)` cancels all of them. Once all branches leave, upstream stops too.
- `group.close()` detaches every branch and waits for outstanding reads, routing callbacks, and cleanup.

`branch.closed` waits for that branch; `group.closed` waits for all branches and shared work. Check `report.source` for cleanup failures. See [cancellation and cleanup](./lifecycle.md).

[broadcast](/mogami-stream/api/functions/broadcast.html){target="_self"} · [route](/mogami-stream/api/functions/route.html){target="_self"}
