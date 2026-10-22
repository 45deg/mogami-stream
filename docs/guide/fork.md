# Sharing a source with fork

`fork(source, { branches, capacity })` sends each source value to every active branch, in source order, using one source execution. It is available from both `mogami-stream` and `mogami-stream/core`.

<<< @/examples/fork.ts

Start all consumers before awaiting any one of them. A branch participates from construction, even before its iterator is acquired. Once its queue fills, it blocks new source reads. Always detach unused branches, including when `take(0)` or a setup failure prevents consumption. Do not hold a shared lock or worker slot while waiting for a sibling that needs that resource.

## Capacity and ownership

`branches`, `capacity`, and their product must be positive safe integers. Invalid bounds throw `RangeError` before source acquisition. Construction is lazy; reading starts with the first branch demand. Each branch holds at most `capacity` queued values plus reservations, and the group has at most one source read in flight. Existing queues remain readable when another branch is full. Idle consumers do not trigger prefetch.

Values, including Promise-valued items, are shared by reference without cloning or awaiting them. Treat shared payloads as immutable. Capacity counts items, not bytes or downstream effects; bound chunk sizes, downstream concurrency, and collected results separately.

Each branch permits one iterator acquisition through `iterate()` or `[Symbol.asyncIterator]()`. A second acquisition throws `TypeError`; overlapping `next()` calls reject with `TypeError`. `from(branch)` adds stream operators but does not make the branch reusable. The group owns the source iterator exclusively; do not consume it elsewhere.

## Departure and cancellation

- `branch.detach()`, iterator `return()`, and a `for await` break release that endpoint immediately. Siblings continue.
- `branch.cancel(reason)` or `iterate({ signal })` cancels only that endpoint. Its pending or next read rejects once; subsequent reads return done.
- Iterator `throw(error)` fails that endpoint and rejects immediately. It does not forward `throw` to the source.
- When the last branch leaves, the group stops the source. A failed or cancelled last branch cancels the source with its reason.
- `group.cancel(reason)` or the group signal cancels every active branch before waiting for cleanup. `group.close()` detaches every active branch and waits for cleanup.

Already aborted signals and stopping before the first read never acquire the source. Termination is idempotent. Cancellation does not interrupt downstream effects automatically; pass a shared signal to the consumers and their I/O when they must stop together.

## Completion reports

`branch.closed` and its iterator's `closed` report endpoint completion without waiting for siblings, downstream callbacks, or shared source cleanup. At EOF, queued values drain and each empty endpoint completes independently. Source read or cleanup failures fail endpoints that are still active and discard their queues; already completed reports stay unchanged.

`group.closed` waits for every endpoint, outstanding source work, and source cleanup. `group.close()` returns that same report promise. Both resolve without rejection. Inspect `report.source` even if every consumer succeeded: shared cleanup can fail after the branches finish. `report.branches` follows creation order and describes endpoints, not downstream effects. Primary reasons and secondary failures follow the [cleanup error rules](./errors.md#cleanup-errors).

Awaiting a completion promise does not stop anything. An unread queue can keep `group.closed` pending after EOF. Uncooperative source work can keep it pending after cancellation, while detached endpoints still finish. Never await `group.closed` inside a source finalizer, since the group waits for that finalizer.

The hub periodically yields during sustained source reads so timers and abort handlers can run. Consumers must still release shared resources and make progress; no queue size can resolve a dependency cycle. See [event loop yielding](../recipes/event-loop.md) for downstream work.
