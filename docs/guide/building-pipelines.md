# Building a pipeline

From a list of tasks, select the first two unfinished tasks and trim the whitespace around their titles. Use separate operators to select items, transform them, and limit the count.

## Operator order

<<< @/examples/building-pipelines.ts#pipeline

[`filter`](../operators.md#operator-filter) selects unfinished tasks, [`map`](../operators.md#operator-map) trims their titles, and [`take(2)`](../operators.md#operator-take) limits the result to two items. Completed tasks never reach [`map`](../operators.md#operator-map), so their titles are not processed.

Operators are applied in the order of the method calls. Here, [`map`](../operators.md#operator-map) changes each task into a string, so `task.done` is no longer available afterward. Filter by task state before that transformation.

## Values and types at each stage

In `Stream<T>`, `T` is the type of the value at that stage. Following each stage's output tells you what the next callback receives.

| Stage                                            | Value                     | Type                |
| ------------------------------------------------ | ------------------------- | ------------------- |
| [`from(tasks)`](../operators.md#operator-from)   | A task object             | `Stream<Task>`      |
| [`filter(...)`](../operators.md#operator-filter) | An unfinished task object | `Stream<Task>`      |
| [`map(...)`](../operators.md#operator-map)       | A trimmed title           | `Stream<string>`    |
| [`take(2)`](../operators.md#operator-take)       | At most two titles        | `Stream<string>`    |
| [`.toArray()`](../operators.md#operator-toarray) | An array of titles        | `Promise<string[]>` |

In this example, [`filter`](../operators.md#operator-filter) selects items, while [`map`](../operators.md#operator-map) changes both the value and its type. [`take`](../operators.md#operator-take) leaves the value type unchanged. After `await`, `result` is a `string[]`.

## `take` placement and item counts

[`take(2)`](../operators.md#operator-take) counts values reaching its position in the pipeline. To select two unfinished tasks, put it after [`filter`](../operators.md#operator-filter), as in the first example. If fewer than two unfinished tasks exist, the result contains fewer than two items.

Limiting the input first considers only the first two entries in the original array:

<<< @/examples/building-pipelines.ts#input-limit

The second task is already complete, so the result contains only `"Review draft"`. The later `"Book room"` task is not read. Place [`take`](../operators.md#operator-take) according to whether you want to limit input items or matching items.

## Array collection and per-item processing

Use [`.toArray()`](../operators.md#operator-toarray) when you need the results together in an array. Here, [`take(2)`](../operators.md#operator-take) limits that array to two items. Without a count limit, make sure all results can fit in memory.

If you do not need an array, process the same `titles` stream one item at a time with [`forEach`](../operators.md#operator-foreach):

<<< @/examples/building-pipelines.ts#consume

This call starts a separate execution that reads the `tasks` array again. [`forEach`](../operators.md#operator-foreach) does not collect a result array and waits for each callback to finish, including asynchronous callbacks.

See [Terminal consumers](./consumers.md) for aggregation and predicate-based early termination, or [Concurrency, capacity, and demand](./concurrency.md) for concurrent asynchronous work. For external I/O, see [Cancellation and cleanup](./lifecycle.md) for stopping work and releasing resources.

## `.pipe()` and custom operators

Use [`.pipe(a, b)`](../operators.md#operator-pipe) for multiple custom operators. Each takes the preceding source and returns a new stream; fluent methods can continue afterward.

<<< @/examples/building-pipelines.ts#custom-operators

## Entry points and type inference {#entry-points}

`mogami-stream/core` provides standalone operators, [`.pipe()`](../operators.md#operator-pipe), and terminal methods. It omits fluent transformation methods such as [`.map()`](../operators.md#operator-map), allowing bundlers to remove unused operators.

<<< @/examples/core.ts

Both entries share execution and cleanup. Pass a core stream to the default entry's [`from()`](../operators.md#operator-from) to use fluent transformations; importing that entry alone does not change existing streams.

Fluent chains have no stage limit. Each [`.pipe()`](../operators.md#operator-pipe) or standalone [`pipe`](../operators.md#operator-pipe) call infers up to sixteen operators; split longer pipelines across calls. Value-independent operators such as `const first = take(1)` remain reusable across element types.
