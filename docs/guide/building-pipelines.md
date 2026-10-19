# Building a pipeline

From a list of tasks, select the first two unfinished tasks and trim the whitespace around their titles. Use separate operators to select items, transform them, and limit the count.

## Choose the processing order

<<< @/examples/building-pipelines.ts#pipeline

`filter` selects unfinished tasks, `map` trims their titles, and `take(2)` limits the result to two items. Completed tasks never reach `map`, so their titles are not processed.

Method calls connect stages in the order written. Here, `map` changes each task into a string, so `task.done` is no longer available afterward. Filter by task state before that transformation.

## Follow the values and types

In `Stream<T>`, `T` is the type of the value at that stage. Following each stage's output tells you what the next callback receives.

| Stage         | Value                     | Type                |
| ------------- | ------------------------- | ------------------- |
| `from(tasks)` | A task object             | `Stream<Task>`      |
| `filter(...)` | An unfinished task object | `Stream<Task>`      |
| `map(...)`    | A trimmed title           | `Stream<string>`    |
| `take(2)`     | At most two titles        | `Stream<string>`    |
| `.toArray()`  | An array of titles        | `Promise<string[]>` |

In this example, `filter` selects items, while `map` changes both the value and its type. `take` leaves the value type unchanged. After `await`, `result` is a `string[]`.

## Decide which items to count

`take(2)` counts values reaching its position in the pipeline. To select two unfinished tasks, put it after `filter`, as in the first example. If fewer than two unfinished tasks exist, the result contains fewer than two items.

Limiting the input first considers only the first two entries in the original array:

<<< @/examples/building-pipelines.ts#input-limit

The second task is already complete, so the result contains only `"Review draft"`. The later `"Book room"` task is not read. Place `take` according to whether you want to limit input items or matching items.

## Choose how to consume the result

Use `.toArray()` when you need the results together in an array. Here, `take(2)` limits that array to two items. Without a count limit, make sure all results can fit in memory.

If you do not need an array, process the same `titles` stream one item at a time with `forEach`:

<<< @/examples/building-pipelines.ts#consume

This call starts a separate execution that reads the `tasks` array again; it does not reuse the earlier `result`. `forEach` does not collect a result array and waits for each callback to finish, including asynchronous callbacks.

See [Terminal consumers](./consumers.md) for aggregation and predicate-based early termination, or [Concurrency, capacity, and demand](./concurrency.md) for concurrent asynchronous work. For external I/O, see [Cancellation and cleanup](./lifecycle.md) for stopping work and releasing resources.

## Connect custom operators

Use `.pipe(a, b)` for multiple custom operators. Each takes the preceding source and returns a new stream; fluent methods can continue afterward.

<<< @/examples/building-pipelines.ts#custom-operators
