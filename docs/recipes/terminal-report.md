# Collecting a result with its report

When an application needs both a terminal result and any secondary errors (`suppressedErrors`), acquire the iterator with `iterate()` and pass that same iterator to `toArray`. The helper below returns a fulfilled or rejected outcome alongside the execution's `CloseReport`, waiting for cleanup in both cases:

<<< @/examples/terminal-report.ts#collect

## Handling results and errors

Branch on `status` rather than the truthiness of `reason`, since JavaScript permits throwing `undefined`. A rejected outcome preserves the primary failure or cancellation reason, while `report.suppressedErrors` collects secondary processing and cleanup errors (a cleanup-only failure also produces a rejected outcome). Because `collectWithReport` resolves rather than throwing stream errors, always inspect `status`.

To replace `await source.toArray(options)`, call `await collectWithReport(source, options)`, inspect `report`, and then return `value` or rethrow `reason` based on `status`. Pass `{ signal }` to the helper so it governs the underlying iterator, and do not consume that iterator concurrently or start a second execution just to read `closed`.

## Including callback errors

Passing an already-created `StreamIterator` into `reduce`, `forEach`, `find`, `some`, or `every` wraps it in a new outer execution, so errors thrown by that consumer's callback are not recorded on the inner iterator's `closed` report. If you need a single `CloseReport` that covers your callbacks as well, run those callbacks inside pipeline operators such as `map` or `tap` before calling `iterate()`.

Use the built-in terminal methods directly whenever you only need the final result or primary error.

## Timeout notification before cleanup completes

Because terminal consumers wait for cleanup before settling, they cannot notify you of a timeout while an uncooperative callback is still running. To observe a timeout immediately while still awaiting teardown later, separate `iterator.next()` from `iterator.closed`:

<<< @/examples/terminal-report.ts#timeout

The pending `next()` call rejects with `TimeoutError` as soon as the timeout fires, whereas `closed` resolves only after the in-flight work settles and `finalize` completes—recording both late failures in the report. Note that `Promise.race` around a terminal consumer neither cancels the execution nor waits for cleanup; always attach a rejection handler to `next()` and eventually await `closed`.

See [CloseReport](../guide/errors.md#cleanup-errors) for termination states and error precedence.
