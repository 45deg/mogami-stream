# API requests

Pass URLs as an async iterable and fetch up to five concurrently. Consume results in completion order without keeping every result in memory.

<<< @/examples/api-requests.ts

[`rateLimit`](../operators.md#operator-ratelimit) allows at most five upstream read starts per second in this execution. Pass the callback's `signal` to `fetch` for cancellation. See [Retries](../guide/errors.md#retry) to add retry handling.
