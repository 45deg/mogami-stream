# Event loop yielding experiment

Keep default scheduling unchanged and use an opt-in `tap` recipe when synchronous work needs to share the host event loop. The count recipe and the dedicated yield prototype have similar elapsed times in this experiment, so no public API was added.

```sh
pnpm build
node benchmarks/event-loop.mjs /tmp/event-loop.json
# Optional settings (defaults shown):
MOGAMI_YIELD_ITEMS=10000 MOGAMI_YIELD_SAMPLES=3 node benchmarks/event-loop.mjs /tmp/event-loop.json
```

The executable recipes are in [`docs/examples/event-loop.mjs`](../docs/examples/event-loop.mjs); the English and Japanese documentation show how to insert them. The experiment also contains a private prototype using the internal stream builder and scheduler. It is not an exported operator.

## Recorded run

Node.js v26.13.0, macOS arm64, Apple M1 Pro; 10,000 items, three measured rounds after one 256-item warmup per policy. All policies ran sequentially in one process; policy order rotates per round. No external I/O or manual GC. Each item goes through a synchronous `map` with 128 integer multiply/add steps, then the sink validates the value and order by recomputing the same work. Timing includes construction, consumption, validation, and cleanup.

[Raw samples and environment](event-loop-results.json) were recorded on 2026-10-29 JST. Throughput and cancellation are separate runs.

| Policy                           |  Total ms | Items/s | First timer ms | Timer-to-cancelled settlement ms¹ | Abort-to-settlement ms | Items seen when cancel timer fired |
| -------------------------------- | --------: | ------: | -------------: | --------------------------------: | ---------------------: | ---------------------------------: |
| Default                          |     14.20 | 704,461 |          14.24 |                                 — |                      — |                             10,000 |
| No-op `tap` control              |     15.30 | 653,442 |          15.32 |                                 — |                      — |                             10,000 |
| Yield every item                 | 12,327.35 |     811 |           1.17 |                              1.46 |                  0.201 |                                  0 |
| Count 256                        |     58.00 | 172,399 |           1.08 |                              1.78 |                  0.110 |                                255 |
| Count 1,024                      |     27.67 | 361,414 |           1.71 |                              2.32 |                  0.088 |                              1,023 |
| Time 4 ms                        |     28.14 | 355,350 |           4.05 |                              4.22 |                  0.099 |                              2,023 |
| Dedicated prototype, count 1,024 |     27.11 | 368,875 |           2.06 |                              1.86 |                  0.059 |                              1,023 |

¹ Measured from scheduling a zero-delay cancellation timer just before starting consumption to the rejected terminal promise after cleanup. The adjacent column measures from the actual abort callback instead. Default and control finished before that timer fired, so cancellation never interrupted their work. The first-timer column comes from full-throughput runs, while the cancellation columns come from separate interrupted runs; their medians should not be subtracted.

The count-1,024 recipe gives timers an earlier turn at roughly a 95% elapsed-time increase on this workload (49% lower throughput). Count 256 gives shorter turns with higher timer overhead. The time budget bounds clock-dependent work per turn without enforcing a hard deadline; checking the clock and wrapping execution with `defer` also adds overhead. Yielding on every item is prohibitively slow for fine-grained items, and pipelines that already await I/O generally do not need explicit yields.

Use count-based yielding on hosts whose clock advances only after I/O, such as Cloudflare Workers. Neither policy can preempt a single blocking synchronous call or restore responsiveness if placed downstream of an operator that consumes many inputs before emitting. Place the yield stage before the synchronous work and tune the interval to match per-item cost.

## Tests

Run `node --test --test-timeout=10000 test/event-loop-recipe.test.mjs` to verify ordered output, Promise-valued item identity, concurrent executions, demand-driven pulling without prefetch, cancellation during a yield, timer-driven cancellation before completion, upstream closure, and abort-listener cleanup.
