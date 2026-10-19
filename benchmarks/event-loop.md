# Event loop yielding experiment

Keep default scheduling unchanged and use an opt-in `tap` recipe when synchronous work needs to share the host event loop. A dedicated yield operator did not improve throughput or timer delay over the count recipe in this experiment, so no public API was added.

```sh
pnpm build
node benchmarks/event-loop.mjs /tmp/event-loop.json
# Optional settings (defaults shown):
MOGAMI_YIELD_ITEMS=10000 MOGAMI_YIELD_SAMPLES=3 node benchmarks/event-loop.mjs /tmp/event-loop.json
```

The executable recipes are in [`docs/examples/event-loop.mjs`](../docs/examples/event-loop.mjs); the English and Japanese documentation show how to insert them. The experiment also contains a private prototype using the internal stream builder and scheduler. It is not an exported operator.

## Recorded run

Node.js v26.5.0, macOS arm64, Apple M1 Pro; 10,000 items, three measured rounds after one 256-item warmup per policy. All policies ran sequentially in one process; policy order rotates per round. No external I/O or manual GC. Each item goes through a synchronous `map` with 128 integer multiply/add steps, then the sink validates the value and order by recomputing the same work. Timing includes construction, consumption, validation, and cleanup. These are exploratory host-specific medians, not cross-runtime guarantees or statistical confidence intervals.

[Raw samples and environment](event-loop-results.json) were recorded on 2026-10-20 JST. Throughput and cancellation are separate runs.

| Policy                           |  Total ms | Items/s | First timer ms | Timer-to-cancelled settlement ms¹ | Abort-to-settlement ms | Items seen when cancel timer fired |
| -------------------------------- | --------: | ------: | -------------: | --------------------------------: | ---------------------: | ---------------------------------: |
| Default                          |     16.91 | 591,308 |          16.94 |                                 — |                      — |                             10,000 |
| No-op `tap` control              |     18.40 | 543,491 |          18.42 |                                 — |                      — |                             10,000 |
| Yield every item                 | 11,539.73 |     867 |           1.23 |                              1.31 |                  0.097 |                                  0 |
| Count 256                        |     65.51 | 152,658 |           1.15 |                              1.66 |                  0.137 |                                255 |
| Count 1,024                      |     29.56 | 338,341 |           1.88 |                              2.13 |                  0.090 |                              1,023 |
| Time 4 ms                        |     27.67 | 361,435 |           4.10 |                              4.21 |                  0.098 |                              2,040 |
| Dedicated prototype, count 1,024 |     30.20 | 331,179 |           1.89 |                              2.33 |                  0.166 |                              1,023 |

¹ From scheduling a zero-delay cancellation timer just before starting consumption to the rejected terminal promise after cleanup. The adjacent column measures from the actual abort callback instead. Default and control finished before that timer fired, so cancellation never interrupted their work and those latencies are unavailable. The first-timer column comes from full-throughput runs, while cancellation columns come from the separate interrupted runs; their medians should not be subtracted.

The count-1,024 recipe gives timers an earlier turn at a roughly 75% elapsed-time increase here (43% less throughput). Count 256 gives shorter turns with more timer overhead. The time budget reduces clock-dependent work per turn but does not provide a hard deadline; checking the clock and wrapping execution with `defer` also has a cost. A per-item timer has prohibitive overhead for this workload. Upstream or callbacks that already await I/O may not benefit from additional yields.

Use count-based yielding on hosts whose clock advances only after I/O, including Cloudflare Workers. Neither policy can preempt a long synchronous callback or recover timer fairness when placed after an operator that discards all its inputs. Place the yield stage before that work and tune it for the actual callback cost.

## Verification scope

`node --test --test-timeout=10000 test/event-loop-recipe.test.mjs` checks ordered output, Promise-valued item identity, simultaneous executions, demand without prefetch, cancellation during a yield, timer-driven cancellation before completion, upstream closure, and abort-listener removal. The recipe is also checked as JavaScript by `pnpm docs:typecheck`. These checks and measurements were executed on Node.js. The generated English and Japanese pages were checked in Chromium 155.0.8059.84, including the rendered code regions, internal links, and existing mobile reflow checks (28 pages, 81 internal URLs). Browser and Cloudflare execution of the recipe itself was not performed.
