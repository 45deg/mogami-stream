# Benchmarks

Async iteration and operator overhead for 100,000 inputs. Bars show median elapsed seconds across 7 runs after 5 warmups; shorter is faster. Each panel has its own scale.

![Sequential async map; concurrent ordered map; 13-stage map/filter pipeline. Node.js and Bun elapsed times in seconds.](/benchmarks.svg)

Node.js v24.22.0 / Bun v1.4.3 · Apple M1 Pro · Normal GC.

| Chart                        | Workload                                                                        |
| ---------------------------- | ------------------------------------------------------------------------------- |
| Sequential async map         | `concurrency: 1`, `capacity: 1`                                                 |
| Concurrent ordered map       | `concurrency: 8`, `capacity: 16`, input-order output                            |
| 13-stage map/filter pipeline | ID projection followed by six increment/filter pairs, excluding multiples of 97 |

Inputs come from an async generator. Map callbacks await `Promise.resolve()`; the workloads contain no timer or network waits.

## Reproduction

Requires Node.js, Bun, pnpm, and uv.

```sh
pnpm install --frozen-lockfile
pnpm bench:svg

# Re-render saved results:
pnpm bench:svg --reuse
```

The chart uses the default normal GC condition (`--gc=none`). See [benchmark methodology and CLI options](https://github.com/45deg/mogami-stream/blob/main/benchmarks/README.md) for individual workloads and JSON output.
