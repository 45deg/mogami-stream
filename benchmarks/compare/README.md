# Library comparison

```sh
pnpm bench:compare --json=/tmp/mogami-compare.json
pnpm bench:compare --case=io-stream --samples=11 --seed=43
```

Runs the shared benchmark suite against third-party libraries using the methodology, workload contracts, correctness checks, and CLI flags documented in [`../README.md`](../README.md). Third-party packages in this workspace are used only for benchmarking; `mogami-stream` itself has zero runtime dependencies.
