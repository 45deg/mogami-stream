# Library comparison

```sh
pnpm bench:compare --json=/tmp/mogami-compare.json
pnpm bench:compare --case=io-stream --samples=11 --seed=43
```

The comparison uses the shared [workload methodology](../README.md). That document defines the matching contracts, supported libraries, correctness checks, metrics, options, and limitations. All dependencies in this workspace are benchmark-only and pinned; Mogami has no runtime dependencies.
