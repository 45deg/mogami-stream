# Development

Use Node.js 22+ and pnpm 11.1.1.

```sh
git clone https://github.com/45deg/mogami-stream.git
cd mogami-stream
pnpm install --frozen-lockfile
pnpm build
pnpm check
```

To use the checkout in another project, run `npm install /absolute/path/to/mogami-stream`. Rebuild after changing source files; package exports point to `dist/`.

`pnpm check` runs Oxlint, tests, type checks, distribution checks, and Oxfmt. Use `pnpm lint:fix` to apply lint fixes and `pnpm format` to format files. Generated files and `experiments/` are excluded from linting and formatting. Lint overrides allow intentional thenables, throwing getters, empty generators, and property accesses in contract and type tests.

## Documentation

```sh
pnpm docs:dev       # Generate API/assets and start VitePress
pnpm docs           # Type-check examples and build the site
pnpm docs:preview   # Preview at /mogami-stream/
pnpm docs:test      # Build and check the site in Chromium
```

Edit matching pages under `docs/` and `docs/ja/` together. Shared snippets live in `docs/examples/` and are checked by `pnpm docs:typecheck`. Operator descriptions live in `docs/.vitepress/theme/components/operators.ts`; `docs:prepare` runs operator examples against `expected.json`. Restart `docs:dev` after changing API comments, operator examples, or benchmark assets.

`pnpm docs:api` generates TypeDoc in `docs/api/`; the complete site is written to `docs/.vitepress/dist/`. Generated files are ignored by Git. Local search covers guides and the operator catalog; TypeDoc has its own search.

The agent-facing index is `docs/public/llms.txt`. Keep its summary and links in sync when changing documentation. It follows the [llms.txt proposal](https://llmstxt.org/): a project heading, a blockquote summary, and sections of annotated links; `Optional` contains secondary resources. Links point to rendered pages so code includes and Vue-based operator descriptions are available. VitePress copies the file unchanged to the build output, and the existing GitHub Pages workflow publishes it at `/mogami-stream/llms.txt`. VitePress pages in both languages include a `rel="describedby"` link to this shared English index.

## Runtime checks

```sh
pnpm test:bun
pnpm test:package
pnpm exec playwright install chromium
pnpm test:browser
pnpm test:cloudflare
pnpm test:runtime-watchdog
```

The browser runner checks Chromium main threads, Dedicated Workers, and Service Workers. It uses local Google Chrome on macOS when available, otherwise Playwright Chromium; override with `PLAYWRIGHT_EXECUTABLE_PATH`. Cloudflare checks run locally in Miniflare/workerd. Both runners have a 120-second watchdog, configurable with `MOGAMI_RUNTIME_TIMEOUT_MS`.

For virtual-clock tests, use the internal `createTestClock()` and `iterateWithScheduler` helpers. See existing timing tests for `advanceBy`, `advanceTo`, and `pendingTimers` usage.

See [benchmark instructions](benchmarks/README.md) for workloads and reproduction commands.
