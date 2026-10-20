## Documentation & Comment Style (EN / JA)

Write direct, concrete engineering prose. Optimize for signal per word.

### 1. Core Principles

- **Lead with purpose and behavior:** State what an API does and why you would use it in the first sentence (e.g., _"For tests and mocks, `fromTimeline(events)` replays..."_).
- **Cut throat-clearing and marketing filler:** Never open with _"In modern applications..."_, _"When working with streams, it is important to..."_, or adverbs like _simply, easily, seamlessly, effortlessly, robustly_.
- **Ban AI buzzwords:** Do not use _delve, tapestry, leverage (as a verb), utilize, harness, empowers, game-changer, comprehensive, cutting-edge, under the hood_. Use `use`, `run`, `build`, `limit`, `cancel`.
- **Keep legitimate caveats natural:** Use natural transitions like _"Note that..."_ when flagging subtle edge cases or behavioral boundaries. Do not strip connecting words to the point of robotic, telegraphic choppiness.

### 2. Avoid "Reverse Slop" (Defensive / Audit-Log Boilerplate)

- **No self-evident disclaimers:** Do not state that application recipes _"belong to the application and are not exported by the library"_, or that benchmark numbers _"are not cross-runtime statistical guarantees"_.
- **No PR/verification logs in docs:** Keep test-run logs, commit hashes, and _"Browser execution was not performed"_ out of user-facing guides and benchmark readmes.
- **No spec-lawyer jargon when plain terms work:** Prefer _"in memory"_ or _"within the same process"_ over _"in the same JavaScript realm"_.

### 3. English Prose (`README.md`, `docs/`, JSDoc)

- Prefer active voice and concrete verbs: _"closes upstream"_ instead of _"is responsible for performing upstream closure"_.
- Introduce code examples in one sentence without restating every parameter already visible in the code block.
- Do not hard-wrap Markdown paragraphs at 80 columns; let the renderer wrap lines.

### 4. Japanese Prose (`docs/ja/`)

- **Write idiomatic technical Japanese, not literal translations:** Restructure English noun chains and passive clauses into natural Japanese flow.
- **No unspaced raw English fragments:** Write `上流の読み取り` and `再試行（retry）` instead of `上流read` or `retryや`.
- **Consistent terminology:** Use consistent terms across pages (e.g., `ステージ` instead of mixing `段` and `ステージ`; `クリーンアップ` or `後処理` consistently within a section).
- **100% EN/JA parity:** Update `docs/` and `docs/ja/` together so behavioral contracts, edge cases, and links never drift.

### 5. Code Comments & JSDoc

- **Inline comments explain _why_, not _what_:** Only comment non-obvious invariants, ordering constraints, or cancellation hazards.
- **JSDoc states the contract:** Specify bounds, error conditions, and lifecycle ownership concisely without repeating the TypeScript type signature in prose.
