import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

function run(args) {
  const child = spawnSync(process.execPath, ["benchmarks/fork.mjs", ...args], {
    cwd: new URL("../", import.meta.url),
    encoding: "utf8",
    timeout: 10_000,
  });
  assert.ifError(child.error);
  return child;
}

test("fork benchmark runs all workloads with independent alternating comparisons", () => {
  const directory = mkdtempSync(join(tmpdir(), "mogami-fork-bench-"));
  try {
    const json = join(directory, "results.json");
    const child = run([
      "--items=131",
      "--warmup=1",
      "--samples=2",
      "--trials=2",
      "--baseline=dist/index.js",
      `--json=${json}`,
    ]);
    assert.equal(child.status, 0, child.stderr);
    const report = JSON.parse(readFileSync(json, "utf8"));
    assert.equal(report.results.length, 20);
    assert.equal(report.results[0].variant, "baseline");
    assert.equal(report.results[10].variant, "candidate");
    assert.equal(report.results[0].case, report.results.at(-1).case);
    for (const result of report.results) {
      assert.equal(result.samples.length, 2);
      assert.ok(result.elapsedMs.median > 0);
      assert.ok(result.cpuMs.median >= 0);
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("fork benchmark rejects reordered output with the same count and checksum", () => {
  const directory = mkdtempSync(join(tmpdir(), "mogami-fork-oracle-"));
  try {
    const module = join(directory, "reordered.mjs");
    writeFileSync(
      module,
      `
      import { fork as actual } from ${JSON.stringify(new URL("../dist/index.js", import.meta.url).href)};
      export function fork(source, options) {
        return actual((function* () {
          for (const value of source) yield value === 0 ? 1 : value === 1 ? 0 : value;
        })(), options);
      }
    `,
    );
    const child = run([
      `--module=${module}`,
      "--case=sync-pair",
      "--items=3",
      "--warmup=0",
      "--samples=1",
      "--trials=1",
    ]);
    assert.notEqual(child.status, 0);
    assert.match(child.stderr, /AssertionError/);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
