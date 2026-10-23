import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

function run(args) {
  const child = spawnSync(process.execPath, ["benchmarks/route.mjs", ...args], {
    cwd: new URL("../", import.meta.url),
    encoding: "utf8",
    timeout: 15_000,
  });
  assert.ifError(child.error);
  return child;
}

test("route benchmark verifies all workloads using independent alternating processes", () => {
  const directory = mkdtempSync(join(tmpdir(), "mogami-route-bench-"));
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
    assert.equal(report.results.length, 32);
    assert.equal(report.results[0].variant, "baseline");
    assert.equal(report.results[16].variant, "candidate");
    assert.equal(report.results[0].case, report.results.at(-1).case);
    for (const result of report.results) {
      assert.equal(result.samples.length, 2);
      assert.ok(result.elapsedMs.median > 0);
      assert.ok(result.cpuMs.median >= 0);
      for (const sample of result.samples) {
        assert.equal(sample.delivered.length, sample.checksums.length);
        assert.ok(
          sample.delivered.reduce((total, count) => total + count, 0) > 0,
        );
      }
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("route benchmark rejects output reordered within a branch despite matching checksums", () => {
  const directory = mkdtempSync(join(tmpdir(), "mogami-route-oracle-"));
  try {
    const module = join(directory, "reordered.mjs");
    writeFileSync(
      module,
      `
      import { route as actual } from ${JSON.stringify(new URL("../dist/index.js", import.meta.url).href)};
      export function route(source, options) {
        return actual((function* () {
          for (const value of source) yield value === 0 ? 2 : value === 2 ? 0 : value;
        })(), options);
      }
    `,
    );
    const child = run([
      `--module=${module}`,
      "--case=numeric-pair",
      "--items=4",
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

test("route benchmark rejects malformed and unknown CLI options", () => {
  for (const args of [
    ["--items=0"],
    ["--items=1.5"],
    ["--warmup=-1"],
    ["--samples=0"],
    ["--trials=Infinity"],
    ["--case=missing"],
    ["--items=1", "--items=2"],
    ["--unknown=1"],
    ["--items"],
  ]) {
    const child = run(args);
    assert.notEqual(child.status, 0);
    assert.match(child.stderr, /Invalid|Unknown/);
  }
});
