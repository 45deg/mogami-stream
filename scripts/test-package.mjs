import assert from "node:assert/strict";
import { selectPackageTests } from "./package-tests.mjs";
import { checkSourceMap } from "./check-source-map.mjs";
import { mkdtemp, mkdir, cp, readdir, rm, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { execFileSync } from "node:child_process";

const root = resolve(import.meta.dirname, "..");
const selectedTests = selectPackageTests(
  (await readdir(resolve(root, "test"))).filter((name) =>
    name.endsWith(".test.mjs"),
  ),
);
const temporary = await mkdtemp(resolve(tmpdir(), "mogami-package-"));
try {
  execFileSync("pnpm", ["pack", "--pack-destination", temporary], {
    cwd: root,
    stdio: "pipe",
  });
  const archive = (await readdir(temporary)).find((name) =>
    name.endsWith(".tgz"),
  );
  const archivePath = resolve(temporary, archive);
  const entries = execFileSync("tar", ["-tzf", archivePath], {
    encoding: "utf8",
  })
    .trim()
    .split("\n");
  for (const entry of entries)
    assert.ok(
      ["package/package.json", "package/README.md", "package/LICENSE"].includes(
        entry,
      ) || entry.startsWith("package/dist/"),
      `Unexpected package file: ${entry}`,
    );
  execFileSync("tar", ["-xzf", archivePath, "-C", temporary]);
  const packed = resolve(temporary, "package");
  for (const entry of entries.filter((entry) => entry.endsWith(".map")))
    await checkSourceMap(resolve(temporary, entry));
  assert.ok(
    entries.some((entry) => entry.endsWith(".map")),
    "Package has no source maps.",
  );
  const manifest = JSON.parse(
    await readFile(resolve(packed, "package.json"), "utf8"),
  );
  if (Object.keys(manifest.dependencies ?? {}).length)
    throw new Error("Packed runtime dependencies are not empty.");
  const publicImport = `
    import { range, map, toArray } from "mogami-stream";
    import { from as coreFrom, map as coreMap } from "mogami-stream/core";
    const coreValues = await coreFrom([1, 2]).pipe(coreMap(x => x * 2)).toArray();
    if (JSON.stringify(coreValues) !== "[2,4]" || "map" in coreFrom([])) throw new Error("Core package import failed.");
    const fluentValues = await range(0, 3).map(x => x * 2).take(2).toArray();
    if (JSON.stringify(fluentValues) !== "[0,2]") throw new Error("Fluent package import failed.");
    const values = await toArray(range(0, 3).pipe(map(x => x * 2)));
    if (JSON.stringify(values) !== "[0,2,4]") throw new Error("Public package import failed.");
  `;
  execFileSync(process.execPath, ["--input-type=module", "-e", publicImport], {
    cwd: packed,
    stdio: "inherit",
  });
  execFileSync("bun", ["-e", publicImport], { cwd: packed, stdio: "inherit" });
  await mkdir(resolve(packed, "test"));
  for (const name of [
    "helpers.mjs",
    "fuzz-helpers.mjs",
    "fixtures",
    ...selectedTests,
  ])
    await cp(resolve(root, "test", name), resolve(packed, "test", name), {
      recursive: true,
    });
  const tests = selectedTests.map((name) => resolve(packed, "test", name));
  execFileSync(process.execPath, ["--test", "--test-timeout=10000", ...tests], {
    cwd: packed,
    stdio: "inherit",
  });
  execFileSync("bun", ["test", ...tests], { cwd: packed, stdio: "inherit" });
  console.log(
    "Packed ESM distribution passed the Node.js and Bun contract suites.",
  );
} finally {
  await rm(temporary, { recursive: true, force: true });
}
