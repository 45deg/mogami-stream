import assert from "node:assert/strict";
import { readdir, readFile } from "node:fs/promises";
import { resolve, dirname } from "node:path";
import ts from "typescript";
import { checkSourceMap } from "./check-source-map.mjs";

const root = resolve(import.meta.dirname, "..");
const dist = resolve(root, "dist");
const manifest = JSON.parse(
  await readFile(resolve(root, "package.json"), "utf8"),
);
assert.equal(
  Object.keys(manifest.dependencies ?? {}).length,
  0,
  "Runtime dependencies must remain empty.",
);
const forbidden = new Set(["process", "Buffer", "window", "document"]);
const config = ts.getParsedCommandLineOfConfigFile(
  resolve(root, "tsconfig.json"),
  {},
  ts.sys,
);
assert.ok(
  config && config.errors.length === 0,
  "Invalid compiler configuration.",
);
const expected = new Set(
  config.fileNames.flatMap((file) =>
    ts.getOutputFileNames(config, file, false),
  ),
);
let checked = 0;
async function inspect(directory) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = resolve(directory, entry.name);
    if (entry.isDirectory()) {
      await inspect(path);
      continue;
    }
    assert.ok(expected.delete(path), `Unexpected distribution file: ${path}`);
    if (entry.name.endsWith(".map")) await checkSourceMap(path);
    if (!entry.name.endsWith(".js") && !entry.name.endsWith(".d.ts")) continue;
    const source = ts.createSourceFile(
      path,
      await readFile(path, "utf8"),
      ts.ScriptTarget.Latest,
      true,
    );
    function visit(node) {
      if (ts.isIdentifier(node))
        assert.ok(
          !forbidden.has(node.text),
          `Forbidden runtime global ${node.text}: ${path}`,
        );
      if (
        (ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) &&
        node.moduleSpecifier
      ) {
        const specifier = node.moduleSpecifier.text;
        assert.ok(
          specifier.startsWith("."),
          `External import ${specifier}: ${path}`,
        );
        assert.ok(
          resolve(dirname(path), specifier).startsWith(dist + "/"),
          `Import escapes the package: ${specifier}`,
        );
      }
      if (
        ts.isCallExpression(node) &&
        (node.expression.kind === ts.SyntaxKind.ImportKeyword ||
          (ts.isIdentifier(node.expression) &&
            node.expression.text === "require"))
      ) {
        throw new Error(`Dynamic module loading is not permitted: ${path}`);
      }
      ts.forEachChild(node, visit);
    }
    visit(source);
    checked++;
  }
}
await inspect(dist);
assert.equal(
  expected.size,
  0,
  `Missing distribution files: ${[...expected].join(", ")}`,
);
const originalTimeout = globalThis.setTimeout;
let timers = 0;
globalThis.setTimeout = (...args) => {
  timers++;
  return originalTimeout(...args);
};
try {
  await import("../dist/index.js");
} finally {
  globalThis.setTimeout = originalTimeout;
}
assert.equal(timers, 0, "Importing the entry point must not start timers.");
console.log(
  `${checked} distribution files checked; no external imports, host globals, or import-time timers.`,
);
