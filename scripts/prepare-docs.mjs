import {
  copyFile,
  cp,
  mkdir,
  readFile,
  readdir,
  rm,
  writeFile,
} from "node:fs/promises";
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { format } from "oxfmt";
import { createMarkdownRenderer } from "vitepress";

const root = new URL("../", import.meta.url);
const publicDir = new URL("docs/public/", root);
await mkdir(publicDir, { recursive: true });
await rm(new URL("api/", publicDir), { recursive: true, force: true });
await cp(new URL("docs/api/", root), new URL("api/", publicDir), {
  recursive: true,
});
await copyFile(
  new URL("benchmarks/results/readme/comparison.svg", root),
  new URL("benchmarks.svg", publicDir),
);

// Display only examples whose results have been checked against the library.
const sourceDir = new URL("docs/examples/operators/", root);
const cacheDir = new URL("docs/.vitepress/cache/operator-examples/", root);
const generated = new URL("docs/.vitepress/generated/", root);
await mkdir(cacheDir, { recursive: true });
await mkdir(generated, { recursive: true });
const expected = JSON.parse(
  await readFile(new URL("expected.json", sourceDir), "utf8"),
);
const files = (await readdir(sourceDir))
  .filter((file) => file.endsWith(".ts"))
  .sort();
assert.deepEqual(
  files.map((file) => file.slice(0, -3)),
  Object.keys(expected).sort(),
);
const markdown = await createMarkdownRenderer(
  fileURLToPath(new URL("docs/", root)),
);
const examples = {};
for (const file of files) {
  const name = file.slice(0, -3);
  const source = await readFile(new URL(file, sourceDir), "utf8");
  const exportStart = source.lastIndexOf("\nexport { result };");
  assert.ok(
    exportStart >= 0,
    `${file} must export its result after the example`,
  );
  const code = await format(
    file,
    source
      .slice(0, exportStart)
      .replace(/^\/\/ #(?:end)?region[^\n]*\n/gm, "") +
      "\nconsole.log(result);\n",
    { printWidth: 80 },
  );
  assert.deepEqual(code.errors, [], `Could not format example: ${name}`);
  const executable = ts.transpileModule(source, {
    compilerOptions: {
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.ESNext,
    },
  }).outputText;
  const moduleUrl = new URL(`${name}.mjs`, cacheDir);
  await writeFile(moduleUrl, executable);
  let timer;
  try {
    const { result } = await Promise.race([
      import(moduleUrl.href),
      new Promise((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`Example timed out: ${name}`)),
          5_000,
        );
      }),
    ]);
    assert.deepEqual(
      result,
      expected[name],
      `Incorrect example result: ${name}`,
    );
    const formattedResult = await format(
      "result.json",
      JSON.stringify(result),
      {
        printWidth: 72,
      },
    );
    assert.deepEqual(
      formattedResult.errors,
      [],
      `Could not format result: ${name}`,
    );
    examples[name] = {
      html: markdown.render(`\`\`\`ts\n${code.code}\n\`\`\``),
      result: formattedResult.code,
    };
  } finally {
    clearTimeout(timer);
  }
}
await writeFile(
  new URL("operator-examples.json", generated),
  JSON.stringify(examples, null, 2) + "\n",
);
console.log(`${files.length} operator examples executed and verified.`);
