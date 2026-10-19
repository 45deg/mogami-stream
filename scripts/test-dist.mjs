import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import {
  access,
  cp,
  mkdtemp,
  mkdir,
  readFile,
  realpath,
  rm,
  symlink,
  unlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";

const root = resolve(import.meta.dirname, "..");
const temporary = await mkdtemp(resolve(tmpdir(), "mogami-dist-"));
const fixture = resolve(temporary, "fixture");
const run = (script) =>
  execFileSync(process.execPath, [resolve(fixture, "scripts", script)], {
    cwd: fixture,
    stdio: "pipe",
  });
try {
  await mkdir(fixture);
  for (const name of ["src", "scripts", "package.json", "tsconfig.json"])
    await cp(resolve(root, name), resolve(fixture, name), { recursive: true });
  await symlink(
    await realpath(resolve(root, "node_modules")),
    resolve(fixture, "node_modules"),
  );
  const source = resolve(fixture, "src/obsolete-fixture.ts");
  await writeFile(source, "export const obsolete = true;\n");
  const preservedSource = await readFile(
    resolve(fixture, "src/index.ts"),
    "utf8",
  );
  const userFile = resolve(fixture, "user-notes.txt");
  await writeFile(userFile, "Unrelated user changes\n");
  run("build.mjs");
  for (const suffix of [".js", ".d.ts", ".js.map"])
    await access(resolve(fixture, `dist/obsolete-fixture${suffix}`));
  await unlink(source);
  run("build.mjs");
  for (const suffix of [".js", ".d.ts", ".js.map"])
    await assert.rejects(
      access(resolve(fixture, `dist/obsolete-fixture${suffix}`)),
      { code: "ENOENT" },
    );
  run("check-dist.mjs");

  const declaration = resolve(fixture, "dist/index.d.ts");
  const declarationText = await readFile(declaration, "utf8");
  await unlink(declaration);
  assert.throws(() => run("check-dist.mjs"), /Missing distribution files/);
  await writeFile(declaration, declarationText);

  const stale = resolve(fixture, "dist/obsolete.js");
  await writeFile(stale, "export const obsolete = true;\n");
  assert.throws(() => run("check-dist.mjs"), /Unexpected distribution file/);
  await unlink(stale);
  const mapPath = resolve(fixture, "dist/index.js.map");
  const map = JSON.parse(await readFile(mapPath, "utf8"));
  delete map.sourcesContent;
  await writeFile(mapPath, JSON.stringify(map));
  assert.throws(
    () => run("check-dist.mjs"),
    /Source map must embed every source/,
  );

  // Cleaning an output symlink must leave its target untouched.
  const external = resolve(temporary, "external");
  await mkdir(external);
  await writeFile(resolve(external, "keep.txt"), "Keep me\n");
  await rm(resolve(fixture, "dist"), { recursive: true });
  await symlink(external, resolve(fixture, "dist"));
  run("build.mjs");
  run("check-dist.mjs");
  assert.equal(
    await readFile(resolve(external, "keep.txt"), "utf8"),
    "Keep me\n",
  );
  assert.equal(await readFile(userFile, "utf8"), "Unrelated user changes\n");
  assert.equal(
    await readFile(resolve(fixture, "src/index.ts"), "utf8"),
    preservedSource,
  );
  console.log(
    "Clean builds remove obsolete output, preserve sources and unrelated files, and validate distribution integrity.",
  );
} finally {
  await rm(temporary, { recursive: true, force: true });
}
