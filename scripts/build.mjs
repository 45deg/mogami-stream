import { rm } from "node:fs/promises";
import { execFileSync } from "node:child_process";
import { resolve } from "node:path";

const root = resolve(import.meta.dirname, "..");
// Only compiler output is disposable. Never follow a dist symlink when cleaning.
await rm(resolve(root, "dist"), { recursive: true, force: true });
execFileSync(
  process.execPath,
  [
    resolve(root, "node_modules/typescript/lib/tsc.js"),
    "-p",
    resolve(root, "tsconfig.json"),
  ],
  { cwd: root, stdio: "inherit" },
);
