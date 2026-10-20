import { execFileSync } from "node:child_process";
import { mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseOptions } from "./lib/harness.mjs";

const root = fileURLToPath(new URL("../", import.meta.url));
const paths = {
  svg: resolve(root, "benchmarks/results/readme/comparison.svg"),
  "results-dir": resolve(root, "benchmarks/results/readme"),
};
const benchmarkArgs = [];
let reuse = false;
for (const argument of process.argv.slice(2)) {
  if (argument === "--reuse") {
    reuse = true;
    continue;
  }
  const match = /^--(svg|png|results-dir)=(.+)$/.exec(argument);
  if (match) paths[match[1]] = resolve(match[2]);
  else benchmarkArgs.push(argument);
}
if (reuse && benchmarkArgs.length)
  throw new Error("--reuse cannot change measurement settings");
parseOptions(benchmarkArgs, { gcAvailable: true });
const options = new Map([
  ["gc", "none"],
  ["items", "100000"],
  ["warmup", "5"],
  ["samples", "7"],
  ["timeout-ms", "60000"],
]);
for (const argument of benchmarkArgs) {
  const [, name, value] = /^--([^=]+)=(.+)$/.exec(argument);
  if (name === "json")
    throw new Error("Use --results-dir for the two JSON reports");
  options.set(name, value);
}
const args = [...options].map(([name, value]) => `--${name}=${value}`);
const files = Object.fromEntries(
  ["node", "bun"].map((runtime) => [
    runtime,
    resolve(paths["results-dir"], `${runtime}.json`),
  ]),
);

await mkdir(paths["results-dir"], { recursive: true });
if (!reuse) {
  for (const runtime of ["node", "bun"])
    execFileSync(
      runtime,
      [
        ...(options.get("gc") === "between" ? ["--expose-gc"] : []),
        "benchmarks/run.mjs",
        ...args,
        `--json=${files[runtime]}`,
      ],
      { cwd: root, stdio: "inherit" },
    );
}
for (const name of ["svg", "png"])
  if (paths[name]) await mkdir(dirname(paths[name]), { recursive: true });
execFileSync(
  "uv",
  [
    "run",
    "--script",
    resolve(root, "benchmarks/plot-readme.py"),
    "--node",
    files.node,
    "--bun",
    files.bun,
    "--svg",
    paths.svg,
    ...(paths.png ? ["--png", paths.png] : []),
  ],
  { cwd: root, stdio: "inherit" },
);
console.log(`Rendered ${paths.svg}`);
