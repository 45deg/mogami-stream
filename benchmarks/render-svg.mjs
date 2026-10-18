import { writeFile } from "node:fs/promises";
import { cpus } from "node:os";
import { runBenchmarks } from "./lib/harness.mjs";

const args = process.argv.slice(2);
const output = args.find((arg) => arg.startsWith("--svg="))?.slice(6);
if (output === "") throw new Error("--svg requires an output path");
const report = await runBenchmarks(
  "compare",
  args.filter((arg) => !arg.startsWith("--svg=")),
);
const destination =
  output ?? new URL("./results/readme/comparison.svg", import.meta.url);
const labels = {
  native: "Native JS",
  mogami: "Mogami",
  ix: "IxJS",
  "iter-tools": "iter-tools",
  "p-map-iterable": "pMapIterable",
};
const find = (workload, library) =>
  report.results.find((r) => r.case === workload && r.library === library);
const median = (workload, library) => find(workload, library).elapsedMs.median;
const panels = [
  {
    name: "records-stream",
    title: "JSON transformation",
    libraries: ["mogami", "ix", "iter-tools", "native"],
  },
  {
    name: "pipeline-depth",
    title: "13-stage map/filter pipeline",
    libraries: ["mogami", "ix", "iter-tools", "native"],
  },
  {
    name: "flatmap-sync",
    title: "Sequential flatMap (4 values per child)",
    libraries: ["mogami", "ix", "iter-tools", "native"],
  },
].filter((panel) =>
  report.results.some((result) => result.case === panel.name),
);
if (!panels.length)
  throw new Error(
    "SVG supports records-stream, pipeline-depth, and flatmap-sync workloads",
  );
for (const panel of panels) {
  const maximum = Math.max(
    ...panel.libraries.map((library) => median(panel.name, library)),
  );
  const magnitude = 10 ** Math.floor(Math.log10(maximum));
  panel.maximum = Math.ceil(maximum / magnitude) * magnitude;
}
const height =
  8 +
  panels.reduce(
    (sum, panel) => sum + 36 + panel.libraries.length * 32 + 37,
    0,
  ) +
  24;
const escape = (value) =>
  String(value).replaceAll("&", "&amp;").replaceAll("<", "&lt;");
const svg = [
  `<svg xmlns="http://www.w3.org/2000/svg" width="800" height="${height}" viewBox="0 0 800 ${height}" role="img" aria-labelledby="title description">`,
  '<title id="title">Mogami stream benchmark comparison</title>',
  '<desc id="description">Median elapsed time in milliseconds; lower is better. Separate zero-based scale for each workload.</desc>',
  `<rect width="800" height="${height}" fill="#ffffff"/>`,
  '<g font-family="system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif" fill="#172033">',
];
const text = (x, y, value, attributes = "") =>
  svg.push(`<text x="${x}" y="${y}" ${attributes}>${escape(value)}</text>`);
let top = 8;
for (const panel of panels) {
  text(24, top + 21, panel.title, 'font-size="17" font-weight="700"');
  const plotX = 180;
  const plotWidth = 470;
  const startY = top + 36;
  const bottomY = startY + panel.libraries.length * 32;
  for (const fraction of [0, 0.5, 1]) {
    const x = plotX + plotWidth * fraction;
    svg.push(`<path d="M${x} ${startY - 8}V${bottomY - 8}" stroke="#e0e5ec"/>`);
    text(
      x,
      bottomY + 14,
      panel.maximum * fraction,
      'font-size="12" text-anchor="middle" fill="#526075"',
    );
  }
  for (const [index, library] of panel.libraries.entries()) {
    const value = median(panel.name, library);
    const y = startY + index * 32;
    const emphasized = library === "mogami";
    text(
      164,
      y + 16,
      labels[library],
      `font-size="16" text-anchor="end" font-weight="${emphasized ? 750 : 450}"`,
    );
    svg.push(
      `<rect x="${plotX}" y="${y}" width="${((value / panel.maximum) * plotWidth).toFixed(3)}" height="22" fill="${emphasized ? "#1967b2" : "#8695a8"}"/>`,
    );
    text(
      674,
      y + 16,
      value.toFixed(2),
      `font-size="16" font-weight="${emphasized ? 750 : 450}"`,
    );
  }
  top = bottomY + 37;
}
const runtimeName =
  { node: "Node.js", bun: "Bun" }[report.runtime.name] ?? report.runtime.name;
text(
  776,
  height - 10,
  `${runtimeName} ${report.runtime.version} · ${cpus()[0].model}`,
  'font-size="11" text-anchor="end" fill="#788392"',
);
svg.push("</g></svg>");
await writeFile(destination, svg.join("\n") + "\n");
console.log(`Rendered ${output ?? "benchmarks/results/readme/comparison.svg"}`);
