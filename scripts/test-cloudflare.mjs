import { runWithWatchdog } from "./runtime-watchdog.mjs";
import { resolve } from "node:path";

await runWithWatchdog(
  import.meta.url,
  "Cloudflare Workers (local workerd)",
  async ({ stage }) => {
    const { Miniflare } = await import("miniflare");
    stage("worker startup");
    const root = resolve(import.meta.dirname, "..");
    const worker = new Miniflare({
      modules: true,
      scriptPath: resolve(root, "test/runtime/cloudflare.mjs"),
      modulesRoot: root,
      modulesRules: [
        {
          type: "ESModule",
          include: ["**/*.mjs", "**/*.js"],
          fallthrough: true,
        },
      ],
      compatibilityDate: "2026-07-30",
    });
    try {
      stage("worker dispatch and contracts");
      const response = await worker.dispatchFetch("https://local.test");
      stage("worker response body");
      const result = await response.json();
      if (!response.ok || result.error)
        throw new Error(result.error ?? `HTTP ${response.status}`);
      console.log(
        `Cloudflare Workers (local workerd): ${result.passed.length} contracts passed`,
      );
    } finally {
      stage("worker cleanup");
      await worker.dispose();
    }
  },
);
