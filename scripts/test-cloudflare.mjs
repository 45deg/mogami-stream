import { Miniflare } from "miniflare";
import { resolve } from "node:path";

const root = resolve(import.meta.dirname, "..");
const worker = new Miniflare({
  modules: true,
  scriptPath: resolve(root, "test/runtime/cloudflare.mjs"),
  modulesRoot: root,
  modulesRules: [
    { type: "ESModule", include: ["**/*.mjs", "**/*.js"], fallthrough: true },
  ],
  compatibilityDate: "2026-07-30",
});
try {
  const response = await worker.dispatchFetch("https://local.test");
  const result = await response.json();
  if (!response.ok || result.error)
    throw new Error(result.error ?? `HTTP ${response.status}`);
  console.log(
    `Cloudflare Workers (local workerd): ${result.passed.length} contracts passed`,
  );
} finally {
  await worker.dispose();
}
