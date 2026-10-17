import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { resolve, sep } from "node:path";
import { chromium } from "playwright";

const root = resolve(import.meta.dirname, "..");
const server = createServer(async (request, response) => {
  const pathname = new URL(request.url, "http://localhost").pathname;
  if (pathname === "/") {
    response.setHeader("Content-Type", "text/html");
    response.end("<!doctype html><title>Runtime contracts</title>");
    return;
  }
  const path = resolve(root, `.${pathname}`);
  if (!path.startsWith(root + sep)) {
    response.writeHead(403);
    response.end();
    return;
  }
  try {
    response.setHeader("Content-Type", "text/javascript");
    response.setHeader("Service-Worker-Allowed", "/");
    response.end(await readFile(path));
  } catch {
    response.writeHead(404);
    response.end();
  }
});
await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
const chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const executablePath =
  process.env.PLAYWRIGHT_EXECUTABLE_PATH ??
  (existsSync(chrome) ? chrome : undefined);
let browser;
try {
  browser = await chromium.launch({
    ...(executablePath ? { executablePath } : {}),
    headless: true,
  });
  const page = await browser.newPage();
  page.setDefaultTimeout(30_000);
  await page.goto(`http://127.0.0.1:${server.address().port}`);
  const results = await page.evaluate(async () => {
    const { runContracts } = await import("/test/runtime/contracts.mjs");
    const main = await runContracts();
    const dedicated = await new Promise((resolve, reject) => {
      const worker = new Worker("/test/runtime/worker.mjs", { type: "module" });
      worker.onerror = (event) => {
        worker.terminate();
        reject(new Error(event.message));
      };
      worker.onmessage = (event) => {
        worker.terminate();
        event.data.error
          ? reject(new Error(event.data.error))
          : resolve(event.data.passed);
      };
      worker.postMessage("run");
    });
    const registration = await navigator.serviceWorker.register(
      "/test/runtime/service-worker.mjs",
      { type: "module", scope: "/" },
    );
    try {
      await navigator.serviceWorker.ready;
      const service = await new Promise((resolve, reject) => {
        const channel = new MessageChannel();
        channel.port1.onmessage = (event) => {
          channel.port1.close();
          event.data.error
            ? reject(new Error(event.data.error))
            : resolve(event.data.passed);
        };
        registration.active.postMessage("run", [channel.port2]);
      });
      return { main, dedicated, service };
    } finally {
      await registration.unregister();
    }
  });
  console.log(`Chromium ${browser.version()}`);
  for (const [host, passed] of Object.entries(results))
    console.log(`${host}: ${passed.length} contracts passed`);
} finally {
  await browser?.close();
  await new Promise((resolve) => server.close(resolve));
}
