import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { resolve, sep } from "node:path";
import { runWithWatchdog } from "./runtime-watchdog.mjs";

await runWithWatchdog(
  import.meta.url,
  "Chromium",
  async ({ stage, trackProcess }) => {
    const { chromium } = await import("playwright");
    stage("HTTP server startup");

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
    stage(`HTTP server listening at http://127.0.0.1:${server.address().port}`);
    const chrome =
      "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
    const executablePath =
      process.env.PLAYWRIGHT_EXECUTABLE_PATH ??
      (existsSync(chrome) ? chrome : undefined);
    let browserServer;
    let browser;
    try {
      stage("browser launch");
      browserServer = await chromium.launchServer({
        ...(executablePath ? { executablePath } : {}),
        headless: true,
      });
      trackProcess(browserServer.process());
      browser = await chromium.connect(browserServer.wsEndpoint());
      stage("page navigation");
      const page = await browser.newPage();
      page.setDefaultTimeout(30_000);
      await page.goto(`http://127.0.0.1:${server.address().port}`);
      stage("main contracts");
      const main = await page.evaluate(async () => {
        const { runContracts } = await import("/test/runtime/contracts.mjs");
        return runContracts();
      });
      stage("dedicated worker contracts");
      const dedicated = await page.evaluate(
        () =>
          new Promise((resolve, reject) => {
            const worker = new Worker("/test/runtime/worker.mjs", {
              type: "module",
            });
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
          }),
      );
      stage("service worker registration and contracts");
      const service = await page.evaluate(async () => {
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
          return service;
        } finally {
          await registration.unregister();
        }
      });
      console.log(`Chromium ${browser.version()}`);
      for (const [host, passed] of Object.entries({ main, dedicated, service }))
        console.log(`${host}: ${passed.length} contracts passed`);
    } finally {
      stage("browser and HTTP server cleanup");
      try {
        await browserServer?.close();
      } finally {
        server.closeAllConnections();
        await new Promise((resolve) => server.close(resolve));
      }
    }
  },
);
