import { runContracts } from "./contracts.mjs";
self.addEventListener("install", (event) => {
  event.waitUntil(self.skipWaiting());
});
self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});
self.addEventListener("message", (event) => {
  event.waitUntil(
    (async () => {
      try {
        event.ports[0].postMessage({ passed: await runContracts() });
      } catch (error) {
        event.ports[0].postMessage({ error: String(error?.stack ?? error) });
      }
    })(),
  );
});
