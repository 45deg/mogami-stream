import { runContracts } from "./contracts.mjs";
self.onmessage = async () => {
  try {
    self.postMessage({ passed: await runContracts() });
  } catch (error) {
    self.postMessage({ error: String(error?.stack ?? error) });
  }
};
