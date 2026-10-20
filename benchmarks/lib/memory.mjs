import { execFileSync } from "node:child_process";

export const memoryFields = [
  "rss",
  "heapTotal",
  "heapUsed",
  "external",
  "arrayBuffers",
];

export function memorySnapshot(raw = process.memoryUsage()) {
  return Object.fromEntries(
    memoryFields.map((key) => [
      key,
      Number.isFinite(raw[key]) && raw[key] >= 0 ? raw[key] : null,
    ]),
  );
}

export function memoryFieldStatus(snapshot, runtime) {
  return Object.fromEntries(
    memoryFields.map((key) => [
      key,
      snapshot[key] === null
        ? "unavailable"
        : runtime === "bun" && ["external", "arrayBuffers"].includes(key)
          ? "unverified-accounting"
          : "reported",
    ]),
  );
}

export function readOsRss() {
  const observation = {
    observedAt: new Date().toISOString(),
    rssBytes: null,
    status: "unavailable",
    reason: null,
  };
  if (!["darwin", "linux"].includes(process.platform))
    return {
      ...observation,
      reason: "ps RSS measurement supports macOS and Linux",
    };
  try {
    const output = execFileSync(
      "ps",
      ["-o", "rss=", "-p", String(process.pid)],
      {
        encoding: "utf8",
        timeout: 2000,
        stdio: ["ignore", "pipe", "pipe"],
      },
    ).trim();
    if (!/^\d+$/.test(output) || !Number.isSafeInteger(Number(output) * 1024))
      throw new Error("Invalid ps RSS output");
    return {
      ...observation,
      rssBytes: Number(output) * 1024,
      status: "measured",
    };
  } catch (error) {
    return { ...observation, reason: error.message };
  }
}
