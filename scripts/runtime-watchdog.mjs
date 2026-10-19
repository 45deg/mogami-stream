import { fork, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const childMarker = "MOGAMI_RUNTIME_CHILD";
const cleanupMs = 5_000;

function killTree(pid, signal) {
  try {
    if (process.platform === "win32") {
      const result = spawnSync("taskkill", ["/pid", String(pid), "/T", "/F"], {
        timeout: cleanupMs,
        stdio: "ignore",
      });
      if (result.error) throw result.error;
    } else {
      process.kill(-pid, signal);
    }
  } catch (error) {
    if (error.code !== "ESRCH")
      console.error(`Cannot stop process ${pid}:`, error);
  }
}

/** Keep the deadline outside both the Node runner and the runtime under test. */
export async function runWithWatchdog(entry, runtime, run) {
  if (process.env[childMarker] === entry) {
    let currentStage = "startup";
    const stage = (name) => {
      currentStage = name;
      process.send?.({ type: "stage", name });
    };
    const trackProcess = (child) => {
      // Playwright launches its browser in a separate process group on POSIX.
      process.send?.({ type: "process", pid: child.pid });
      child.once("exit", () => {
        if (process.connected)
          process.send({ type: "process-exit", pid: child.pid });
      });
    };
    try {
      await run({ stage, trackProcess });
    } catch (error) {
      console.error(`[${runtime}] failed during ${currentStage}:`, error);
      process.exitCode = 1;
    } finally {
      process.disconnect?.();
    }
    return;
  }

  const timeoutMs = Number(process.env.MOGAMI_RUNTIME_TIMEOUT_MS ?? 120_000);
  if (
    !Number.isSafeInteger(timeoutMs) ||
    timeoutMs <= 0 ||
    timeoutMs > 2 ** 31 - 1
  )
    throw new Error(
      "MOGAMI_RUNTIME_TIMEOUT_MS must be an integer in 1..2147483647",
    );

  process.exitCode = await new Promise((resolve) => {
    let currentStage = "startup";
    let stopping = false;
    let forcedStop;
    let finished = false;
    const processes = new Set();
    const child = fork(fileURLToPath(entry), [], {
      detached: process.platform !== "win32",
      env: { ...process.env, [childMarker]: entry },
      stdio: ["ignore", "inherit", "inherit", "ipc"],
    });
    console.log(`[${runtime}] runner process group ${child.pid}`);
    const killResources = (signal) => {
      for (const pid of processes) killTree(pid, signal);
      if (child.pid) killTree(child.pid, signal);
    };
    const stop = (reason) => {
      if (stopping) return;
      stopping = true;
      console.error(
        `[${runtime}] ${reason} during ${currentStage}; stopping resources`,
      );
      killResources("SIGTERM");
      forcedStop = setTimeout(() => {
        console.error(
          `[${runtime}] cleanup exceeded ${cleanupMs}ms; forcing shutdown`,
        );
        killResources("SIGKILL");
      }, cleanupMs);
    };
    const onInterrupt = () => stop("interrupted (SIGINT)");
    const onTerminate = () => stop("interrupted (SIGTERM)");
    process.once("SIGINT", onInterrupt);
    process.once("SIGTERM", onTerminate);
    const deadline = setTimeout(
      () => stop(`deadline exceeded (${timeoutMs}ms)`),
      timeoutMs,
    );
    child.on("message", (message) => {
      if (message.type === "stage") {
        currentStage = message.name;
        console.log(`[${runtime}] ${currentStage}`);
      } else if (message.type === "process") {
        processes.add(message.pid);
        console.log(`[${runtime}] runtime process group ${message.pid}`);
      } else if (message.type === "process-exit") {
        processes.delete(message.pid);
      }
    });
    const finish = (code) => {
      if (finished) return;
      finished = true;
      clearTimeout(deadline);
      clearTimeout(forcedStop);
      process.off("SIGINT", onInterrupt);
      process.off("SIGTERM", onTerminate);
      // Reap descendants even if their owner exits before finishing cleanup.
      killResources("SIGKILL");
      resolve(stopping ? 1 : code);
    };
    child.once("error", (error) => {
      console.error(`[${runtime}] cannot start runner:`, error);
      finish(1);
    });
    child.once("exit", (code, signal) => {
      if (code !== 0 && !stopping)
        console.error(
          `[${runtime}] runner exited during ${currentStage} (${signal ?? code})`,
        );
      finish(code === 0 ? 0 : 1);
    });
  });
}
