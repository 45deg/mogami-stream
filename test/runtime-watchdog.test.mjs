import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { promisify } from "node:util";
import { test } from "node:test";

const watchdog = pathToFileURL(
  resolve(import.meta.dirname, "../scripts/runtime-watchdog.mjs"),
).href;

for (const fault of ["promise", "loop"]) {
  test(
    `watchdog stops a Node ${fault} and its resources even when SIGTERM is ignored`,
    { timeout: 9_500 },
    async (t) => {
      const directory = await mkdtemp(
        resolve(tmpdir(), "mogami-node-watchdog-"),
      );
      t.after(() => rm(directory, { recursive: true, force: true }));
      const script = resolve(directory, "runner.mjs");
      await writeFile(
        script,
        `
      import { runWithWatchdog } from ${JSON.stringify(watchdog)};
      import { createServer } from "node:http";
      import { spawn } from "node:child_process";
      await runWithWatchdog(import.meta.url, "fixture", async ({stage}) => {
        process.on("SIGTERM", () => {});
        const server = createServer((request, response) => response.end("alive"));
        await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
        const child = spawn(process.execPath, ["-e", 'process.on("SIGTERM", () => {}); setInterval(() => {}, 1000)'], {stdio: "ignore"});
        stage("resources " + process.pid + " " + child.pid + " " + server.address().port);
        ${fault === "promise" ? "await new Promise(() => {});" : "while (true) {}"}
      });
    `,
      );
      let failure;
      const started = performance.now();
      try {
        await promisify(execFile)(process.execPath, [script], {
          env: { ...process.env, MOGAMI_RUNTIME_TIMEOUT_MS: "1000" },
          timeout: 9_000,
        });
      } catch (error) {
        failure = error;
      }
      assert.ok(failure);
      const output = `${failure.stdout}\n${failure.stderr}`;
      assert.equal(failure.code, 1, output);
      assert.equal(failure.killed, false);
      assert.match(
        output,
        /\[fixture\] deadline exceeded \(1000ms\) during resources/,
      );
      if (process.platform !== "win32")
        assert.match(output, /forcing shutdown/);
      assert.ok(performance.now() - started < 8_500);
      const [, runner, child, port] =
        output.match(/resources (\d+) (\d+) (\d+)/) ?? [];
      assert.ok(runner && child && port, output);
      for (const pid of [runner, child])
        assert.throws(() => process.kill(Number(pid), 0), { code: "ESRCH" });
      await assert.rejects(
        fetch(`http://127.0.0.1:${port}`, {
          signal: AbortSignal.timeout(1000),
        }),
      );
    },
  );
}
