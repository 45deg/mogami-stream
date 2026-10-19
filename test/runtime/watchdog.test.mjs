import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { cp, mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { promisify } from "node:util";
import { setTimeout as delay } from "node:timers/promises";
import { test } from "node:test";

const root = resolve(import.meta.dirname, "../..");
const timeoutMs = Number(process.env.MOGAMI_WATCHDOG_TEST_TIMEOUT_MS ?? 10_000);

async function fixture(t) {
  const directory = await mkdtemp(resolve(tmpdir(), "mogami-watchdog-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  await mkdir(resolve(directory, "test/runtime"), { recursive: true });
  await cp(resolve(root, "scripts"), resolve(directory, "scripts"), {
    recursive: true,
  });
  await cp(resolve(root, "test/runtime"), resolve(directory, "test/runtime"), {
    recursive: true,
  });
  await symlink(
    resolve(root, "node_modules"),
    resolve(directory, "node_modules"),
    "dir",
  );
  await symlink(resolve(root, "dist"), resolve(directory, "dist"), "dir");
  return directory;
}

async function expectDeadline(script, expectedStage, deadline = timeoutMs) {
  const started = performance.now();
  let failure;
  try {
    await promisify(execFile)(process.execPath, [script], {
      env: { ...process.env, MOGAMI_RUNTIME_TIMEOUT_MS: String(deadline) },
      timeout: deadline + 10_000,
    });
  } catch (error) {
    failure = error;
  }
  assert.ok(failure, "the isolated runner must fail");
  const output = `${failure.stdout}\n${failure.stderr}`;
  assert.equal(failure.code, 1, output);
  assert.equal(
    failure.killed,
    false,
    "the harness must not supply the deadline",
  );
  assert.match(
    output,
    new RegExp(`deadline exceeded \\(${deadline}ms\\) during ${expectedStage}`),
  );
  assert.ok(performance.now() - started < deadline + 9_000, output);
  const runner = Number(output.match(/runner process group (\d+)/)?.[1]);
  assert.ok(runner > 0, output);
  await expectStopped(runner);
  if (process.platform !== "win32") await expectStopped(-runner);
  return output;
}

async function expectStopped(pid) {
  for (let attempt = 0; attempt < 40; attempt++) {
    try {
      process.kill(pid, 0);
    } catch (error) {
      if (error.code === "ESRCH") return;
      throw error;
    }
    await delay(50);
  }
  assert.fail(`runtime process ${pid} survived the watchdog`);
}

for (const host of ["main", "dedicated", "service"]) {
  for (const fault of ["promise", "loop"]) {
    test(`Chromium ${host}: ${fault} cannot escape the watchdog`, async (t) => {
      const directory = await fixture(t);
      await writeFile(
        resolve(directory, "test/runtime/contracts.mjs"),
        `export async function runContracts() {
          const host = typeof document !== "undefined" ? "main"
            : typeof ServiceWorkerGlobalScope !== "undefined" ? "service" : "dedicated";
          if (host === ${JSON.stringify(host)}) {
            ${fault === "promise" ? "await new Promise(() => {});" : "while (true) {}"}
          }
          return [];
        }`,
      );
      const stage = {
        main: "main contracts",
        dedicated: "dedicated worker contracts",
        service: "service worker registration and contracts",
      }[host];
      const output = await expectDeadline(
        resolve(directory, "scripts/test-browser.mjs"),
        stage,
      );
      assert.match(output, /\[Chromium\]/);
      const pid = Number(output.match(/runtime process group (\d+)/)?.[1]);
      assert.ok(pid > 0, output);
      await expectStopped(pid);
      if (process.platform !== "win32") await expectStopped(-pid);
      const origin = output.match(
        /HTTP server listening at (http:\/\/127\.0\.0\.1:\d+)/,
      )?.[1];
      assert.ok(origin, output);
      await assert.rejects(
        fetch(origin, { signal: AbortSignal.timeout(1_000) }),
      );
    });
  }
}

for (const fault of ["promise", "loop"]) {
  test(`Cloudflare workerd: ${fault} cannot escape the watchdog`, async (t) => {
    const directory = await fixture(t);
    await writeFile(
      resolve(directory, "test/runtime/contracts.mjs"),
      `export async function runContracts() {
        ${fault === "promise" ? "await new Promise(() => { setInterval(() => {}, 1000); });" : "while (true) {}"}
      }`,
    );
    const output = await expectDeadline(
      resolve(directory, "scripts/test-cloudflare.mjs"),
      "worker dispatch and contracts",
    );
    assert.match(output, /\[Cloudflare Workers \(local workerd\)\]/);
  });
}
