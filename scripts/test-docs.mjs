import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdir } from "node:fs/promises";
import { once } from "node:events";
import { resolve } from "node:path";
import { chromium } from "playwright";
import { serve } from "vitepress";

const root = resolve(import.meta.dirname, "..");
const server = await serve({ root: resolve(root, "docs"), port: 0 });
if (!server.server.listening) await once(server.server, "listening");
const origin = `http://localhost:${server.server.address().port}`;
const base = `${origin}/mogami-stream/`;
const chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const executablePath =
  process.env.PLAYWRIGHT_EXECUTABLE_PATH ??
  (existsSync(chrome) ? chrome : undefined);
const screenshotDir = process.env.DOCS_SCREENSHOT_DIR;
if (screenshotDir) await mkdir(screenshotDir, { recursive: true });
let browser;
try {
  browser = await chromium.launch({
    ...(executablePath ? { executablePath } : {}),
    headless: true,
  });
  const page = await browser.newPage({
    viewport: { width: 1440, height: 1000 },
  });
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  const links = new Set();
  const routes = [
    "",
    "guide/getting-started.html",
    "guide/building-pipelines.html",
    "guide/consumers.html",
    "guide/concurrency.html",
    "guide/lifecycle.html",
    "guide/timing.html",
    "guide/errors.html",
    "guide/retry.html",
    "operators.html",
    "recipes/api-requests.html",
    "recipes/events.html",
    "recipes/database.html",
    "recipes/testing.html",
    "recipes/event-loop.html",
    "recipes/shared-rate-limiter.html",
    "recipes/terminal-report.html",
    "benchmarks.html",
  ];
  const locales = [
    {
      prefix: "",
      lang: "en",
      term: "retry",
      expand: "Expand all examples",
      collapse: "Collapse all examples",
      next: "Next",
      reset: "Reset",
      step: "Step 8 / 8",
    },
    {
      prefix: "ja/",
      lang: "ja",
      term: "再試行",
      expand: "すべての例を開く",
      collapse: "すべての例を閉じる",
      next: "次へ",
      reset: "最初に戻る",
      step: "ステップ 8 / 8",
    },
  ];

  for (const locale of locales) {
    for (const route of routes) {
      const response = await page.goto(`${base}${locale.prefix}${route}`);
      assert.equal(response.status(), 200, `${locale.prefix}${route}`);
      assert.equal(
        await page.locator("html").getAttribute("lang"),
        locale.lang,
      );
      await page.locator(".vp-doc h1").waitFor();
      if (
        screenshotDir &&
        locale.lang === "ja" &&
        route === "guide/timing.html"
      )
        await page.screenshot({
          path: resolve(screenshotDir, "timing-ja-desktop.png"),
        });
      if (route === "" || route === "guide/getting-started.html") {
        assert.deepEqual(
          await page.locator(".vp-code-group label").allTextContents(),
          ["npm", "pnpm", "Bun"],
        );
      }
      if (route === "recipes/testing.html") {
        assert.equal(
          await page
            .locator(
              '.vp-doc a[href*="test-clock"], .vp-doc a[href*="iterateWithScheduler"]',
            )
            .count(),
          0,
        );
        assert.doesNotMatch(
          await page.locator(".vp-doc").innerText(),
          /createTestClock|iterateWithScheduler/,
        );
      }
      if (route === "recipes/event-loop.html") {
        const examples = await page
          .locator(".vp-doc .language-js pre")
          .allTextContents();
        assert.equal(examples.length, 4);
        assert.match(examples[0], /yieldEvery\(1_024\)/);
        assert.match(examples[1], /function yieldToHost/);
        assert.match(examples[2], /function yieldEvery/);
        assert.match(examples[3], /function yieldAfter/);
        assert.ok(examples.every((example) => !example.includes("#region")));
      }
      if (route === "guide/building-pipelines.html") {
        const examples = await page
          .locator(".vp-doc .language-ts pre")
          .allTextContents();
        assert.equal(examples.length, 3);
        assert.match(examples[0], /const titles = from\(tasks\)/);
        assert.match(examples[1], /const firstTwoInputs = from\(tasks\)/);
        assert.match(examples[2], /await titles\.forEach/);
        assert.ok(examples.every((example) => !example.includes("#region")));
        const guideLinks = await page
          .locator(".VPSidebar a[href]")
          .evaluateAll((nodes) => nodes.map((node) => node.href));
        assert.deepEqual(guideLinks.slice(0, 3), [
          `${base}${locale.prefix}guide/getting-started.html`,
          `${base}${locale.prefix}guide/building-pipelines.html`,
          `${base}${locale.prefix}operators.html`,
        ]);
      }
      if (route === "guide/errors.html") {
        const examples = await page
          .locator(".vp-doc .language-ts pre")
          .allTextContents();
        assert.equal(examples.length, 4);
        assert.match(examples[0], /mapSettled/);
        assert.match(examples[1], /retries: 3/);
        assert.match(examples[2], /catchError/);
        assert.match(examples[3], /type CloseReport/);
        assert.ok(examples.every((example) => !example.includes("#region")));
        for (const anchor of [
          "item-errors",
          "retry",
          "catch-error",
          "cleanup-errors",
        ])
          assert.equal(await page.locator(`.vp-doc #${anchor}`).count(), 1);
        assert.equal(
          await page.locator('.VPSidebar a[href$="guide/retry.html"]').count(),
          0,
        );
        assert.equal(
          await page.locator('.VPSidebar a[href$="guide/errors.html"]').count(),
          1,
        );
      }
      if (route === "guide/retry.html") {
        assert.equal(
          await page
            .locator(
              '.vp-doc a[href="./errors.html#retry"], .vp-doc a[href$="/errors.html#retry"]',
            )
            .count(),
          1,
        );
      }
      if (route === "guide/consumers.html") {
        assert.equal(await page.locator(".vp-doc .language-ts").count(), 3);
        const examples = await page.locator(".vp-doc pre").allTextContents();
        assert.match(examples[0], /collectValues/);
        assert.match(examples[1], /processValues/);
        assert.match(examples[2], /checkValues/);
        assert.ok(examples.every((example) => !example.includes("#region")));
      }
      for (const href of await page
        .locator("a[href], .vp-doc img[src]")
        .evaluateAll((nodes) => nodes.map((node) => node.href || node.src))) {
        const url = new URL(href);
        if (url.origin === origin) {
          url.hash = "";
          links.add(url.href);
        }
      }
    }

    await page.goto(`${base}${locale.prefix}operators.html`);
    assert.equal(
      await page.locator(".vp-doc input, .vp-doc select").count(),
      0,
    );
    const snippets = await page
      .locator(".operator-snippet pre code")
      .allTextContents();
    assert.equal(snippets.length, 44);
    assert.ok(
      snippets.every(
        (snippet) =>
          snippet.includes('from "mogami-stream"') &&
          snippet.includes("console.log(result)"),
      ),
    );
    assert.ok(
      snippets.every((snippet) => !snippet.includes("declare function")),
    );
    assert.equal(await page.locator(".operator-output code").count(), 44);
    assert.deepEqual(
      JSON.parse(
        await page.locator("#operator-map .operator-output").textContent(),
      ),
      [2, 4, 6],
    );
    assert.deepEqual(
      JSON.parse(
        await page.locator("#operator-flatten .operator-output").textContent(),
      ),
      ["fast", "slow"],
    );
    assert.deepEqual(
      JSON.parse(
        await page.locator("#operator-concat .operator-output").textContent(),
      ),
      ["slow", "fast"],
    );
    const outcomes = JSON.parse(
      await page.locator("#operator-mapsettled .operator-output").textContent(),
    );
    assert.equal(outcomes[1].status, "rejected");
    assert.equal(await page.locator(".operator-navigation a").count(), 8);
    assert.equal(await page.locator(".operator-list details[open]").count(), 0);
    const names = await page.locator(".operator-name").allTextContents();
    assert.equal(new Set(names).size, 44);
    const mapSummary = page.locator("#operator-map summary");
    await mapSummary.focus();
    await page.keyboard.press("Enter");
    await page.locator("#operator-map details[open]").waitFor();
    assert.match(
      await page.locator("#operator-map .operator-snippet pre").innerText(),
      /concurrency: 2/,
    );
    assert.ok(
      (await page
        .locator('#operator-map .operator-snippet pre span[style*="shiki"]')
        .count()) > 0,
    );
    // Exercise VitePress' copy handler with an isolated clipboard in this test page.
    await page.evaluate(() => {
      Object.defineProperty(navigator, "clipboard", {
        value: {
          writeText: async (text) => {
            window.copiedOperatorExample = text;
          },
        },
        configurable: true,
      });
    });
    await page.locator("#operator-map button.copy").click();
    await page.locator("#operator-map button.copied").waitFor();
    assert.equal(
      await page.evaluate(() => window.copiedOperatorExample),
      await page
        .locator("#operator-map .operator-snippet pre code")
        .textContent(),
    );
    await page
      .getByRole("button", { name: locale.expand, exact: true })
      .click();
    await page.waitForFunction(
      () =>
        document.querySelectorAll(".operator-list details[open]").length === 44,
    );
    await page
      .getByRole("button", { name: locale.collapse, exact: true })
      .click();
    await page.waitForFunction(
      () =>
        document.querySelectorAll(".operator-list details[open]").length === 0,
    );
    await page
      .locator('.operator-navigation a[href="#operator-group-timing"]')
      .click();
    await page.waitForURL(/#operator-group-timing$/);
    await page.waitForFunction(() => {
      const top = document
        .getElementById("operator-group-timing")
        .getBoundingClientRect().top;
      return top >= 0 && top < innerHeight;
    });
    const category = await page.locator("#operator-group-timing").boundingBox();
    assert.ok(category.y >= 0 && category.y < 1000);
    await page.goto(`${base}${locale.prefix}operators.html#operator-map`);
    await page.locator("#operator-map details[open]").waitFor();
    if (screenshotDir && locale.lang === "ja") {
      await page.goto(`${base}${locale.prefix}operators.html`);
      await page.screenshot({
        path: resolve(screenshotDir, "operators-ja-desktop.png"),
      });
      await page.locator("#operator-map summary").click();
      await page.screenshot({
        path: resolve(screenshotDir, "operators-ja-example.png"),
      });
    }

    await page.goto(`${base}${locale.prefix}guide/concurrency.html`);
    for (let i = 0; i < 7; i++)
      await page
        .getByRole("button", { name: locale.next, exact: true })
        .click();
    assert.match(
      await page.locator(".docs-widget [aria-live]").innerText(),
      new RegExp(locale.step),
    );
    assert.equal(
      await page
        .getByRole("button", { name: locale.next, exact: true })
        .isDisabled(),
      true,
    );
    await page.getByRole("button", { name: locale.reset, exact: true }).click();
    assert.match(
      await page.locator(".docs-widget [aria-live]").innerText(),
      /1 \/ 8/,
    );

    await page.locator("#local-search button").click();
    await page.locator("#localsearch-input").fill(locale.term);
    await page.locator(".VPLocalSearchBox .result").first().waitFor();
    assert.ok((await page.locator(".VPLocalSearchBox .result").count()) > 0);
    // Component-only text must also be indexed, with an anchor into the catalog.
    await page.locator("#localsearch-input").fill("filterMap");
    await page
      .locator('.VPLocalSearchBox a[href*="operator-filtermap"]')
      .first()
      .waitFor();
    await page.keyboard.press("Escape");
  }

  for (const href of links) {
    const response = await page.request.get(href);
    assert.equal(response.status(), 200, `Broken link: ${href}`);
  }
  await page.goto(`${base}ja/guide/concurrency.html`);
  await page.getByRole("button", { name: "言語を変更" }).hover();
  await page
    .locator(".VPNavBarTranslations a")
    .filter({ hasText: "English" })
    .click();
  await page.waitForURL(`${base}guide/concurrency.html`);
  await page.locator('html[lang="en"]').waitFor();
  assert.equal(await page.locator("html").getAttribute("lang"), "en");

  // Exercise the responsive layout at 320 CSS px (also the 400% zoom reflow width).
  await page.setViewportSize({ width: 320, height: 800 });
  for (const prefix of ["", "ja/"]) {
    for (const route of [
      "",
      "operators.html",
      "guide/concurrency.html",
      "guide/timing.html",
    ]) {
      await page.goto(`${base}${prefix}${route}`);
      if (screenshotDir && prefix === "ja/" && route === "guide/timing.html")
        await page.screenshot({
          path: resolve(screenshotDir, "timing-ja-mobile.png"),
        });
      if (route === "operators.html") {
        if (screenshotDir && prefix === "ja/")
          await page.screenshot({
            path: resolve(screenshotDir, "operators-ja-mobile.png"),
          });
        await page.locator("#operator-map summary").click();
        await page.locator("#operator-map details[open]").waitFor();
      }
      assert.equal(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
        true,
        `Overflow: ${prefix}${route}`,
      );
    }
  }
  await page.emulateMedia({ reducedMotion: "reduce", colorScheme: "dark" });
  await page.goto(`${base}ja/guide/concurrency.html`);
  const next = page.getByRole("button", { name: "次へ", exact: true });
  await next.focus();
  await page.keyboard.press("Enter");
  assert.match(
    await page.locator(".docs-widget [aria-live]").innerText(),
    /2 \/ 8/,
  );
  assert.deepEqual(errors, []);
  console.log(
    `Chromium ${browser.version()}: ${routes.length * locales.length} pages, ${links.size} internal URLs, localized search, grouped operator overview, example disclosure/copy, slot controls, language switch, and mobile reflow passed.`,
  );
} finally {
  await browser?.close();
  await new Promise((resolve) => server.server.close(resolve));
}
