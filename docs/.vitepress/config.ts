import { defineConfig, type DefaultTheme } from "vitepress";
import { operators, operatorGroups } from "./theme/components/operators";

const labels = {
  en: {
    guide: "Guide",
    start: "Getting started",
    consumers: "Terminal consumers",
    concepts: "Concurrency & demand",
    lifecycle: "Cancellation & cleanup",
    timing: "Timing",
    operators: "Operators",
    retry: "Retries",
    recipes: "Recipes",
    requests: "API requests",
    events: "WebSocket & SSE",
    database: "Database batches",
    testing: "Timeline testing",
    eventLoop: "Event loop yielding",
    benchmarks: "Benchmarks",
    api: "API reference ↗",
  },
  ja: {
    guide: "ガイド",
    start: "はじめに",
    consumers: "終端処理",
    concepts: "並行数・容量・需要",
    lifecycle: "キャンセルと後処理",
    timing: "時間制御",
    operators: "オペレータ",
    retry: "リトライ",
    recipes: "実践レシピ",
    requests: "API 取得",
    events: "WebSocket と SSE",
    database: "DB バッチ書き込み",
    testing: "タイムラインによるテスト",
    eventLoop: "イベントループへ処理を譲る",
    benchmarks: "ベンチマーク",
    api: "API リファレンス ↗",
  },
};

function theme(lang: keyof typeof labels): DefaultTheme.Config {
  const t = labels[lang];
  const prefix = lang === "en" ? "" : `/${lang}`;
  const link = (path: string) => `${prefix}/${path}`;
  return {
    nav: [
      { text: t.guide, link: link("guide/getting-started") },
      { text: t.operators, link: link("operators") },
      { text: t.api, link: "/api/index.html", target: "_self" },
    ],
    sidebar: [
      {
        text: t.guide,
        items: [
          { text: t.start, link: link("guide/getting-started") },
          { text: t.consumers, link: link("guide/consumers") },
          { text: t.concepts, link: link("guide/concurrency") },
          { text: t.lifecycle, link: link("guide/lifecycle") },
          { text: t.timing, link: link("guide/timing") },
          { text: t.retry, link: link("guide/retry") },
          { text: t.operators, link: link("operators") },
        ],
      },
      {
        text: t.recipes,
        items: [
          { text: t.requests, link: link("recipes/api-requests") },
          { text: t.events, link: link("recipes/events") },
          { text: t.database, link: link("recipes/database") },
          { text: t.testing, link: link("recipes/testing") },
          { text: t.eventLoop, link: link("recipes/event-loop") },
        ],
      },
      {
        items: [
          { text: t.benchmarks, link: link("benchmarks") },
          { text: t.api, link: "/api/index.html", target: "_self" },
        ],
      },
    ],
    ...(lang === "ja"
      ? {
          outline: { label: "このページの内容" },
          docFooter: { prev: "前のページ", next: "次のページ" },
          sidebarMenuLabel: "メニュー",
          returnToTopLabel: "ページ上部に戻る",
          darkModeSwitchLabel: "表示テーマ",
          langMenuLabel: "言語を変更",
          skipToContentLabel: "本文へ移動",
        }
      : {}),
  };
}

export default defineConfig({
  title: "mogami-stream",
  description: "Bounded, cancellable AsyncIterable pipelines for TypeScript.",
  base: "/mogami-stream/",
  srcExclude: ["api/**"],
  cleanUrls: false,
  sitemap: { hostname: "https://45deg.github.io/mogami-stream/" },
  locales: {
    root: { label: "English", lang: "en", themeConfig: theme("en") },
    ja: {
      label: "日本語",
      lang: "ja",
      description:
        "並行実行数とバッファを制御できる TypeScript の AsyncIterable ライブラリ。",
      themeConfig: theme("ja"),
    },
  },
  themeConfig: {
    socialLinks: [
      { icon: "github", link: "https://github.com/45deg/mogami-stream" },
    ],
    search: {
      provider: "local",
      options: {
        miniSearch: {
          options: {
            // Self-contained: VitePress serializes this function for the browser.
            tokenize: (text) =>
              Array.from(
                new Intl.Segmenter("ja", { granularity: "word" }).segment(text),
              )
                .filter((part) => part.isWordLike)
                .map((part) => part.segment),
          },
        },
        _render(src, env, md) {
          // Vue components are not expanded by the Markdown search indexer.
          const locale = env.relativePath.startsWith("ja/") ? "ja" : "en";
          const catalog = Object.entries(operatorGroups)
            .map(
              ([id, label]) =>
                `## ${label[locale]} {#operator-group-${id}}\n\n` +
                operators
                  .filter((op) => op.group === id)
                  .map(
                    (op) =>
                      `### ${op.name} {#operator-${op.id}}\n\n${op.description[locale]}\n`,
                  )
                  .join("\n"),
            )
            .join("\n");
          return md.render(src.replace("<OperatorCatalog />", catalog), env);
        },
        locales: {
          ja: {
            translations: {
              button: {
                buttonText: "検索",
                buttonAriaLabel: "ドキュメントを検索",
              },
              modal: {
                noResultsText: "該当する結果がありません",
                resetButtonTitle: "検索をクリア",
                footer: {
                  selectText: "選択",
                  navigateText: "移動",
                  closeText: "閉じる",
                },
              },
            },
          },
        },
      },
    },
  },
});
