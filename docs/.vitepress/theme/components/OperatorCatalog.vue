<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from "vue";
import { useData, withBase } from "vitepress";
import { operators, operatorGroups, type Locale } from "./operators";
import examples from "../../generated/operator-examples.json";

const { lang } = useData();
const locale = computed<Locale>(() => (lang.value === "ja" ? "ja" : "en"));
const copy = {
  en: {
    navigation: "Operator categories",
    count: "operators",
    expand: "Expand all examples",
    collapse: "Collapse all examples",
    example: "Example",
    output: "Output",
    copy: "Copy example",
  },
  ja: {
    navigation: "オペレータのカテゴリ",
    count: "件",
    expand: "すべての例を開く",
    collapse: "すべての例を閉じる",
    example: "例",
    output: "出力",
    copy: "サンプルをコピー",
  },
};
const t = computed(() => copy[locale.value]);
const groups = computed(() =>
  Object.entries(operatorGroups).map(([id, labels]) => ({
    id,
    label: labels[locale.value],
    operators: operators.filter((op) => op.group === id),
  })),
);
const expanded = ref(new Set<string>());
const allExpanded = computed(() => expanded.value.size === operators.length);

function exampleHtml(name: string) {
  return examples[name as keyof typeof examples].html.replace(
    'title="Copy Code"',
    `title="${t.value.copy}"`,
  );
}
function toggleAll() {
  expanded.value = allExpanded.value
    ? new Set()
    : new Set(operators.map((op) => op.id));
}
function syncExpanded(id: string, event: Event) {
  if ((event.target as HTMLDetailsElement).open) expanded.value.add(id);
  else expanded.value.delete(id);
}
function openLinkedExample() {
  const op = operators.find(
    (op) => `#operator-${op.id}` === window.location.hash,
  );
  if (op) expanded.value.add(op.id);
}
onMounted(() => {
  openLinkedExample();
  window.addEventListener("hashchange", openLinkedExample);
});
onUnmounted(() => window.removeEventListener("hashchange", openLinkedExample));
</script>

<template>
  <nav :aria-label="t.navigation" class="operator-navigation">
    <ul>
      <li v-for="group in groups" :key="group.id">
        <a :href="`#operator-group-${group.id}`"
          >{{ group.label }} <span>({{ group.operators.length }})</span></a
        >
      </li>
    </ul>
  </nav>
  <div class="operator-toolbar">
    <span>{{ operators.length }} {{ t.count }}</span>
    <button
      type="button"
      aria-controls="operator-groups"
      :aria-expanded="allExpanded"
      @click="toggleAll"
    >
      {{ allExpanded ? t.collapse : t.expand }}
    </button>
  </div>
  <div id="operator-groups">
    <section
      v-for="group in groups"
      :key="group.id"
      class="operator-group"
      :aria-labelledby="`operator-group-${group.id}`"
    >
      <h2 :id="`operator-group-${group.id}`">{{ group.label }}</h2>
      <ul class="operator-list">
        <li
          v-for="op in group.operators"
          :id="`operator-${op.id}`"
          :key="op.name"
        >
          <details
            :open="expanded.has(op.id)"
            @toggle="syncExpanded(op.id, $event)"
          >
            <summary>
              <a
                :href="withBase(`/api/${op.page}`)"
                target="_self"
                class="operator-name"
                :aria-label="`${op.name} API`"
                ><code>{{ op.name }}</code></a
              >
              <span class="operator-description">{{
                op.description[locale]
              }}</span>
              <span class="example-hint" aria-hidden="true"
                >{{ t.example }}
                <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
                  <path
                    d="m3 4.5 3 3 3-3"
                    stroke="currentColor"
                    stroke-width="1.5"
                  /></svg
              ></span>
            </summary>
            <div class="operator-example-content">
              <div
                class="operator-snippet"
                :aria-label="`${op.name} ${t.example}`"
                v-html="exampleHtml(op.name)"
              />
              <p class="output-label">{{ t.output }}</p>
              <pre
                class="operator-output"
                tabindex="0"
              ><code>{{ examples[op.name as keyof typeof examples].result }}</code></pre>
            </div>
          </details>
        </li>
      </ul>
    </section>
  </div>
</template>

<style scoped>
.operator-navigation {
  margin: 24px 0 16px;
  padding-bottom: 20px;
  border-bottom: 1px solid var(--vp-c-divider);
}
.operator-navigation ul {
  display: flex;
  flex-wrap: wrap;
  gap: 8px 22px;
  padding: 0;
  margin: 0;
  list-style: none;
}
.operator-navigation li {
  margin: 0;
}
.operator-navigation a {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  min-height: 36px;
}
.operator-navigation span {
  font-size: 13px;
  font-variant-numeric: tabular-nums;
}
.operator-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 12px;
  color: var(--vp-c-text-2);
  font-size: 14px;
}
.operator-toolbar button {
  min-height: 44px;
  padding: 6px 12px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 4px;
  color: var(--vp-c-text-1);
  background: var(--vp-c-bg);
  font: inherit;
}
.operator-toolbar button:hover {
  border-color: var(--vp-c-brand-1);
}
.operator-group h2,
.operator-list li {
  scroll-margin-top: 100px;
}
.operator-group h2 {
  margin: 32px 0 12px;
  padding: 0;
  border-top: none;
  font-size: 20px;
}
.operator-list {
  list-style: none;
  padding: 0;
  margin: 0;
  border-top: 1px solid var(--vp-c-divider);
}
.operator-list li {
  padding: 0;
  margin: 0;
  border-bottom: 1px solid var(--vp-c-divider);
}
summary {
  display: grid;
  grid-template-columns: 130px minmax(0, 1fr) 68px;
  align-items: baseline;
  gap: 16px;
  margin: 0;
  padding: 12px 0;
  list-style: none;
  cursor: pointer;
}
summary::-webkit-details-marker {
  display: none;
}
summary:focus-visible {
  outline: 3px solid var(--vp-c-brand-1);
  outline-offset: 3px;
  border-radius: 2px;
}
summary:hover {
  background: var(--vp-c-bg-soft);
}
.operator-name {
  width: fit-content;
}
.operator-name code {
  padding: 0;
  background: none;
  font-size: 14px;
}
.operator-description {
  font-size: 14px;
  line-height: 1.75;
}
.example-hint {
  justify-self: end;
  display: inline-flex;
  align-items: center;
  gap: 4px;
  color: var(--vp-c-text-2);
  font-size: 12px;
  white-space: nowrap;
}
.example-hint svg {
  flex-shrink: 0;
}
details[open] .example-hint svg {
  transform: rotate(180deg);
}
.operator-example-content {
  min-width: 0;
  padding: 0 0 16px;
}
.operator-example-content :deep(div[class*="language-"]) {
  margin: 0;
  border-radius: 6px;
}
.operator-snippet :deep(pre) {
  max-height: 420px;
  overflow: auto;
}
.operator-example-content :deep(button.copy) {
  opacity: 1;
}
.operator-example-content :deep(.lang) {
  display: none;
}
.output-label {
  margin: 12px 0 6px;
  font-size: 13px;
  color: var(--vp-c-text-2);
}
.operator-output {
  max-width: 100%;
  margin: 0;
  padding: 12px 16px;
  overflow: auto;
  background: var(--vp-c-bg-soft);
  border: 1px solid var(--vp-c-divider);
  border-radius: 6px;
}
.operator-output code {
  padding: 0;
  background: none;
  color: var(--vp-c-text-1);
  font-size: 13px;
}
@media (max-width: 640px) {
  summary {
    grid-template-columns: minmax(0, 1fr) 68px;
    gap: 8px 12px;
  }
  .operator-name {
    grid-column: 1;
    grid-row: 1;
  }
  .operator-description {
    grid-column: 1 / -1;
    grid-row: 2;
  }
  .example-hint {
    grid-column: 2;
    grid-row: 1;
    justify-self: end;
  }
  .operator-navigation ul {
    gap: 4px 16px;
  }
}
</style>
