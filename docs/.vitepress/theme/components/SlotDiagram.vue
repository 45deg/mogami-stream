<script setup lang="ts">
import { computed, ref } from "vue";
import { useData } from "vitepress";

const { lang } = useData();
const step = ref(0);
const copy = {
  en: {
    title: "Ordered map stage · concurrency: 2, capacity: 3",
    caption:
      "Scenario: items A, B, and C are read in order; B and C finish before A while ordered: true holds delivery for A.",
    states: ["Read", "Running", "Buffered", "Free"],
    demand: "Pending downstream next()",
    yes: "Yes",
    no: "No",
    previous: "Previous",
    next: "Next",
    reset: "Reset",
    step: "Step",
    reserved: "reads + running",
    occupied: "occupied slots",
    notes: [
      "No downstream demand: no upstream read starts. All three capacity slots are free.",
      "Downstream calls next(). An upstream read for A starts, reserving one concurrency slot and one capacity slot.",
      "A enters the mapper callback. A second upstream read starts for B (at most one upstream read is in flight at a time).",
      "A and B are both running in the mapper. Because concurrency is 2, no read for C can start yet.",
      "B finishes before A and buffers in its slot. With only A running, an upstream read for C can now start (1 running + 1 read = concurrency 2).",
      "C is now running alongside A, while B's completed value remains buffered waiting for A.",
      "C also finishes before A. All 3 capacity slots are now occupied (A running, B and C buffered). Ordered delivery waits for A.",
      "A finishes and is delivered to resolve the pending next(). B and C stay buffered for subsequent reads; no new upstream read starts until downstream calls next() again.",
    ],
  },
  ja: {
    title: "順序保証つき map ステージ · concurrency: 2, capacity: 3",
    caption:
      "シナリオ: A → B → C の順に読み取り、A より先に B と C が完了した場合（ordered: true のため A の完了まで下流への出力を待機）。",
    states: ["読み取り中", "実行中", "結果保持", "空き"],
    demand: "下流の next() 待機中",
    yes: "あり",
    no: "なし",
    previous: "前へ",
    next: "次へ",
    reset: "最初に戻る",
    step: "ステップ",
    reserved: "読み取り + 実行中",
    occupied: "使用中スロット",
    notes: [
      "下流からの要求（next()）がないため、上流の読み取りはまだ始まりません。3つのスロットはすべて空いています。",
      "下流が next() を呼び出すと、A の上流読み取りが開始され、concurrency と capacity の枠を1つずつ予約します。",
      "A の mapper 実行が始まり、続いて B の上流読み取りを開始します（同時に進行する上流読み取りは常に最大1つです）。",
      "A と B の両方が mapper を実行中です。concurrency の上限（2）に達しているため、C の読み取りはまだ開始できません。",
      "B が A より先に完了し、スロット内で出力待ちになります。実行中が A の1件だけになったため、3つ目のスロットで C の読み取りを開始できます。",
      "A と C が並行して実行中です。完了済みの B は A の後ろで capacity スロットを1つ占有し続けます。",
      "C も完了しました。A が実行中、B と C が結果保持中となり capacity の全3スロットが埋まります。ordered: true のため A の完了を待ちます。",
      "A が完了して下流へ渡され、待機中だった next() が解決します。B と C はバッファに残っており、次に next() が呼ばれるまで新たな上流読み取りは行われません。",
    ],
  },
};
const t = computed(() => copy[lang.value === "ja" ? "ja" : "en"]);
// 0: read, 1: running, 2: buffered, 3: free. A read counts toward both limits.
const frames = [
  [3, 3, 3],
  [0, 3, 3],
  [1, 0, 3],
  [1, 1, 3],
  [1, 2, 0],
  [1, 2, 1],
  [1, 2, 2],
  [3, 2, 2],
];
const slots = computed(() => frames[step.value]);
const running = computed(() => slots.value.filter((state) => state < 2).length);
const occupied = computed(
  () => slots.value.filter((state) => state !== 3).length,
);
</script>

<template>
  <section class="docs-widget" :aria-label="t.title">
    <strong>{{ t.title }}</strong>
    <p class="widget-note">{{ t.caption }}</p>
    <div class="slots">
      <div
        v-for="(state, i) in slots"
        :key="i"
        class="slot"
        :class="`state-${state}`"
      >
        <b>{{ state === 3 ? "—" : ["A", "B", "C"][i] }}</b>
        <span>{{ t.states[state] }}</span>
      </div>
    </div>
    <div aria-live="polite" aria-atomic="true">
      <p>
        <b>{{ t.step }} {{ step + 1 }} / {{ frames.length }}</b> ·
        {{ t.demand }}: <b>{{ step > 0 && step < 7 ? t.yes : t.no }}</b>
      </p>
      <p>{{ t.notes[step] }}</p>
      <p class="widget-note">
        {{ t.reserved }}: {{ running }} ≤ 2 · {{ t.occupied }}: {{ occupied }} ≤
        3
      </p>
    </div>
    <div class="widget-controls">
      <button :disabled="step === 0" @click="step--">{{ t.previous }}</button>
      <button :disabled="step === frames.length - 1" @click="step++">
        {{ t.next }}
      </button>
      <button @click="step = 0">{{ t.reset }}</button>
    </div>
  </section>
</template>

<style scoped>
.slots {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 10px;
}
.slot {
  display: grid;
  gap: 4px;
  padding: 16px 4px;
  text-align: center;
  border: 2px solid var(--vp-c-text-2);
  border-radius: 6px;
  background: var(--vp-c-bg);
}
.slot b {
  font-family: var(--vp-font-family-mono);
  font-size: 24px;
}
.slot span {
  font-size: 13px;
}
.state-0 {
  border-style: dashed;
}
.state-1 {
  border-color: var(--vp-c-brand-1);
  background: var(--vp-c-brand-soft);
}
.state-2 {
  border-style: double;
  border-width: 4px;
  padding: 14px 2px;
}
.state-3 {
  border-style: dotted;
}
</style>
