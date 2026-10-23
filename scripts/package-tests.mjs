import assert from "node:assert/strict";

// Distribution tests may import dist and test helpers, but no repository tools.
export const distributionTests = [
  "boundaries.test.mjs",
  "cancellation-races.test.mjs",
  "close-lifecycle.test.mjs",
  "concurrency-fuzz.test.mjs",
  "concurrency.test.mjs",
  "consumption-timeline.test.mjs",
  "core-entry.test.mjs",
  "core.test.mjs",
  "execution-signal.test.mjs",
  "fluent.test.mjs",
  "broadcast.test.mjs",
  "fuzz.test.mjs",
  "higher-order-cleanup.test.mjs",
  "higher-order-direct-cancellation.test.mjs",
  "iterator-acquisition.test.mjs",
  "iterator-values.test.mjs",
  "map-lifecycle.test.mjs",
  "memory-retention.test.mjs",
  "merge-contracts.test.mjs",
  "operator-delivery.test.mjs",
  "operator-performance-contracts.test.mjs",
  "performance-contracts.test.mjs",
  "push.test.mjs",
  "range-cancellation.test.mjs",
  "route.test.mjs",
  "read-path.test.mjs",
  "recovery-selection.test.mjs",
  "resilient-map.test.mjs",
  "resource-ownership.test.mjs",
  "scan-state.test.mjs",
  "scope-drain.test.mjs",
  "sequential-delivery.test.mjs",
  "timing.test.mjs",
  "type-modeling.test.mjs",
];
export const repositoryTests = [
  "benchmarks.test.mjs",
  "documentation-recipes.test.mjs",
  "event-loop-recipe.test.mjs",
  "representative-benchmarks.test.mjs",
  "shared-rate-limiter-recipe.test.mjs",
  "terminal-report-recipe.test.mjs",
  "runtime-watchdog.test.mjs",
  "package-test-selection.test.mjs",
];

export function selectPackageTests(discovered) {
  const expected = [...distributionTests, ...repositoryTests];
  assert.equal(
    new Set(expected).size,
    expected.length,
    "Test classifications overlap",
  );
  assert.deepEqual(
    [...discovered].sort(),
    [...expected].sort(),
    "Classify added or removed tests in scripts/package-tests.mjs",
  );
  return [...distributionTests];
}
