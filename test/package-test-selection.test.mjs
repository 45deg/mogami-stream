import assert from "node:assert/strict";
import { test } from "node:test";
import { readdir } from "node:fs/promises";
import {
  distributionTests,
  repositoryTests,
  selectPackageTests,
} from "../scripts/package-tests.mjs";

test("package suite classifies all tests and excludes repository tools", async () => {
  const discovered = (await readdir(new URL("./", import.meta.url))).filter(
    (name) => name.endsWith(".test.mjs"),
  );
  assert.deepEqual(selectPackageTests(discovered), distributionTests);
  for (const name of repositoryTests)
    assert.ok(!distributionTests.includes(name));
  assert.throws(
    () => selectPackageTests([...discovered, "new-contract.test.mjs"]),
    /Classify/,
  );
  assert.throws(() => selectPackageTests(discovered.slice(1)), /Classify/);
});
