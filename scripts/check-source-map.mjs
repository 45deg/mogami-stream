import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

export async function checkSourceMap(path) {
  const map = JSON.parse(await readFile(path, "utf8"));
  assert.ok(map.sources.length > 0, `Source map has no sources: ${path}`);
  assert.equal(
    map.sourcesContent?.length,
    map.sources.length,
    `Source map must embed every source: ${path}`,
  );
  for (const content of map.sourcesContent)
    assert.equal(typeof content, "string", `Missing source content: ${path}`);
}
