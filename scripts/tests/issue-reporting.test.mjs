import assert from "node:assert/strict";
import { cp, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const root = new URL("../../", import.meta.url);
const bug = ["Summary", "Goal", "Component", "Versions", "Environment", "Stage", "Reproduction", "Expected result", "Actual result", "Evidence"];
const feature = ["Summary", "Component", "Use case", "Current limitation", "Proposed capability", "Success criteria", "Alternatives"];

test("an isolated Auto Research installation carries the same report sections as its issue forms", async () => {
  const temporary = await mkdtemp(join(tmpdir(), "reporting-skill-"));
  try {
    const installed = join(temporary, ".agents", "skills", "tiangong-auto-research");
    await cp(new URL("tiangong-auto-research/", root), installed, { recursive: true });
    const entry = await readFile(join(installed, "SKILL.md"), "utf8");
    const route = entry.match(/\[[^\]]+\]\((references\/issue-reporting\.md)\)/);
    assert.ok(route, "reporting must be discoverable from the installed entrypoint");
    const reference = await readFile(join(installed, route[1]), "utf8");
    const templates = [...reference.matchAll(/```md\n([\s\S]*?)```/g)].map((match) =>
      [...match[1].matchAll(/^### (.+)$/gm)].map((heading) => heading[1]),
    );
    assert.deepEqual(templates, [bug, feature]);
    for (const [name, headings] of [["bug_report", bug], ["feature_request", feature]]) {
      const form = await readFile(new URL(`.github/ISSUE_TEMPLATE/${name}.yml`, root), "utf8");
      const labels = [...form.matchAll(/^      label: (.+)$/gm)].map((match) => match[1]);
      assert.deepEqual(labels, headings, "GitHub and offline agent reports must have identical sections");
    }
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
});
