import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const skillRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const skill = await readFile(join(skillRoot, "SKILL.md"), "utf8");
const production = await readFile(join(skillRoot, "references", "production-research.md"), "utf8");
const nativeExecution = await readFile(join(skillRoot, "references", "native-execution.md"), "utf8");
const exhaustion = await readFile(
  join(skillRoot, "references", "evidence-exhaustion.md"),
  "utf8",
);
const policyBrief = await readFile(
  join(skillRoot, "assets", "research-policy", "defaults", "project", "publication-brief.md"),
  "utf8",
);

assert.match(skill, /references\/evidence-exhaustion\.md/);
assert.match(skill, /all plan-bound lawful agent routes/i);
assert.match(production, /evidence access status/i);
assert.match(nativeExecution, /interactive-challenge/i);
assert.match(nativeExecution, /evidence-exhausted/i);

for (const phrase of [
  /required evidence role/i,
  /terminal event hash/i,
  /purchase\s+or\s+subscription/i,
  /official HTTPS locator/i,
  /alternatives tried/i,
  /resume criteria/i,
  /do not fabricate/i,
  /do not continue substitute searching/i,
]) {
  assert.match(exhaustion, phrase);
}

assert.match(policyBrief, /lawful acquisition routes/i);
assert.match(policyBrief, /purchase, subscription, authorization, or external response/i);
assert.match(policyBrief, /all agent-executable routes/i);

process.stdout.write("evidence exhaustion contract tests passed\n");
