#!/usr/bin/env node

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const skillsRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..");
const skillNames = [
  "tiangong-auto-research",
  "tiangong-kb-sci-search",
  "tiangong-kb-report-search",
  "tiangong-kb-patent-search",
];

async function description(skillName) {
  const text = await readFile(join(skillsRoot, skillName, "SKILL.md"), "utf8");
  const match = text.match(/^description:\s*(.+)$/m);
  assert.ok(match, `${skillName} must have a one-line routing description`);
  return match[1].toLowerCase();
}

const descriptions = Object.fromEntries(
  await Promise.all(skillNames.map(async (name) => [name, await description(name)])),
);

const autoResearchSkill = await readFile(
  join(skillsRoot, "tiangong-auto-research", "SKILL.md"),
  "utf8",
);
const setupReference = await readFile(
  join(skillsRoot, "tiangong-auto-research", "references", "setup.md"),
  "utf8",
);
const environmentReference = await readFile(
  join(skillsRoot, "tiangong-auto-research", "references", "env.md"),
  "utf8",
);

for (const marker of [
  ".tiangong-research/setup.yaml",
  "research setup init",
  "never scans parent directories",
  "overallReadiness",
]) {
  assert.ok(autoResearchSkill.includes(marker), `Auto Research entry must explain ${marker}`);
}

for (const marker of [
  "## Declarative clean-directory setup",
  ".tiangong-research/setup.env.example",
  "replaceExistingPlan: true",
  "does not fall back to the Wizard",
  "overallReadiness=READY",
]) {
  assert.ok(setupReference.includes(marker), `Setup reference must explain ${marker}`);
}

for (const marker of [
  ".tiangong-research/setup.env",
  "chmod 600",
  "literal `NAME=value`",
  "must not differ",
  "owner-only logical stores",
]) {
  assert.ok(environmentReference.includes(marker), `Environment reference must explain ${marker}`);
}

for (const marker of [
  "open-ended",
  "multi-source",
  "current native",
  "independent review",
  ".tiangong-research",
  "takes precedence",
  "研究一下",
  "朝这个方向做一做",
  "结合已有成果继续研究",
  "查资料并形成结论",
  "系统梳理证据",
]) {
  assert.ok(
    descriptions["tiangong-auto-research"].includes(marker),
    `Auto Research routing description must include ${marker}`,
  );
}

for (const source of ["sci", "report", "patent"]) {
  const skill = `tiangong-kb-${source}-search`;
  for (const marker of [
    "one isolated",
    ".tiangong-research",
    "route to `tiangong-auto-research`",
    "execution_mode=standalone",
  ]) {
    assert.ok(descriptions[skill].includes(marker), `${skill} must include ${marker}`);
  }
}

function expectedRoute(prompt, managedWorkspace) {
  const normalized = prompt.toLowerCase();
  const source = normalized.includes("sci")
    ? "sci"
    : normalized.includes("报告") || normalized.includes("report")
      ? "report"
      : normalized.includes("专利") || normalized.includes("patent")
        ? "patent"
        : null;
  const explicitlyIsolated =
    source !== null &&
    /(只|仅|one isolated|only).*(sci|报告|report|专利|patent)|(sci|报告|report|专利|patent).*(只|仅|only)/i.test(
      normalized,
    );
  const systematic =
    /(研究一下|朝这个方向做一做|结合.*已有成果|查资料.*结论|系统梳理|open-ended|multi-source|investigate|form a conclusion|reviewed research artifact)/i.test(
      normalized,
    );
  if (explicitlyIsolated) {
    return `tiangong-kb-${source}-search`;
  }
  if (managedWorkspace || systematic) {
    return "tiangong-auto-research";
  }
  return source ? `tiangong-kb-${source}-search` : "unrelated";
}

const fixtures = [
  ["研究一下新能源汽车变重是否增加道路损伤，并形成结论", true, "tiangong-auto-research"],
  ["朝这个方向做一做", false, "tiangong-auto-research"],
  ["结合这个目录中的已有成果继续研究", true, "tiangong-auto-research"],
  ["查资料并形成结论/报告", false, "tiangong-auto-research"],
  ["系统梳理证据和应对措施", false, "tiangong-auto-research"],
  ["Investigate this as a multi-source reviewed research artifact", false, "tiangong-auto-research"],
  ["只在 SCI 库查询标题 X，返回前 5 条", true, "tiangong-kb-sci-search"],
  ["Only search the report database for title X", true, "tiangong-kb-report-search"],
  ["仅查询专利库中的申请号 X", true, "tiangong-kb-patent-search"],
];
for (const [prompt, managed, expected] of fixtures) {
  assert.equal(expectedRoute(prompt, managed), expected, prompt);
}

process.stdout.write("Auto Research routing contract tests passed\n");
