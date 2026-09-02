#!/usr/bin/env node

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const skillsRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..");
const skillNames = [
  "tiangong-auto-research",
  "tiangong-auto-research-workbuddy",
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
const evidencePipelineReference = await readFile(
  join(skillsRoot, "tiangong-auto-research", "references", "evidence-pipeline.md"),
  "utf8",
);
const publicationReference = await readFile(
  join(skillsRoot, "tiangong-auto-research", "references", "publication-policy.md"),
  "utf8",
);
const sandboxedIdeReference = await readFile(
  join(skillsRoot, "tiangong-auto-research", "references", "sandboxed-ide.md"),
  "utf8",
);
const scientificDesignReference = await readFile(
  join(skillsRoot, "tiangong-auto-research", "references", "scientific-design.md"),
  "utf8",
);
const nativeExecutionReference = await readFile(
  join(skillsRoot, "tiangong-auto-research", "references", "native-execution.md"),
  "utf8",
);

// Test the installed command recipes and their ordering, not a second runtime
// implementation. Behavioral decisions are evaluated independently before release.
function researchRecipes(reference) {
  return [...reference.matchAll(/```bash\n([\s\S]*?)```/g)].flatMap((match) =>
    match[1].replace(/\\\n\s*/g, " ").split("\n")
      .filter((line) => line.startsWith('node "$AUTO_RESEARCH_CLI"'))
      .map((line) => line.slice(line.indexOf(" -- ") + 4).trim()),
  );
}
const evidenceRecipes = researchRecipes(evidencePipelineReference);
const forecastRecipe = evidenceRecipes.findIndex((line) => line.startsWith("research project evidence content forecast "));
const freezeRecipe = evidenceRecipes.findIndex((line) => line.startsWith("research project evidence content freeze "));
assert.ok(forecastRecipe >= 0 && forecastRecipe < freezeRecipe,
  "The installed acquisition recipe must forecast before the immutable typed-content boundary");
for (const prefix of [
  "research project evidence artifact preflight ",
  "research project evidence decomposition batch ",
  "research project evidence atom batch ",
]) assert.ok(evidenceRecipes.some((line) => line.startsWith(prefix)), `Missing public efficient recipe: ${prefix}`);
const scientificRecipes = researchRecipes(scientificDesignReference);
assert.ok(scientificRecipes.some((line) => line.startsWith("research project scientific review execute ") && line.includes("--confirm-review-cost")),
  "The installed scientific-review recipe must use explicit isolated execution with cost consent");
assert.ok(scientificRecipes.some((line) => line.startsWith("research project fork ") && line.includes("--resume-through discover")),
  "Acquisition recovery must preserve discovery through the supported fork boundary");
assert.match(nativeExecutionReference, /scientific-stopped/u,
  "Native routing must distinguish stopped scientific work from a producer action");

const questionGateHeading = "## Gate the research question before acting";
const questionGateIndex = autoResearchSkill.indexOf(questionGateHeading);
const firstManagedCommandIndex = autoResearchSkill.indexOf(
  "For an existing managed directory",
);
assert.ok(questionGateIndex > 0, "Auto Research must define the research-question gate");
assert.ok(
  questionGateIndex < firstManagedCommandIndex,
  "The research-question gate must run before setup, resolver, or other tool instructions",
);
const normalizedQuestionGate = autoResearchSkill
  .slice(questionGateIndex, firstManagedCommandIndex)
  .toLowerCase()
  .replace(/\s+/g, " ");
for (const marker of [
  "before any cli, browser, search, database, or file operation",
  "do not call tools or begin setup",
  "one testable rewrite",
  "explicit confirmation",
  "directional hypothesis",
  "null results",
  "alternative explanations",
  "counterevidence",
  "fabricate, conceal, or misrepresent evidence",
]) {
  assert.ok(
    normalizedQuestionGate.includes(marker),
    `Research-question gate must preserve the observable behavior: ${marker}`,
  );
}

for (const marker of [
  ".tiangong-research/setup.yaml",
  "research setup init",
  "never scans parent directories",
  "overallReadiness",
]) {
  assert.ok(autoResearchSkill.includes(marker), `Auto Research entry must explain ${marker}`);
}

for (const marker of [
  "runDataCapability",
  "standalone `data run`",
  "dynamic data catalog",
]) {
  assert.ok(
    autoResearchSkill.includes(marker),
    `Auto Research entry must explain native data evidence marker ${marker}`,
  );
}

const managedDataGuidance = `${autoResearchSkill}\n${evidencePipelineReference}`;
assert.doesNotMatch(autoResearchSkill, /later stages are tool-free/u,
  "Acquisition must retain packet-governed download/parsing operations after discovery");
assert.match(
  managedDataGuidance,
  /node "\$AUTO_RESEARCH_CLI"[\s\S]*?--[\s\\\n]+data describe <capability-id> --json/,
  "Managed Auto Research must inspect data capabilities through the locked resolver",
);
assert.doesNotMatch(
  managedDataGuidance,
  /`tiangong-ai data describe|^tiangong-ai data describe/m,
  "Managed Auto Research must not expose a bare data describe command",
);

for (const marker of [
  "native-direct",
  "sandbox-bridge",
  "Default Permission",
  "research reviewer serve",
  "research reviewer status",
  "research reviewer doctor",
  "--host-agent workbuddy",
  "no arbitrary-command action",
  "Never switch",
  "RESEARCH_REVIEW_BRIDGE_UNAVAILABLE",
  "RESEARCH_REVIEW_BRIDGE_VERSION_MISMATCH",
  "RESEARCH_REVIEW_BRIDGE_ATTESTATION_INVALID",
  "RESEARCH_REVIEW_BRIDGE_SANDBOX_POLICY_INVALID",
  "RESEARCH_REVIEW_BRIDGE_MODEL_MISMATCH",
  "RESEARCH_REVIEW_BRIDGE_NONCE_REPLAY",
  "RESEARCH_REVIEW_BRIDGE_RESULT_BINDING_INVALID",
]) {
  assert.ok(
    sandboxedIdeReference.includes(marker),
    `Sandboxed IDE reference must explain ${marker}`,
  );
}

for (const marker of [
  "## Declarative clean-directory setup",
  "schemaVersion: 2",
  ".tiangong-research/setup.env.example",
  "selection.skills",
  "all current catalog Skills",
  "requirement",
  "enabled: false",
  "replaceExistingPlan: true",
  "does not fall back to the Wizard",
  "overallReadiness=READY",
]) {
  assert.ok(setupReference.includes(marker), `Setup reference must explain ${marker}`);
}


for (const marker of [
  "workbuddy",
  "codebuddy",
  "default permission",
  "sandbox-bridge",
  "thin router",
]) {
  assert.ok(
    descriptions["tiangong-auto-research-workbuddy"].includes(marker),
    `WorkBuddy adapter routing description must include ${marker}`,
  );
}

for (const marker of [
  ".tiangong-research/setup.env",
  "chmod 600",
  "literal `NAME=value`",
  "all catalog credentials",
  "disabled credential",
  "must not differ",
  "owner-only logical stores",
]) {
  assert.ok(environmentReference.includes(marker), `Environment reference must explain ${marker}`);
}

for (const marker of [
  "evidence decomposition record",
  "evidence atom register",
  "evidence content freeze",
  "inference-snapshot.json",
  "claim-evidence-graph.json",
  "evidencePipeline",
  "structured data capabilities",
  "research project evidence data run",
  "core receipt digest",
  "data-runtime receipt",
]) {
  assert.ok(
    evidencePipelineReference.includes(marker),
    `Evidence pipeline reference must explain ${marker}`,
  );
}

for (const marker of [
  "--submission",
  "reporting-checklist",
  "source-data",
  "Claim-Evidence Graph",
  "submissionPackageSha256",
]) {
  assert.ok(
    publicationReference.includes(marker),
    `Publication reference must explain ${marker}`,
  );
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
