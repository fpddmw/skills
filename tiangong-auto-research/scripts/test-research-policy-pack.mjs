#!/usr/bin/env node

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const skillRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const policyRoot = join(skillRoot, "assets", "research-policy", "defaults");
const expected = {
  baseline: ["top-journal.md"],
  "article-types": [
    "computational-modeling.md",
    "methods-data-resource.md",
    "original-empirical.md",
    "perspective-theory.md",
    "systematic-review-meta-analysis.md",
  ],
  fields: [
    "earth-environment.md",
    "economics-management.md",
    "engineering-computing.md",
    "humanities.md",
    "life-health.md",
    "multidisciplinary.md",
    "physical-chemical-materials.md",
    "social-behavioral.md",
  ],
  "journal-classes": [
    "discipline-flagship.md",
    "methods-data-journal.md",
    "multidisciplinary-selective.md",
    "specialist-top-tier.md",
    "top-review-journal.md",
  ],
  journals: ["exact-journal-template.md"],
  "reviewer-rubrics": [
    "domain-novelty.md",
    "evidence.md",
    "journal-editor.md",
    "methods-reproducibility.md",
  ],
  project: ["publication-brief.md"],
};

const ids = new Set();
for (const [category, files] of Object.entries(expected)) {
  for (const file of files) {
    const path = join(policyRoot, category, file);
    const text = await readFile(path, "utf8");
    assert.match(text, /^---\n[\s\S]+?\n---\n/, `${path} must have frontmatter`);
    const id = text.match(/^id:\s*([a-z0-9.-]+)$/m)?.[1];
    assert.ok(id, `${path} must declare a stable id`);
    assert.equal(ids.has(id), false, `duplicate policy id ${id}`);
    ids.add(id);
    assert.match(text, /^schemaVersion:\s*1$/m, `${path} must use policy schema v1`);
    assert.match(text, /^kind:\s*[a-z-]+$/m, `${path} must declare kind`);
    assert.match(text, /^templateClass:\s*(bundled-default|exact-journal-template|project-template)$/m);
    assert.match(text, /^#\s+\S/m, `${path} must have a title`);
  }
}

for (const relative of [
  ["baseline", "top-journal.md"],
  ["article-types", "original-empirical.md"],
  ["reviewer-rubrics", "journal-editor.md"],
]) {
  const text = await readFile(join(policyRoot, ...relative), "utf8");
  for (const heading of [
    "Scope",
    "Editorial significance",
    "Evidence expectations",
    "Methods and validation",
    "Reproducibility",
    "Desk-reject triggers",
    "Required reviewer questions",
    "Permitted pivots",
  ]) {
    assert.match(text, new RegExp(`^## ${heading}$`, "m"), `${relative.join("/")} lacks ${heading}`);
  }
}

for (const relative of [
  ["baseline", "top-journal.md"],
  ...expected["article-types"].map((file) => ["article-types", file]),
]) {
  const text = await readFile(join(policyRoot, ...relative), "utf8");
  assert.match(text, /^constraints:$/m, `${relative.join("/")} lacks mechanical constraints`);
  assert.match(
    text,
    /^  minDirectPeerReviewedFullText:\s*[1-9][0-9]*$/m,
    `${relative.join("/")} lacks a positive direct full-text floor`,
  );
}

assert.equal(ids.size, Object.values(expected).flat().length);

const skill = await readFile(join(skillRoot, "SKILL.md"), "utf8");
const publicationReference = await readFile(join(skillRoot, "references", "publication-policy.md"), "utf8");
const scientificDesignReference = await readFile(
  join(skillRoot, "references", "scientific-design.md"),
  "utf8",
);
const openAiMetadata = await readFile(join(skillRoot, "agents", "openai.yaml"), "utf8");
assert.match(
  skill,
  /references\/publication-policy\.md/,
  "SKILL.md must route top-journal publication work to the detailed reference",
);
assert.match(
  skill,
  /references\/scientific-design\.md/,
  "SKILL.md must route scientific design and early-gate work to the detailed reference",
);
assert.match(skill, /current native host/i, "final manuscript authoring must remain in the native host");
for (const role of [
  "evidence",
  "methods-reproducibility",
  "domain-novelty",
  "journal-editor",
]) {
  assert.match(publicationReference, new RegExp(`\\b${role}\\b`));
}
assert.match(publicationReference, /does not predict or guarantee editorial acceptance/i);
for (const marker of [
  "real-record construct canary",
  "effective independent units",
  "reviewer prose cannot override",
  "project audit export",
  "futureGateObligations",
  "raw-file-bytes",
  "executable-frozen",
  "jointStateBindings",
]) {
  assert.match(scientificDesignReference, new RegExp(marker, "i"));
}
assert.match(scientificDesignReference, /current native Codex or Claude host/i);
assert.match(scientificDesignReference, /does not create the study design or launch a nested producer/i);
assert.match(openAiMetadata, /scientific design/i);
assert.match(openAiMetadata, /independent scientific and final publication reviews/i);
assert.match(openAiMetadata, /portable audit/i);
process.stdout.write(`Research Policy default pack tests passed (${ids.size} templates)\n`);
