import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, extname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const testDir = dirname(fileURLToPath(import.meta.url));
const skillRoot = resolve(testDir, "..", "..");
const manifestPath = join(skillRoot, "references", "format-requirements.json");
const queryScript = join(skillRoot, "scripts", "requirements.mjs");

const requiredCategories = [
  "source-and-scope",
  "profile",
  "structure",
  "binding-and-print",
  "cover-chinese",
  "cover-english",
  "committee",
  "authorization",
  "abstract-chinese",
  "abstract-english",
  "toc",
  "lists",
  "symbols",
  "body-start",
  "headings",
  "body-text",
  "footnotes",
  "references-layout",
  "references-citations",
  "appendix",
  "acknowledgements",
  "statement",
  "resume-achievements",
  "advisor-comments",
  "resolution",
  "other-materials",
  "units",
  "figures",
  "tables",
  "equations",
  "page-layout",
  "headers-pagination",
  "language-and-content",
  "thuthesis-build",
  "pdf-submission",
  "visual-qa",
];

const requiredPageRoles = [
  "chinese-cover",
  "english-cover",
  "authorization",
  "chinese-abstract",
  "table-of-contents",
  "body-opening",
  "figure-table-equation",
  "references",
  "statement",
  "final-material",
];

function walkFiles(root) {
  const output = [];
  for (const entry of readdirSync(root)) {
    const path = join(root, entry);
    if (statSync(path).isDirectory()) {
      output.push(...walkFiles(path));
    } else {
      output.push(path);
    }
  }
  return output;
}

function readManifest() {
  assert.ok(
    existsSync(manifestPath),
    "references/format-requirements.json must exist",
  );
  return JSON.parse(readFileSync(manifestPath, "utf8"));
}

function applies(rule, profile) {
  for (const [dimension, allowed] of Object.entries(rule.appliesTo ?? {})) {
    if (!allowed.includes(profile[dimension])) return false;
  }
  return true;
}

test("the public skill surface is LaTeX-only", () => {
  const publicFiles = walkFiles(skillRoot).filter((path) =>
    [".md", ".yaml", ".json"].includes(extname(path)),
  );
  const forbidden = [
    /\bWord\b/i,
    /\.docx?\b/i,
    /word\.md/i,
    /LaTeX\s*(?:与|和|or|\/|、)\s*Word/i,
    /Word\s*(?:到|to)\s*LaTeX/i,
  ];

  assert.ok(!existsSync(join(skillRoot, "references", "word.md")));
  for (const path of publicFiles) {
    const content = readFileSync(path, "utf8");
    for (const pattern of forbidden) {
      assert.doesNotMatch(content, pattern, `${path} contains ${pattern}`);
    }
  }

  const skill = readFileSync(join(skillRoot, "SKILL.md"), "utf8");
  assert.match(skill, /ThuThesis/);
  assert.match(skill, /LaTeX/);
});

test("all local Markdown links resolve inside the skill", () => {
  const markdownFiles = walkFiles(skillRoot).filter(
    (path) => extname(path) === ".md",
  );
  const linkPattern = /\[[^\]]+\]\(([^)]+)\)/g;
  for (const path of markdownFiles) {
    const content = readFileSync(path, "utf8");
    for (const match of content.matchAll(linkPattern)) {
      const target = match[1].split("#", 1)[0];
      if (!target || /^https?:\/\//.test(target)) continue;
      assert.ok(
        existsSync(resolve(dirname(path), target)),
        `${path} has missing local link ${target}`,
      );
    }
  }
});

test("the format manifest is complete, typed, and source-backed", () => {
  const manifest = readManifest();
  assert.equal(manifest.schemaVersion, 1);
  assert.deepEqual(manifest.dimensions.degree, ["master", "doctor"]);
  assert.deepEqual(manifest.dimensions.degreeType, ["academic", "professional"]);
  assert.deepEqual(manifest.dimensions.language, ["chinese", "english"]);
  assert.deepEqual(manifest.dimensions.output, ["print", "electronic"]);
  assert.deepEqual(manifest.dimensions.secrecy, ["public", "secret"]);

  const sourceIds = new Set(manifest.sources.map((source) => source.id));
  for (const expected of [
    "graduate-guide-2025-03",
    "graduate-guide-2026-05-delta",
    "thuthesis-7.7.1",
    "department-current",
  ]) {
    assert.ok(sourceIds.has(expected), `missing source ${expected}`);
  }

  const categoryIds = new Set(manifest.categories.map((category) => category.id));
  for (const category of requiredCategories) {
    assert.ok(categoryIds.has(category), `missing category ${category}`);
  }

  assert.ok(
    manifest.requirements.length >= 90,
    `expected at least 90 detailed requirements, got ${manifest.requirements.length}`,
  );

  const requirementIds = new Set();
  const categoriesWithRules = new Set();
  for (const rule of manifest.requirements) {
    assert.match(rule.id, /^[a-z0-9]+(?:[.-][a-z0-9]+)*$/);
    assert.ok(!requirementIds.has(rule.id), `duplicate requirement ${rule.id}`);
    requirementIds.add(rule.id);
    assert.ok(categoryIds.has(rule.category), `unknown category ${rule.category}`);
    categoriesWithRules.add(rule.category);
    assert.ok(["required", "conditional", "recommended"].includes(rule.severity));
    assert.ok(rule.requirement.trim().length >= 8, `${rule.id} lacks detail`);
    assert.ok(sourceIds.has(rule.source.id), `${rule.id} has unknown source`);
    assert.ok(rule.source.locator.trim(), `${rule.id} lacks source locator`);
    assert.ok(
      ["automatic", "configure", "author", "manual-review", "department"].includes(
        rule.thuthesis.status,
      ),
      `${rule.id} has invalid ThuThesis status`,
    );
    assert.ok(
      Array.isArray(rule.verification) && rule.verification.length > 0,
      `${rule.id} lacks verification steps`,
    );
    for (const [dimension, allowed] of Object.entries(rule.appliesTo ?? {})) {
      assert.ok(dimension in manifest.dimensions, `${rule.id} has unknown dimension`);
      assert.ok(Array.isArray(allowed) && allowed.length > 0);
      for (const value of allowed) {
        assert.ok(
          manifest.dimensions[dimension].includes(value),
          `${rule.id} has invalid ${dimension}=${value}`,
        );
      }
    }
  }

  for (const category of requiredCategories) {
    assert.ok(categoriesWithRules.has(category), `${category} has no requirements`);
  }
});

test("the visual review plan covers every high-risk page role", () => {
  const manifest = readManifest();
  assert.ok(existsSync(join(skillRoot, "references", "visual-qa.md")));
  const actual = new Set(manifest.visualReview.pageRoles.map((item) => item.id));
  for (const role of requiredPageRoles) {
    assert.ok(actual.has(role), `missing visual page role ${role}`);
  }
  for (const item of manifest.visualReview.pageRoles) {
    assert.ok(item.selection.trim());
    assert.ok(Array.isArray(item.checks) && item.checks.length >= 2);
  }
});

test("visual QA fails closed when the PDF renderer is unhealthy", () => {
  const skill = readFileSync(join(skillRoot, "SKILL.md"), "utf8");
  const visualQa = readFileSync(join(skillRoot, "references", "visual-qa.md"), "utf8");
  assert.ok(existsSync(join(skillRoot, "scripts", "render-pdf.mjs")));
  assert.match(skill, /scripts\/render-pdf\.mjs/);
  assert.match(visualQa, /renderer_environment_error/);
  assert.match(visualQa, /退出码仍为 0/);
  assert.doesNotMatch(visualQa, /^pdftoppm\s/m);
});

test("the requirement query filters a concrete thesis profile", () => {
  assert.ok(existsSync(queryScript), "scripts/requirements.mjs must exist");
  const args = [
    queryScript,
    "--degree",
    "master",
    "--degree-type",
    "academic",
    "--language",
    "chinese",
    "--output",
    "electronic",
    "--secrecy",
    "public",
    "--format",
    "json",
  ];
  const result = spawnSync(process.execPath, args, { encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  const payload = JSON.parse(result.stdout);
  assert.deepEqual(payload.profile, {
    degree: "master",
    degreeType: "academic",
    language: "chinese",
    output: "electronic",
    secrecy: "public",
  });
  assert.ok(payload.requirements.length > 40);
  for (const rule of payload.requirements) {
    assert.ok(applies(rule, payload.profile), `${rule.id} leaked across profiles`);
  }
});

test("the requirement query narrows by category and requirement id", () => {
  const profileArgs = [
    "--degree",
    "doctor",
    "--degree-type",
    "professional",
    "--language",
    "chinese",
    "--output",
    "print",
    "--secrecy",
    "secret",
  ];
  const categoryResult = spawnSync(
    process.execPath,
    [queryScript, ...profileArgs, "--categories", "page-layout,headers-pagination", "--format", "json"],
    { encoding: "utf8" },
  );
  assert.equal(categoryResult.status, 0, categoryResult.stderr);
  const categoryPayload = JSON.parse(categoryResult.stdout);
  assert.ok(categoryPayload.requirements.length > 0);
  assert.ok(
    categoryPayload.requirements.every((rule) =>
      ["page-layout", "headers-pagination"].includes(rule.category),
    ),
  );

  const idResult = spawnSync(
    process.execPath,
    [queryScript, ...profileArgs, "--id", "page.content-margins", "--format", "json"],
    { encoding: "utf8" },
  );
  assert.equal(idResult.status, 0, idResult.stderr);
  const idPayload = JSON.parse(idResult.stdout);
  assert.deepEqual(idPayload.requirements.map((rule) => rule.id), ["page.content-margins"]);
});

test("the requirement query rejects an invalid profile deterministically", () => {
  assert.ok(existsSync(queryScript), "scripts/requirements.mjs must exist");
  const result = spawnSync(
    process.execPath,
    [queryScript, "--degree", "bachelor", "--format", "json"],
    { encoding: "utf8" },
  );
  assert.equal(result.status, 2);
  assert.match(result.stderr, /master.*doctor/i);
});
