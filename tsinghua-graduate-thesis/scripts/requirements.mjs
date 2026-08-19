#!/usr/bin/env node

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const manifest = JSON.parse(
  readFileSync(join(scriptDir, "..", "references", "format-requirements.json"), "utf8"),
);

const flagToDimension = {
  "degree": "degree",
  "degree-type": "degreeType",
  "language": "language",
  "output": "output",
  "secrecy": "secrecy",
};

function usage() {
  return [
    "Usage: requirements.mjs --degree master|doctor --degree-type academic|professional",
    "  --language chinese|english --output print|electronic --secrecy public|secret",
    "  [--categories id,id] [--id requirement-id] [--format markdown|json]",
  ].join("\n");
}

function fail(message) {
  process.stderr.write(`${message}\n${usage()}\n`);
  process.exit(2);
}

function parseArguments(argv) {
  const options = { format: "markdown" };
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--help") {
      process.stdout.write(`${usage()}\n`);
      process.exit(0);
    }
    if (!token.startsWith("--")) fail(`Unexpected argument: ${token}`);
    const key = token.slice(2);
    const value = argv[index + 1];
    if (!value || value.startsWith("--")) fail(`Missing value for --${key}`);
    options[key] = value;
    index += 1;
  }
  return options;
}

function buildProfile(options) {
  const profile = {};
  for (const [flag, dimension] of Object.entries(flagToDimension)) {
    const value = options[flag];
    if (!value) fail(`Missing required profile flag --${flag}`);
    const allowed = manifest.dimensions[dimension];
    if (!allowed.includes(value)) {
      fail(`Invalid --${flag} value '${value}'; expected ${allowed.join(" or ")}`);
    }
    profile[dimension] = value;
  }
  return profile;
}

function applies(rule, profile) {
  return Object.entries(rule.appliesTo ?? {}).every(([dimension, allowed]) =>
    allowed.includes(profile[dimension]),
  );
}

function selectRequirements(profile, options) {
  let selected = manifest.requirements.filter((rule) => applies(rule, profile));
  if (options.categories) {
    const categories = new Set(options.categories.split(",").filter(Boolean));
    const known = new Set(manifest.categories.map((item) => item.id));
    for (const category of categories) {
      if (!known.has(category)) fail(`Unknown category '${category}'`);
    }
    selected = selected.filter((rule) => categories.has(rule.category));
  }
  if (options.id) {
    selected = selected.filter((rule) => rule.id === options.id);
    if (selected.length === 0) fail(`Requirement '${options.id}' does not apply or exist`);
  }
  return selected;
}

function renderMarkdown(profile, requirements) {
  const lines = [
    "# 清华研究生学位论文格式清单",
    "",
    `- degree: ${profile.degree}`,
    `- degree-type: ${profile.degreeType}`,
    `- language: ${profile.language}`,
    `- output: ${profile.output}`,
    `- secrecy: ${profile.secrecy}`,
    `- baseline: ThuThesis ${manifest.baseline.thuthesisVersion}; guide ${manifest.baseline.graduateGuideVersion} + ${manifest.baseline.deltaVersion} delta`,
    "",
  ];
  const categoryTitles = new Map(manifest.categories.map((item) => [item.id, item.title]));
  let activeCategory = null;
  for (const rule of requirements) {
    if (rule.category !== activeCategory) {
      activeCategory = rule.category;
      lines.push(`## ${categoryTitles.get(activeCategory)}`, "");
    }
    lines.push(
      `- [ ] **${rule.id}** (${rule.severity}) ${rule.requirement}`,
      `  - 来源: ${rule.source.id} ${rule.source.locator}`,
      `  - ThuThesis: ${rule.thuthesis.status}`,
      `  - 验证: ${rule.verification.join("；")}`,
    );
  }
  return `${lines.join("\n")}\n`;
}

const options = parseArguments(process.argv.slice(2));
if (!(["markdown", "json"].includes(options.format))) {
  fail(`Invalid --format value '${options.format}'; expected markdown or json`);
}
const profile = buildProfile(options);
const requirements = selectRequirements(profile, options);
const payload = {
  schemaVersion: manifest.schemaVersion,
  baseline: manifest.baseline,
  profile,
  requirements,
};

if (options.format === "json") {
  process.stdout.write(`${JSON.stringify(payload, null, 2)}\n`);
} else {
  process.stdout.write(renderMarkdown(profile, requirements));
}
