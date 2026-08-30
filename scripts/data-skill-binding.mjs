#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { readFileSync, renameSync, writeFileSync } from "node:fs";
import { basename, dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const BINDING_SCHEMA_VERSION = "tiangong.data.skill-binding.v1";
const DESCRIBE_SCHEMA_VERSION = "tiangong.data.describe.v1";
const DIGEST_PATTERN = /^[0-9a-f]{64}$/;
const SEMVER_PATTERN = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;
const BINDING_FIELDS = new Set([
  "schemaVersion",
  "skillName",
  "generatedWithCliVersion",
  "capabilityId",
  "capabilityVersion",
  "minimumCliVersion",
  "manifestDigest",
  "operations",
]);
const OPERATION_FIELDS = new Set([
  "operationId",
  "operationVersion",
  "inputSchemaId",
  "inputSchemaDigest",
  "outputSchemaId",
  "outputSchemaDigest",
]);

function fail(message) {
  throw new Error(message);
}

function assertPlainObject(value, label) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    fail(`${label} must be an object.`);
  }
  return value;
}

function assertClosedObject(value, allowedFields, label) {
  const object = assertPlainObject(value, label);
  for (const key of Object.keys(object)) {
    if (!allowedFields.has(key)) {
      fail(`Unexpected ${label} field: ${key}`);
    }
  }
  return object;
}

function requireString(value, label) {
  if (typeof value !== "string" || value.length === 0) {
    fail(`${label} must be a non-empty string.`);
  }
  return value;
}

function requireDigest(value, label) {
  const digest = requireString(value, label);
  if (!DIGEST_PATTERN.test(digest)) {
    fail(`${label} must be a lowercase SHA-256 digest.`);
  }
  return digest;
}

function parseSemver(value, label) {
  const version = requireString(value, label);
  const match = SEMVER_PATTERN.exec(version);
  if (!match) {
    fail(`${label} must be an exact x.y.z version.`);
  }
  return match.slice(1).map(Number);
}

function compareSemver(left, right) {
  const leftParts = parseSemver(left, "left version");
  const rightParts = parseSemver(right, "right version");
  for (let index = 0; index < leftParts.length; index += 1) {
    if (leftParts[index] !== rightParts[index]) {
      return leftParts[index] < rightParts[index] ? -1 : 1;
    }
  }
  return 0;
}

function manifestFromDescribe(describe) {
  const envelope = assertPlainObject(describe, "describe envelope");
  if (envelope.schemaVersion !== DESCRIBE_SCHEMA_VERSION) {
    fail(
      `describe schemaVersion must be ${DESCRIBE_SCHEMA_VERSION}, got ${String(envelope.schemaVersion)}.`,
    );
  }
  const manifest = assertPlainObject(envelope.manifest, "describe manifest");
  if (!Array.isArray(manifest.operations)) {
    fail("describe manifest.operations must be an array.");
  }
  return manifest;
}

function operationBinding(operation) {
  const item = assertPlainObject(operation, "manifest operation");
  const inputSchema = assertPlainObject(
    item.inputSchema,
    "manifest operation inputSchema",
  );
  const outputSchema = assertPlainObject(
    item.outputSchema,
    "manifest operation outputSchema",
  );
  return {
    operationId: requireString(item.operationId, "operationId"),
    operationVersion: requireString(
      item.operationVersion,
      "operationVersion",
    ),
    inputSchemaId: requireString(inputSchema.schemaId, "inputSchemaId"),
    inputSchemaDigest: requireDigest(
      inputSchema.digest,
      "inputSchemaDigest",
    ),
    outputSchemaId: requireString(outputSchema.schemaId, "outputSchemaId"),
    outputSchemaDigest: requireDigest(
      outputSchema.digest,
      "outputSchemaDigest",
    ),
  };
}

function validateBindingShape(binding) {
  const value = assertClosedObject(binding, BINDING_FIELDS, "binding");
  if (value.schemaVersion !== BINDING_SCHEMA_VERSION) {
    fail(
      `binding schemaVersion must be ${BINDING_SCHEMA_VERSION}, got ${String(value.schemaVersion)}.`,
    );
  }
  requireString(value.skillName, "skillName");
  parseSemver(value.generatedWithCliVersion, "generatedWithCliVersion");
  requireString(value.capabilityId, "capabilityId");
  requireString(value.capabilityVersion, "capabilityVersion");
  parseSemver(value.minimumCliVersion, "minimumCliVersion");
  requireDigest(value.manifestDigest, "manifestDigest");
  if (!Array.isArray(value.operations) || value.operations.length === 0) {
    fail("binding operations must be a non-empty array.");
  }
  for (const operation of value.operations) {
    const item = assertClosedObject(
      operation,
      OPERATION_FIELDS,
      "binding operation",
    );
    operationBinding({
      operationId: item.operationId,
      operationVersion: item.operationVersion,
      inputSchema: {
        schemaId: item.inputSchemaId,
        digest: item.inputSchemaDigest,
      },
      outputSchema: {
        schemaId: item.outputSchemaId,
        digest: item.outputSchemaDigest,
      },
    });
  }
  return value;
}

function selectedOperations(manifest, operationIds) {
  const requested = [...new Set(operationIds)].sort();
  if (requested.length === 0) {
    fail("At least one operation ID is required.");
  }
  return requested.map((operationId) => {
    const operation = manifest.operations.find(
      (candidate) => candidate.operationId === operationId,
    );
    if (!operation) {
      fail(`Capability does not publish operationId ${operationId}.`);
    }
    return operationBinding(operation);
  });
}

export function buildDataSkillBinding({
  skillName,
  cliVersion,
  describe,
  operationIds,
}) {
  requireString(skillName, "skillName");
  parseSemver(cliVersion, "CLI version");
  const manifest = manifestFromDescribe(describe);
  const minimumCliVersion = requireString(
    manifest.minimumCliVersion,
    "minimumCliVersion",
  );
  parseSemver(minimumCliVersion, "minimumCliVersion");
  if (compareSemver(cliVersion, minimumCliVersion) < 0) {
    fail(
      `CLI version ${cliVersion} is older than minimumCliVersion ${minimumCliVersion}.`,
    );
  }

  return {
    schemaVersion: BINDING_SCHEMA_VERSION,
    skillName,
    generatedWithCliVersion: cliVersion,
    capabilityId: requireString(manifest.capabilityId, "capabilityId"),
    capabilityVersion: requireString(
      manifest.capabilityVersion,
      "capabilityVersion",
    ),
    minimumCliVersion,
    manifestDigest: requireDigest(
      manifest.manifestDigest,
      "manifestDigest",
    ),
    operations: selectedOperations(manifest, operationIds),
  };
}

function compareField(actual, expected, label) {
  if (actual !== expected) {
    fail(`${label} drift: expected ${expected}, got ${String(actual)}.`);
  }
}

export function verifyDataSkillBinding({ binding, cliVersion, describe }) {
  const expected = validateBindingShape(binding);
  parseSemver(cliVersion, "CLI version");
  if (cliVersion !== expected.generatedWithCliVersion) {
    fail(
      `CLI version ${cliVersion} does not match generatedWithCliVersion ${expected.generatedWithCliVersion}.`,
    );
  }
  if (compareSemver(cliVersion, expected.minimumCliVersion) < 0) {
    fail(
      `CLI version ${cliVersion} is older than minimumCliVersion ${expected.minimumCliVersion}.`,
    );
  }

  const manifest = manifestFromDescribe(describe);
  compareField(manifest.capabilityId, expected.capabilityId, "capabilityId");
  compareField(
    manifest.capabilityVersion,
    expected.capabilityVersion,
    "capabilityVersion",
  );
  compareField(
    manifest.minimumCliVersion,
    expected.minimumCliVersion,
    "minimumCliVersion",
  );
  compareField(
    manifest.manifestDigest,
    expected.manifestDigest,
    "manifestDigest",
  );

  for (const expectedOperation of expected.operations) {
    const operation = manifest.operations.find(
      (candidate) =>
        candidate.operationId === expectedOperation.operationId,
    );
    if (!operation) {
      fail(`operationId drift: missing ${expectedOperation.operationId}.`);
    }
    const actualOperation = operationBinding(operation);
    for (const field of OPERATION_FIELDS) {
      compareField(
        actualOperation[field],
        expectedOperation[field],
        `${expectedOperation.operationId}.${field}`,
      );
    }
  }
}

function parseArguments(argv) {
  const [command, ...tokens] = argv;
  if (!command || !["generate", "verify"].includes(command)) {
    fail("Expected command generate or verify.");
  }
  const options = {};
  for (let index = 0; index < tokens.length; index += 2) {
    const flag = tokens[index];
    const value = tokens[index + 1];
    if (!flag?.startsWith("--") || value === undefined) {
      fail(`Invalid argument near ${String(flag)}.`);
    }
    options[flag.slice(2)] = value;
  }
  return { command, options };
}

function requireOption(options, name) {
  return requireString(options[name], `--${name}`);
}

function runExactCli(cliVersion, packageSpecifier, args) {
  const result = spawnSync(
    "npx",
    [
      "--yes",
      "--package",
      packageSpecifier,
      "--",
      "tiangong-ai",
      ...args,
    ],
    {
      encoding: "utf8",
      env: { ...process.env, NO_COLOR: "1" },
      maxBuffer: 32 * 1024 * 1024,
    },
  );
  if (result.error) {
    fail(`Unable to run exact CLI ${cliVersion}: ${result.error.message}`);
  }
  if (result.status !== 0) {
    const detail = (result.stderr || result.stdout || "unknown error").trim();
    fail(`Exact CLI ${cliVersion} failed: ${detail}`);
  }
  return result.stdout.trim();
}

function loadExactDescribe(options, capabilityId) {
  const cliVersion = requireOption(options, "cli-version");
  parseSemver(cliVersion, "--cli-version");
  const packageSpecifier =
    options.package ?? `@tiangong-ai/cli@${cliVersion}`;
  const actualVersion = runExactCli(cliVersion, packageSpecifier, ["--version"]);
  if (actualVersion !== cliVersion) {
    fail(
      `Exact CLI package reported ${actualVersion}; expected ${cliVersion}.`,
    );
  }
  const output = runExactCli(cliVersion, packageSpecifier, [
    "data",
    "describe",
    capabilityId,
    "--json",
  ]);
  try {
    return { cliVersion, describe: JSON.parse(output) };
  } catch (error) {
    fail(`Exact CLI describe output is not JSON: ${error.message}`);
  }
}

function readSkillName(skillPath) {
  const skillFile = resolve(skillPath, "SKILL.md");
  const text = readFileSync(skillFile, "utf8");
  const frontmatter = /^---\n([\s\S]*?)\n---\n/.exec(text)?.[1];
  const name = frontmatter && /^name:\s*([^\s]+)\s*$/m.exec(frontmatter)?.[1];
  return requireString(name, `${skillFile} frontmatter name`);
}

function writeJsonAtomically(path, value) {
  const outputPath = resolve(path);
  const temporaryPath = resolve(
    dirname(outputPath),
    `.${basename(outputPath)}.tmp-${process.pid}`,
  );
  writeFileSync(temporaryPath, `${JSON.stringify(value, null, 2)}\n`, {
    encoding: "utf8",
    mode: 0o600,
  });
  renameSync(temporaryPath, outputPath);
}

function generateCommand(options) {
  const skillPath = resolve(requireOption(options, "skill"));
  const capabilityId = requireOption(options, "capability");
  const operationIds = requireOption(options, "operations")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  const { cliVersion, describe } = loadExactDescribe(options, capabilityId);
  const binding = buildDataSkillBinding({
    skillName: readSkillName(skillPath),
    cliVersion,
    describe,
    operationIds,
  });
  const outputPath =
    options.output ??
    resolve(skillPath, "references", "tiangong-data-binding.json");
  writeJsonAtomically(outputPath, binding);
  process.stdout.write(`${resolve(outputPath)}\n`);
}

function verifyCommand(options) {
  const bindingPath = resolve(requireOption(options, "binding"));
  const binding = JSON.parse(readFileSync(bindingPath, "utf8"));
  const { cliVersion, describe } = loadExactDescribe(
    options,
    requireString(binding.capabilityId, "binding capabilityId"),
  );
  verifyDataSkillBinding({ binding, cliVersion, describe });
  process.stdout.write(`${bindingPath}: valid\n`);
}

function main() {
  const { command, options } = parseArguments(process.argv.slice(2));
  if (command === "generate") {
    generateCommand(options);
  } else {
    verifyCommand(options);
  }
}

const entryPath = process.argv[1] ? resolve(process.argv[1]) : "";
if (entryPath === fileURLToPath(import.meta.url)) {
  try {
    main();
  } catch (error) {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  }
}
