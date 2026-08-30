import assert from "node:assert/strict";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { dirname, relative, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
  buildDataSkillBinding,
  validateDataSkillBinding,
  verifyDataSkillBinding,
} from "../data-skill-binding.mjs";

const CLI_VERSION = "0.0.54";
const REPOSITORY_ROOT = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "../..",
);
const PILOT_SKILLS = [
  {
    name: "airnow-hourly-obs-fetch",
    capabilityId: "airnow.hourly-observations",
    operationId: "fetch-hourly",
  },
  {
    name: "federal-register-doc-fetch",
    capabilityId: "federal-register.documents",
    operationId: "search",
  },
];

function listFiles(root, current = root) {
  return readdirSync(current, { withFileTypes: true }).flatMap((entry) => {
    const path = resolve(current, entry.name);
    return entry.isDirectory() ? listFiles(root, path) : [relative(root, path)];
  });
}

function describeFixture() {
  return {
    schemaVersion: "tiangong.data.describe.v1",
    manifest: {
      schemaVersion: "tiangong.data.manifest.v1",
      capabilityId: "example.records",
      capabilityVersion: "1.2.3",
      minimumCliVersion: "0.0.54",
      providerId: "example",
      endpoints: [],
      credentials: [],
      limits: {},
      diagnostics: { static: true, live: false },
      operations: [
        {
          operationId: "search",
          operationVersion: "2.0.0",
          inputSchema: {
            schemaId: "https://schemas.tiangong.ai/data/example/input.v1.json",
            digest: "a".repeat(64),
          },
          outputSchema: {
            schemaId: "https://schemas.tiangong.ai/data/example/output.v1.json",
            digest: "b".repeat(64),
          },
          limits: {},
        },
      ],
      manifestDigest: "c".repeat(64),
    },
    discovery: {
      schemaVersion: "tiangong.data.discovery.v1",
      capabilityId: "example.records",
      capabilityVersion: "1.2.3",
      summary: "Example discovery text.",
      discoveryDigest: "d".repeat(64),
    },
    schemas: {},
  };
}

test("builds a closed execution-only binding", () => {
  const binding = buildDataSkillBinding({
    skillName: "example-record-search",
    cliVersion: CLI_VERSION,
    describe: describeFixture(),
    operationIds: ["search"],
  });

  assert.deepEqual(binding, {
    schemaVersion: "tiangong.data.skill-binding.v1",
    skillName: "example-record-search",
    generatedWithCliVersion: CLI_VERSION,
    capabilityId: "example.records",
    capabilityVersion: "1.2.3",
    minimumCliVersion: "0.0.54",
    manifestDigest: "c".repeat(64),
    operations: [
      {
        operationId: "search",
        operationVersion: "2.0.0",
        inputSchemaId:
          "https://schemas.tiangong.ai/data/example/input.v1.json",
        inputSchemaDigest: "a".repeat(64),
        outputSchemaId:
          "https://schemas.tiangong.ai/data/example/output.v1.json",
        outputSchemaDigest: "b".repeat(64),
      },
    ],
  });
  assert.equal("discoveryDigest" in binding, false);
});

test("accepts discovery-only wording changes", () => {
  const describe = describeFixture();
  const binding = buildDataSkillBinding({
    skillName: "example-record-search",
    cliVersion: CLI_VERSION,
    describe,
    operationIds: ["search"],
  });
  describe.discovery.summary = "Updated wording that does not affect execution.";
  describe.discovery.discoveryDigest = "e".repeat(64);

  assert.doesNotThrow(() =>
    verifyDataSkillBinding({ binding, cliVersion: CLI_VERSION, describe }),
  );
});

test("rejects a missing capability", () => {
  const describe = describeFixture();
  const binding = buildDataSkillBinding({
    skillName: "example-record-search",
    cliVersion: CLI_VERSION,
    describe,
    operationIds: ["search"],
  });
  describe.manifest.capabilityId = "other.records";

  assert.throws(
    () => verifyDataSkillBinding({ binding, cliVersion: CLI_VERSION, describe }),
    /capabilityId/,
  );
});

test("rejects an older CLI than the generated and minimum versions", () => {
  const describe = describeFixture();
  const binding = buildDataSkillBinding({
    skillName: "example-record-search",
    cliVersion: CLI_VERSION,
    describe,
    operationIds: ["search"],
  });

  assert.throws(
    () => verifyDataSkillBinding({ binding, cliVersion: "0.0.53", describe }),
    /CLI version/,
  );
});

test("rejects manifest and operation schema drift", () => {
  const describe = describeFixture();
  const binding = buildDataSkillBinding({
    skillName: "example-record-search",
    cliVersion: CLI_VERSION,
    describe,
    operationIds: ["search"],
  });

  describe.manifest.manifestDigest = "f".repeat(64);
  assert.throws(
    () => verifyDataSkillBinding({ binding, cliVersion: CLI_VERSION, describe }),
    /manifestDigest/,
  );

  describe.manifest.manifestDigest = binding.manifestDigest;
  describe.manifest.operations[0].inputSchema.digest = "0".repeat(64);
  assert.throws(
    () => verifyDataSkillBinding({ binding, cliVersion: CLI_VERSION, describe }),
    /inputSchemaDigest/,
  );
});

test("rejects undeclared binding fields", () => {
  const describe = describeFixture();
  const binding = {
    ...buildDataSkillBinding({
      skillName: "example-record-search",
      cliVersion: CLI_VERSION,
      describe,
      operationIds: ["search"],
    }),
    discoveryDigest: "d".repeat(64),
  };

  assert.throws(
    () => verifyDataSkillBinding({ binding, cliVersion: CLI_VERSION, describe }),
    /Unexpected binding field/,
  );
});

test("rejects internally inconsistent or duplicate binding entries", () => {
  const binding = buildDataSkillBinding({
    skillName: "example-record-search",
    cliVersion: CLI_VERSION,
    describe: describeFixture(),
    operationIds: ["search"],
  });

  assert.throws(
    () =>
      validateDataSkillBinding({
        ...binding,
        generatedWithCliVersion: "0.0.53",
      }),
    /older than minimumCliVersion/,
  );
  assert.throws(
    () =>
      validateDataSkillBinding({
        ...binding,
        operations: [...binding.operations, binding.operations[0]],
      }),
    /Duplicate binding operationId/,
  );
});

test("pilot data skills are thin CLI semantic entrypoints", () => {
  const generatedWithCliVersions = [];
  for (const pilot of PILOT_SKILLS) {
    const root = resolve(REPOSITORY_ROOT, pilot.name);
    const bindingPath = resolve(
      root,
      "references/tiangong-data-binding.json",
    );
    assert.equal(existsSync(resolve(root, "scripts")), false, pilot.name);
    assert.equal(existsSync(resolve(root, "assets")), false, pilot.name);
    assert.deepEqual(
      listFiles(root).sort(),
      [
        "SKILL.md",
        "agents/openai.yaml",
        "references/tiangong-data-binding.json",
      ],
      pilot.name,
    );
    assert.deepEqual(
      readdirSync(resolve(root, "references")).sort(),
      ["tiangong-data-binding.json"],
      pilot.name,
    );

    const binding = JSON.parse(readFileSync(bindingPath, "utf8"));
    validateDataSkillBinding(binding);
    generatedWithCliVersions.push(binding.generatedWithCliVersion);
    assert.equal(binding.skillName, pilot.name);
    assert.equal(binding.capabilityId, pilot.capabilityId);
    assert.deepEqual(
      binding.operations.map((operation) => operation.operationId),
      [pilot.operationId],
    );

    const skill = readFileSync(resolve(root, "SKILL.md"), "utf8");
    assert.match(skill, /references\/tiangong-data-binding\.json/);
    assert.match(skill, new RegExp(`data describe ${pilot.capabilityId}`));
    assert.match(
      skill,
      new RegExp(`data run ${pilot.capabilityId} ${pilot.operationId}`),
    );
    assert.match(skill, /tiangong\.data\.run-request\.v1/);
    assert.match(skill, /"input": \{/);
    assert.doesNotMatch(
      skill,
      /python3|OpenClaw|eco-council|check-config|--dry-run|--output|config\.example\.env/,
    );
    assert.doesNotMatch(skill, /@tiangong-ai\/cli@\d+\.\d+\.\d+/);

    const agent = readFileSync(resolve(root, "agents/openai.yaml"), "utf8");
    assert.match(agent, new RegExp(`\\$${pilot.name}`));
    assert.doesNotMatch(agent, /raw artifact|OpenClaw|eco-council/);
  }
  assert.equal(new Set(generatedWithCliVersions).size, 1);
});
