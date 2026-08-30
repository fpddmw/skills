import assert from "node:assert/strict";
import test from "node:test";

import {
  buildDataSkillBinding,
  verifyDataSkillBinding,
} from "../data-skill-binding.mjs";

const CLI_VERSION = "0.0.54";

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
