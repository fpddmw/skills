#!/usr/bin/env node

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { chmod, mkdir, mkdtemp, readFile, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const script = join(dirname(fileURLToPath(import.meta.url)), "research_cli.mjs");
const root = await mkdtemp(join(tmpdir(), "tiangong-auto-research-resolver-"));
const fakeBin = join(root, "bin");
const auditPath = join(root, "npx-args.json");
const cliPackage = "@tiangong-ai/cli";
const lockedVersion = "9.8.7";
await mkdir(fakeBin);
const fakeNpx = join(fakeBin, "npx");
await writeFile(
  fakeNpx,
  `#!/bin/sh\nprintf '%s\\n' \"$@\" | node -e 'const fs=require("node:fs"); const lines=fs.readFileSync(0,"utf8").trimEnd().split("\\n"); fs.writeFileSync(process.env.FAKE_NPX_AUDIT, JSON.stringify(lines));'\n`,
  "utf8",
);
await chmod(fakeNpx, 0o700);

async function createWorkspace(name, lock) {
  const workspace = join(root, name);
  const control = join(workspace, ".tiangong-research");
  await mkdir(control, { recursive: true });
  if (lock !== undefined) {
    await writeFile(join(control, "runtime-lock.json"), JSON.stringify(lock), "utf8");
  }
  return workspace;
}

function invoke(workspace, extraEnv = {}) {
  return spawnSync(
    process.execPath,
    [script, "--workspace", workspace, "--", "research", "setup", "status", "--json"],
    {
      encoding: "utf8",
      env: {
        ...process.env,
        ...extraEnv,
        FAKE_NPX_AUDIT: auditPath,
        PATH: `${fakeBin}:${process.env.PATH ?? ""}`,
      },
    },
  );
}

const validWorkspace = await createWorkspace("valid", {
  schemaVersion: 1,
  protocolVersion: 1,
  packageName: cliPackage,
  packageVersion: lockedVersion,
  workspaceId: "resolver-test",
});
const valid = invoke(validWorkspace);
assert.equal(valid.status, 0, valid.stderr);
assert.deepEqual(JSON.parse(await readFile(auditPath, "utf8")), [
  "--yes",
  "--package",
  `${cliPackage}@${lockedVersion}`,
  "--",
  "tiangong-ai",
  "research",
  "setup",
  "status",
  "--json",
]);

const plannedWorkspace = await createWorkspace("planned");
await writeFile(
  join(plannedWorkspace, ".tiangong-research", "setup-plan.json"),
  JSON.stringify({
    schemaVersion: 1,
    kind: "tiangong-research-setup-plan",
    cli: { package: cliPackage, version: "8.7.6" },
    planSha256: "a".repeat(64),
  }),
  "utf8",
);
await writeFile(
  join(plannedWorkspace, ".tiangong-research", "setup-state.json"),
  JSON.stringify({
    schemaVersion: 1,
    status: "pending",
    currentStep: null,
    lastError: null,
  }),
  "utf8",
);
const planned = invoke(plannedWorkspace);
assert.equal(planned.status, 0, planned.stderr);
assert.deepEqual(JSON.parse(await readFile(auditPath, "utf8")), [
  "--yes",
  "--package",
  `${cliPackage}@8.7.6`,
  "--",
  "tiangong-ai",
  "research",
  "setup",
  "status",
  "--json",
]);

const injectionWorkspace = await createWorkspace("injection", {
    schemaVersion: 1,
    packageName: cliPackage,
    packageVersion: "9.8.7;echo-token",
});
const injection = invoke(injectionWorkspace);
assert.equal(injection.status, 2);
assert.match(injection.stderr, /AUTO_RESEARCH_RUNTIME_VERSION_INVALID/);
assert.doesNotMatch(injection.stderr, /echo-token/);

const missingWorkspace = await createWorkspace("missing");
const missing = invoke(missingWorkspace);
assert.equal(missing.status, 2);
assert.match(missing.stderr, /AUTO_RESEARCH_RUNTIME_LOCK_REQUIRED/);

const symlinkWorkspace = await createWorkspace("symlink");
const realLock = join(root, "real-lock.json");
await writeFile(
  realLock,
  JSON.stringify({
    schemaVersion: 1,
    packageName: cliPackage,
    packageVersion: lockedVersion,
  }),
  "utf8",
);
await symlink(realLock, join(symlinkWorkspace, ".tiangong-research", "runtime-lock.json"));
const linked = invoke(symlinkWorkspace);
assert.equal(linked.status, 2);
assert.match(linked.stderr, /AUTO_RESEARCH_RUNTIME_LOCK_INVALID/);

for (const result of [injection, missing, linked]) {
  const parsed = JSON.parse(result.stderr);
  assert.equal(parsed.error.details.executionMode, "workspace-locked");
  assert.equal(parsed.error.details.credentialScope, "broker");
  assert.equal(parsed.error.details.networkAttempted, false);
  assert.equal(typeof parsed.error.details.minimumAction, "string");
}

process.stdout.write("tiangong-auto-research runtime resolver tests passed\n");
