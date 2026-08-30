import assert from "node:assert/strict";
import {
  existsSync,
  mkdtempSync,
  readFileSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import test from "node:test";
import { fileURLToPath } from "node:url";

const REPOSITORY_ROOT = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "../..",
);
const CLI_PACKAGE = process.env.TIANGONG_DATA_CLI_PACKAGE;
const CLI_VERSION = process.env.TIANGONG_DATA_CLI_VERSION;
const RUN_INSTALL_SMOKE =
  process.env.TIANGONG_DATA_SKILLS_RUN_INSTALL_SMOKE === "1";
const PILOTS = [
  {
    skill: "airnow-hourly-obs-fetch",
    capability: "airnow.hourly-observations",
    operation: "fetch-hourly",
  },
  {
    skill: "federal-register-doc-fetch",
    capability: "federal-register.documents",
    operation: "search",
  },
  {
    skill: "usgs-water-iv-fetch",
    capability: "usgs.water-instantaneous-values",
    operation: "fetch",
  },
];

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd,
    encoding: "utf8",
    env: options.env,
    input: options.input,
    maxBuffer: 32 * 1024 * 1024,
    timeout: 240_000,
  });
  if (result.error) {
    throw result.error;
  }
  return result;
}

function readInstalledBinding(consumer, pilot) {
  return JSON.parse(
    readFileSync(
      resolve(
        consumer,
        ".agents/skills",
        pilot.skill,
        "references/tiangong-data-binding.json",
      ),
      "utf8",
    ),
  );
}

test(
  "copy and symlink installs use one exact CLI without provider network access",
  { skip: !RUN_INSTALL_SMOKE },
  () => {
    assert.match(CLI_VERSION ?? "", /^\d+\.\d+\.\d+$/);
    assert.ok(CLI_PACKAGE, "TIANGONG_DATA_CLI_PACKAGE is required");

    const temporaryRoot = mkdtempSync(
      resolve(tmpdir(), "tiangong-data-skills-install-"),
    );
    try {
      const npmCache = resolve(temporaryRoot, "npm-cache");
      for (const installMode of ["copy", "symlink"]) {
        const consumer = resolve(temporaryRoot, installMode);
        const home = resolve(consumer, "home");
        const environment = {
          ...process.env,
          CI: "1",
          HOME: home,
          NO_COLOR: "1",
          npm_config_cache: npmCache,
        };
        for (const name of Object.keys(environment)) {
          if (/AIRNOW|FEDERAL_REGISTER|USGS|TIANGONG.*KEY/i.test(name)) {
            delete environment[name];
          }
        }
        assert.equal(run("mkdir", ["-p", home]).status, 0);
        assert.equal(run("git", ["init", "-q"], { cwd: consumer }).status, 0);

        for (const pilot of PILOTS) {
          const installArguments = [
            "--yes",
            "skills@1.5.22",
            "add",
            REPOSITORY_ROOT,
            "--skill",
            pilot.skill,
            "--agent",
            "codex",
            "--yes",
          ];
          if (installMode === "copy") {
            installArguments.push("--copy");
          }
          const install = run("npx", installArguments, {
            cwd: consumer,
            env: environment,
          });
          assert.equal(install.status, 0, install.stderr || install.stdout);

          const installed = resolve(
            consumer,
            ".agents/skills",
            pilot.skill,
          );
          for (const relative of [
            "SKILL.md",
            "agents/openai.yaml",
            "references/tiangong-data-binding.json",
          ]) {
            assert.equal(existsSync(resolve(installed, relative)), true, relative);
          }
          assert.equal(existsSync(resolve(installed, "scripts")), false);
          assert.equal(existsSync(resolve(installed, "assets")), false);
          const binding = readInstalledBinding(consumer, pilot);
          assert.equal(binding.generatedWithCliVersion, CLI_VERSION);
        }

        const cli = [
          "--yes",
          "--package",
          CLI_PACKAGE,
          "--",
          "tiangong-ai",
        ];
        const version = run("npx", [...cli, "--version"], {
          cwd: consumer,
          env: environment,
        });
        assert.equal(version.status, 0, version.stderr);
        assert.equal(version.stdout.trim(), CLI_VERSION);

        const catalog = run("npx", [...cli, "data", "catalog", "--json"], {
          cwd: consumer,
          env: environment,
        });
        assert.equal(catalog.status, 0, catalog.stderr);
        assert.equal(JSON.parse(catalog.stdout).capabilities.length >= 3, true);

        for (const pilot of PILOTS) {
          const describe = run(
            "npx",
            [...cli, "data", "describe", pilot.capability, "--json"],
            { cwd: consumer, env: environment },
          );
          assert.equal(describe.status, 0, describe.stderr);
          assert.equal(
            JSON.parse(describe.stdout).manifest.capabilityId,
            pilot.capability,
          );

          const doctor = run(
            "npx",
            [...cli, "data", "doctor", pilot.capability, "--json"],
            { cwd: consumer, env: environment },
          );
          assert.equal(doctor.status, 0, doctor.stderr);

          const binding = readInstalledBinding(consumer, pilot);
          const blocked = run(
            "npx",
            [
              ...cli,
              "data",
              "run",
              pilot.capability,
              pilot.operation,
              "--input",
              "-",
              "--json",
            ],
            {
              cwd: consumer,
              env: environment,
              input: `${JSON.stringify({
                schemaVersion: "tiangong.data.run-request.v1",
                capabilityId: pilot.capability,
                capabilityVersion: binding.capabilityVersion,
                operationId: pilot.operation,
                operationVersion: binding.operations[0].operationVersion,
                input: {},
              })}\n`,
            },
          );
          assert.notEqual(blocked.status, 0);
          assert.equal(JSON.parse(blocked.stdout).status, "blocked");
        }
      }
    } finally {
      rmSync(temporaryRoot, { recursive: true, force: true });
    }
  },
);
