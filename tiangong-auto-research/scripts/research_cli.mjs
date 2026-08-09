#!/usr/bin/env node

import { lstat, readFile, realpath } from "node:fs/promises";
import { spawn } from "node:child_process";
import { isAbsolute, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const MAX_RUNTIME_LOCK_BYTES = 64 * 1024;
const EXACT_STABLE_SEMVER = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/;
const CLI_PACKAGE = "@tiangong-ai/cli";

class ResolverError extends Error {
  constructor(code, message, minimumAction) {
    super(message);
    this.name = "ResolverError";
    this.code = code;
    this.minimumAction = minimumAction;
  }
}

function resolverError(code, message, minimumAction) {
  return new ResolverError(code, message, minimumAction);
}

export async function resolveWorkspaceRuntime(workspaceInput) {
  if (typeof workspaceInput !== "string" || !isAbsolute(workspaceInput) || workspaceInput.includes("\0")) {
    throw resolverError(
      "AUTO_RESEARCH_WORKSPACE_INVALID",
      "The Auto Research workspace must be an absolute existing directory.",
      "Pass the exact absolute workspace path selected by the user.",
    );
  }

  let workspace;
  try {
    workspace = await realpath(workspaceInput);
    const workspaceStat = await lstat(workspace);
    if (!workspaceStat.isDirectory()) {
      throw new Error("not-directory");
    }
  } catch {
    throw resolverError(
      "AUTO_RESEARCH_WORKSPACE_INVALID",
      "The Auto Research workspace must be an absolute existing directory.",
      "Create or select the workspace directory, then run the reviewed bootstrap CLI once.",
    );
  }

  const lockPath = join(workspace, ".tiangong-research", "runtime-lock.json");
  let lockStat;
  try {
    lockStat = await lstat(lockPath);
  } catch {
    throw resolverError(
      "AUTO_RESEARCH_RUNTIME_LOCK_REQUIRED",
      "No Auto Research runtime lock is available for this workspace.",
      "For a new directory, explicitly choose a reviewed exact CLI version and run research setup. Do not use npm latest implicitly.",
    );
  }
  if (lockStat.isSymbolicLink() || !lockStat.isFile() || lockStat.size < 2 || lockStat.size > MAX_RUNTIME_LOCK_BYTES) {
    throw resolverError(
      "AUTO_RESEARCH_RUNTIME_LOCK_INVALID",
      "The Auto Research runtime lock must be a bounded regular non-symlink file.",
      "Restore the CLI-created runtime-lock.json or perform a reviewed setup upgrade; do not edit the lock by hand.",
    );
  }

  let lock;
  try {
    lock = JSON.parse(await readFile(lockPath, "utf8"));
  } catch {
    throw resolverError(
      "AUTO_RESEARCH_RUNTIME_LOCK_INVALID",
      "The Auto Research runtime lock is not valid JSON.",
      "Restore the CLI-created runtime-lock.json or perform a reviewed setup upgrade; do not edit the lock by hand.",
    );
  }

  if (
    lock === null ||
    typeof lock !== "object" ||
    Array.isArray(lock) ||
    lock.schemaVersion !== 1 ||
    lock.packageName !== CLI_PACKAGE ||
    typeof lock.packageVersion !== "string" ||
    !EXACT_STABLE_SEMVER.test(lock.packageVersion)
  ) {
    throw resolverError(
      "AUTO_RESEARCH_RUNTIME_VERSION_INVALID",
      "The Auto Research runtime lock does not contain the supported exact CLI package and stable version.",
      "Use the CLI setup upgrade workflow to create a reviewed exact runtime lock; tags, ranges, paths, and command fragments are forbidden.",
    );
  }

  return {
    packageName: CLI_PACKAGE,
    packageVersion: lock.packageVersion,
  };
}

function parseArguments(argv) {
  const separator = argv.indexOf("--");
  if (separator < 0 || separator === argv.length - 1) {
    throw resolverError(
      "AUTO_RESEARCH_RUNTIME_ARGUMENT_INVALID",
      "The locked CLI resolver requires a command after `--`.",
      "Use: research_cli.mjs --workspace /absolute/path -- research <command>.",
    );
  }
  const resolverArgs = argv.slice(0, separator);
  if (resolverArgs.length !== 2 || resolverArgs[0] !== "--workspace") {
    throw resolverError(
      "AUTO_RESEARCH_RUNTIME_ARGUMENT_INVALID",
      "The locked CLI resolver accepts only one explicit --workspace argument before `--`.",
      "Use: research_cli.mjs --workspace /absolute/path -- research <command>.",
    );
  }
  return {
    workspace: resolverArgs[1],
    command: argv.slice(separator + 1),
  };
}

function writeStructuredError(error) {
  const normalized =
    error instanceof ResolverError
      ? error
      : resolverError(
          "AUTO_RESEARCH_RUNTIME_EXEC_FAILED",
          "The locked Auto Research CLI could not be started.",
          "Verify Node.js 24, npx availability, and registry access, then retry the same locked command.",
        );
  process.stderr.write(
    `${JSON.stringify({
      error: {
        code: normalized.code,
        message: normalized.message,
        details: {
          executionMode: "workspace-locked",
          credentialScope: "broker",
          networkAttempted: false,
          minimumAction: normalized.minimumAction,
        },
      },
    })}\n`,
  );
}

async function run() {
  const parsed = parseArguments(process.argv.slice(2));
  const runtime = await resolveWorkspaceRuntime(parsed.workspace);
  const executable = process.platform === "win32" ? "npx.cmd" : "npx";
  const args = [
    "--yes",
    "--package",
    `${runtime.packageName}@${runtime.packageVersion}`,
    "--",
    "tiangong-ai",
    ...parsed.command,
  ];

  const exitCode = await new Promise((resolveExit, rejectExit) => {
    const child = spawn(executable, args, {
      env: { ...process.env, DO_NOT_TRACK: "1" },
      shell: false,
      stdio: "inherit",
    });
    child.once("error", rejectExit);
    child.once("exit", (code, signal) => resolveExit(code ?? (signal ? 1 : 0)));
  });
  process.exitCode = exitCode;
}

const isEntrypoint =
  process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url;
if (isEntrypoint) {
  try {
    await run();
  } catch (error) {
    writeStructuredError(error);
    process.exitCode = 2;
  }
}
