---
docType: runbook
scope: repo
status: current
authoritative: true
owner: skills
language: en
whenToUse: "When creating, updating, validating, or publishing reusable skills."
whenToUpdate: "When skill creation, generated agent config, validation, install, or marketplace update workflow changes."
checkPaths:
  - AGENTS.md
  - README.md
  - README.zh-CN.md
  - .dockerignore
  - Dockerfile.clean-test
  - .github/workflows/docpact.yml
  - scripts/**
  - .claude-plugin/**
  - "*/SKILL.md"
lastReviewedAt: 2026-08-12
lastReviewedCommit: 3d5d85f41bac03b7b31208e89d6c5e56da320baf
---

# Skills Development Runbook

## Before Editing A Skill

1. Read `AGENTS.md`.
2. Read the Codex `skill-creator` guidance required by `AGENTS.md`.
3. Run docpact route for the target skill paths.
4. Inspect existing skill resources before changing structure.

## New Skill Or Major Skill Update

Use the `skill-creator` workflow. Prefer the official initializer when creating
new skills, then fill in `SKILL.md`, optional `scripts/`, `references/`,
`assets/`, and generated `agents/**` files as required.

## Validation

Run:

```bash
docpact validate-config --root . --strict
```

For skill changes, run the validator required by `AGENTS.md` from the
`skill-creator` scripts directory, such as:

```bash
scripts/quick_validate.py <skill-path>
```

Run representative script tests when a skill script changes.

For `tiangong-auto-research` or its direct SCI/report/patent wrappers, use
test-driven development exclusively through the mandatory clean-room entrypoint
for the red and green cycles:

```bash
scripts/test-clean-container.sh
```

It builds from the digest-pinned Node 24 image, copies only the secret-filtered
repository context, and runs the routing, resolver, agent
wrapper, Research Policy pack, and evidence-wrapper suites as a non-root user
with isolated HOME and runtime networking disabled. Do not mount host agent
directories, CLIs, runtime caches, credentials, browser profiles, or source
worktrees into the running container.

The default entrypoint may reuse Docker layers whose declared inputs still
match, while every test run uses a new container. Run the explicit cold mode
after changing `.dockerignore`, `Dockerfile.clean-test`, or dependency inputs,
and before PR or release delivery:

```bash
scripts/test-clean-container.sh --cold-build
```

Cold mode adds `--no-cache`; neither mode uses `--pull`, so the base image can
change only through a reviewed digest update.

## README And Marketplace Updates

Update `README.md` and `README.zh-CN.md` when installation, environment
variables, target agents, or user-facing skill availability changes. Update
`.claude-plugin/marketplace.json` when marketplace discovery metadata changes.
