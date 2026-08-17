---
docType: architecture
scope: repo
status: current
authoritative: true
owner: skills
language: en
whenToUse: "When changing skill directories, generated agent configs, repository README files, or marketplace metadata."
whenToUpdate: "When skill layout, install flow, validation workflow, or discovery metadata changes."
checkPaths:
  - AGENTS.md
  - README.md
  - README.zh-CN.md
  - .claude-plugin/**
  - "*/SKILL.md"
lastReviewedAt: 2026-08-17
lastReviewedCommit: c26f4b17d8e50cd04267a1d86ff9d3ad9a07039a
---

# Skills Repository Architecture

## Overview

The repository is a collection of reusable agent skills. Each skill is a
directory with a required `SKILL.md` and optional `scripts/`, `references/`,
`assets/`, and generated `agents/` resources.

## Key Paths

- `AGENTS.md`: mandatory repository-level skill creation and validation rules.
- `README.md` and `README.zh-CN.md`: install, update, target agent, and
  environment variable instructions.
- `.claude-plugin/marketplace.json`: curated marketplace grouping metadata; it
  may be a subset of installable skill directories.
- `*/SKILL.md`: individual skill entrypoint and trigger description.
- `*/scripts/**`: executable helpers used by skills.
- `*/references/**`: supporting reference material.
- `*/assets/**`: reusable skill assets or templates.
- `*/agents/**`: generated agent configuration files.

## Runtime Shape

Skills are consumed by external agent runtimes through the `skills` CLI or by
copy/symlink installation. Some skills require environment variables for
external APIs; those requirements belong in the relevant skill docs and the
repository README when broadly useful.

`tiangong-auto-research` documents both the interactive setup Wizard and the
CLI-owned declarative path. Its references explain fixed workspace-local YAML
discovery, complete explicit materialization of every current catalog Skill,
credential, and setting, owner-only env input with empty disabled options, no
interactive fallback after a declaration error, and complete-readiness gating.
The Skill does not duplicate the closed YAML schema or parse configuration; the
CLI-generated template and validator remain authoritative.

`tiangong-auto-research/assets/research-policy/defaults/**` is the versioned,
generic source pack for top-journal Policy initialization. It is immutable
source material, not a user Policy or journal endorsement. The CLI copies a
selected stack into the user-selected research workspace, where a human may
customize and explicitly approve the exact resolved content.

`tiangong-auto-research/references/scientific-design.md` defines the Skill-side
native workflow for a closed project-specific design, three early independent
scientific review gates, Policy-owned future freeze obligations for models,
environment locks, and source-derived uncertainty states, authoritative
recovery generations, and portable audit handoff. The Skill supplies
instructions and conservative defaults only. The CLI owns schemas, hashing,
stage admission, mechanical evaluation, lifecycle reservations, reviewer
isolation, and audit verification; native Codex or Claude remains the
scientific producer.

## Integration Points

- The root workspace pins this repository as a submodule.
- Consumers install skills into project or user agent directories.
- Marketplace metadata influences discovery and install ordering for the subset
  it lists.
