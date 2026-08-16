---
docType: contract
scope: repo
status: current
authoritative: true
owner: skills
language: en
whenToUse: "When deciding whether a change belongs in the skills repository."
whenToUpdate: "When ownership, skill format rules, generated agent config requirements, marketplace metadata, or completion criteria change."
checkPaths:
  - AGENTS.md
  - README.md
  - README.zh-CN.md
  - .claude-plugin/**
  - .docpact/config.yaml
  - .dockerignore
  - Dockerfile.clean-test
  - scripts/**
  - "*/SKILL.md"
lastReviewedAt: 2026-08-16
lastReviewedCommit: cc090cd161bff05cd41e15b74b7ef281165ae6d3
---

# Skills Repository Contract

## Ownership

This repository owns reusable agent skills, per-skill scripts, references,
assets, generated agent configuration files, README files, and curated
marketplace grouping metadata.

## Boundaries

- Project-level vendored skills under a consuming repository's `.agents/**`
  belong to that consuming repository.
- Root workspace governance, branch policy, and submodule integration remain in
  the workspace repository.
- Runtime credentials and user-private data do not belong in skill assets,
  references, or scripts.
- Default Research Policy assets must remain conservative, non-secret, and
  visibly generic. They must not claim target-journal fit or acceptance; user
  customization, approval, expiry, and hash enforcement belong to the CLI and
  research workspace.
- Scientific-design guidance and defaults must preserve the native-producer
  boundary, distinguish observation from model comparison/scenario/accounting,
  distinguish byte identity from model executability, bind pending model,
  environment, and uncertainty objects to explicit future gates, require exact
  joint-state mappings, never treat resampling as additional independent data,
  and defer closed schemas, mechanical gates, reviewer-session enforcement,
  lifecycle budgets, authoritative generations, and portable audit verification
  to the CLI.

## Skill Surface

Each skill directory must follow the repository `AGENTS.md` rules and the
Codex `skill-creator` guidance. Changes to `SKILL.md`, scripts, references,
assets, generated `agents/**` files, or marketplace metadata require review of:

- `AGENTS.md`
- `README.md`
- `README.zh-CN.md`
- `_docs/runbooks/development.md`
- `_docs/standards/documentation-standards.md`

## Completion Criteria

- Run `docpact route` before editing governed files.
- Run `docpact validate-config --root . --strict` after governance changes.
- For skill changes, run the applicable `skill-creator` validation workflow,
  including `scripts/quick_validate.py <skill-path>` from the `skill-creator`
  skill when available.
- Regenerate or update agent config files when the skill workflow requires it.
- Run Auto Research red/green cycles in separate clean runtime containers;
  valid Docker build layers may be reused iteratively, while PR and release
  evidence must include the explicit cold-build gate.
- Do not leave install, validation, or trigger facts only in chat.

`.claude-plugin/marketplace.json` is curated marketplace grouping metadata. It
may be a subset of installable skill directories unless the marketplace file is
explicitly updated to include every skill.
