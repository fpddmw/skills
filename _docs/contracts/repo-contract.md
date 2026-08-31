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
lastReviewedAt: 2026-08-31
lastReviewedCommit: f61170eb09f7b1a518f023bd80810ad36895f70d
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
- Atomic data Skills own trigger semantics, source guidance, limitations, and
  an exact compatibility binding. Tiangong CLI owns connector execution,
  machine schemas, HTTP/authentication, retries, errors, and core receipts.
  A data Skill must not merge before its referenced CLI contract is available
  from an approved package, and it must not retain a second runtime after
  migration.
- Default Research Policy assets must remain conservative, non-secret, and
  visibly generic. They must not claim target-journal fit or acceptance; user
  customization, approval, expiry, and hash enforcement belong to the CLI and
  research workspace.
- Scientific-design guidance and defaults must preserve the native-producer
  boundary, distinguish observation from model comparison/scenario/accounting,
  distinguish byte identity from model executability, bind pending model,
  environment, and uncertainty objects to explicit future gates, require the
  public content-addressed intake for frozen raw model/environment bytes rather
  than manual control-store writes, use null bindings for genuinely pending
  objects, require exact
  joint-state mappings, bind real-record construct artifacts only after frozen
  acquisition and typed-content snapshots, require exact decomposition lineage
  and evidence atoms before inference, never treat resampling as additional
  independent data, require a reproduced analysis/Claim-Evidence Graph/
  submission-package chain for publication, and defer closed schemas,
  mechanical gates, reviewer-family/session enforcement, lifecycle budgets,
  authoritative generations, and semantic portable-audit verification to the
  CLI.
- Sandboxed-IDE adapters must remain thin routers to the canonical
  `tiangong-auto-research` Skill. They must preserve Default Permission, record
  WorkBuddy/CodeBuddy honestly as the native producer, require an explicit
  reviewer transport, and forbid Full Access, nested-sandbox bypass, arbitrary
  sidecar commands, or silent transport fallback.

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
- When a Skill changes an exact external CLI pin, add or update an offline
  stale-pin contract and run its clean temporary install/command-surface smoke.
- When migrating an atomic data Skill, verify its capability, operation,
  minimum CLI version, manifest digest, and input/output schema digests against
  the exact CLI package before removing the old executable implementation.
- For `tsinghua-graduate-thesis` PDF renderer changes, preserve the privacy-safe
  embedded Adobe-GB1 binary fixture, observe the behavior regression in its
  targeted clean container, and turn it green in a separate container. The
  fixture must contain no user thesis data or restricted font binary.
- Regenerate or update agent config files when the skill workflow requires it.
- Run Auto Research red/green cycles in separate clean runtime containers;
  valid Docker build layers may be reused iteratively, while PR and release
  evidence must include the explicit cold-build gate.
- Do not leave install, validation, or trigger facts only in chat.

`.claude-plugin/marketplace.json` is curated marketplace grouping metadata. It
may be a subset of installable skill directories unless the marketplace file is
explicitly updated to include every skill.
