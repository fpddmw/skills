---
docType: index
scope: repo
status: current
authoritative: true
owner: skills
language: en
whenToUse: "When navigating skills repository documentation."
whenToUpdate: "When repository documentation layers, key docs, or governance routing change."
checkPaths:
  - AGENTS.md
  - .docpact/config.yaml
  - .github/workflows/docpact.yml
  - _docs/**
lastReviewedAt: 2026-08-19
lastReviewedCommit: b6654fa5fd942c48f2aa1ed99a92373a7304a3d2
---

# Skills Documentation

This directory contains the repo-local source documents governed by docpact.

## Layers

- Layer 0: `AGENTS.md` for mandatory agent entry guidance and skill-creator
  rules.
- Layer 1: `.docpact/config.yaml` for machine-readable governance.
- CI: `.github/workflows/docpact.yml` for config validation and PR
  documentation lint.
- Layer 2: current contracts, architecture, standards, and runbooks under
  `_docs/**`.

## Current Documents

- `_docs/contracts/repo-contract.md`: repository ownership, boundaries, and
  skill completion rules.
- `_docs/architecture/repo-architecture.md`: skill repository topology.
- `_docs/runbooks/development.md`: creation, validation, and marketplace update
  workflow.
- `_docs/standards/documentation-standards.md`: repo-local documentation rules.
