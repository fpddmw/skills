---
docType: guide
scope: repo
status: current
authoritative: true
owner: skills
language: en
whenToUse: "When installing, updating, or using the Tiangong AI reusable skills repository."
whenToUpdate: "When install commands, target agents, scope behavior, environment variables, or available skill guidance changes."
checkPaths:
  - AGENTS.md
  - .docpact/config.yaml
  - README.zh-CN.md
  - .claude-plugin/**
  - "*/SKILL.md"
lastReviewedAt: 2026-08-31
lastReviewedCommit: fe16c358d834fd0b8551365396d9eb4da52721c1
---

# Tiangong AI Skills

Repository: https://github.com/tiangong-ai/skills

Use the `skills` CLI from https://github.com/vercel-labs/skills to install, update, and manage these skills.

## Atomic data skills

Eighteen local candidate Skills—AirNow Hourly Observations, Federal Register Documents, NASA
FIRMS Active Fire, OpenAQ Air Quality, Regulations.gov Comments,
Regulations.gov Comment Details, USBR RISE, USGS Water IV, three Open-Meteo sources, and
GDELT DOC, Events, GKG, and Mentions, plus Bluesky Cascades, YouTube Video
Search, and YouTube Comments—are thin semantic Skills over the Tiangong CLI
TypeScript 7 data runtime. Each candidate records one exact CLI package binding
in `references/tiangong-data-binding.json`; the agent uses CLI `data describe`
for current source facts and `data run` for the bound operation. These Skills
contain no second provider connector runtime.

This candidate set is not a complete EcoCouncil migration. The authoritative
EcoCouncil source baseline, `main@ac19289b4876d8a90595a0270721ef3f5ee7ced8`,
contains 21 `source-fetch` Skills. Three are not yet represented here—EPA EIS
records, Regulations.gov attachments, and USBR project records—and the other
17 existing candidates still require source-semantic revalidation before a
Skills PR.

See `_docs/architecture/atomic-data-capabilities.md` and
`_docs/runbooks/atomic-data-skill-migration.md` for the ownership boundary,
candidate inventory, source audit correction, staged migration order, and
release gates. The audited
RSS/full-text, Figshare-download, academic-paper, Tiangong/KB, and private-email
candidates retain their existing content, artifact, product, research, or
security boundaries instead of being narrowed into stateless data connectors.

## Install the CLI

```bash
npm i skills -g
```

## Install

- List available skills (no install):
  ```bash
  npx skills add https://github.com/tiangong-ai/skills --list
  ```
- Install all skills (project scope by default):
  ```bash
  npx skills add https://github.com/tiangong-ai/skills
  ```
- Install specific skills:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills --skill tiangong-auto-research --skill tiangong-kb-sci-search
  ```
- Install the Tsinghua graduate thesis LaTeX workflow:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills --skill tsinghua-graduate-thesis
  ```
- Install the Tiangong KB ingest workflow:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills --skill tiangong-kb-ingest
  ```
- For a WorkBuddy/CodeBuddy producer, install the thin adapter beside the
  canonical orchestrator:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills --skill tiangong-auto-research --skill tiangong-auto-research-workbuddy
  ```

## Target agents and scope

- Target specific agents:
  ```bash
  npx skills add https://github.com/tiangong-ai/skills -a codex -a claude-code
  ```
- Install globally (user scope):
  ```bash
  npx skills add https://github.com/tiangong-ai/skills -g
  ```
- Scope notes:
  - Codex is a universal agent: project scope uses `./.agents/skills`, and
    global scope uses `$HOME/.agents/skills`. `CODEX_HOME` does not change the
    `skills@1.5.22` global destination.
  - Claude Code project scope uses `./.claude/skills`; global scope uses
    `$CLAUDE_CONFIG_DIR/skills` when set, otherwise `$HOME/.claude/skills`.
  - Other agents have their own directories; inspect the exact path reported by
    the pinned `skills` CLI rather than deriving `~/<agent>/skills` by analogy.

## Install method

- Interactive installs let you choose:
  - Symlink (recommended)
  - Copy

## Update and verify

- List installed skills:
  ```bash
  npx skills list
  ```
- Check for updates:
  ```bash
  npx skills check
  ```
- Update all skills:
  ```bash
  npx skills update
  ```

## Environment Variables

Environment requirements live with each skill. Before using a skill that calls
an external service, read that skill's `references/env.md` when present.
`npx skills add` installs or links skill files; it does not provision language
runtimes or execute post-install hooks. When a skill provides a locked runtime
bootstrap and smoke command, run those explicit steps from its own instructions.

## Tiangong KB Ingest Compatibility

`tiangong-kb-ingest` uses the exact reviewed CLI 0.0.48 distribution. After
installing or updating the Skill, run the credential-free compatibility smoke:

```bash
npx --yes --package "@tiangong-ai/cli@0.0.48" -- tiangong-ai --version
npx --yes --package "@tiangong-ai/cli@0.0.48" -- tiangong-ai kb --help
```

The version command must print `0.0.48`; KB help must list the bulk scan,
metadata dry-run, collection list/schema, ingest, and status surfaces. These
checks do not call or mutate the KB backend.

## Auto Research External Skill Setup

Use `npx skills` directly for ordinary Skill management. For an Auto Research
workspace, prefer the CLI's guarded setup layer: it still uses the exact pinned
`skills` CLI underneath, while also binding source commits, tree hashes,
destinations, license choices, safe credential bindings, and audit state. The
Wizard lets ordinary users enter each selected key with hidden TTY input; named
environment variables and bounded stdin/password-manager input remain explicit
alternatives. Start with the read-only catalog or the guided Wizard:

```bash
REVIEWED_BOOTSTRAP_CLI_VERSION=X.Y.Z # replace with one reviewed exact stable release
npx --yes --package "@tiangong-ai/cli@$REVIEWED_BOOTSTRAP_CLI_VERSION" -- \
  tiangong-ai research setup catalog \
  --workspace /absolute/path/to/workspace --json
npx --yes --package "@tiangong-ai/cli@$REVIEWED_BOOTSTRAP_CLI_VERSION" -- \
  tiangong-ai research setup \
  --workspace /absolute/path/to/workspace
```

For repeatable non-interactive provisioning, run `research setup init` first.
It creates a no-overwrite schema-v2 `.tiangong-research/setup.yaml` template
plus `setup.env.example`. The YAML explicitly lists every current catalog Skill,
credential, and setting, including disabled optional entries; the env example
lists every credential variable with an empty placeholder. After the user
reviews the current catalog, enabled states, licenses, models, pricing, checks,
and confirmations, bare `research setup` detects only that workspace-local YAML
and bypasses the Wizard. The optional real `setup.env` must be owner-only;
disabled credentials stay empty and a non-empty disabled value is rejected.
The removed v1 declaration and all invalid or incomplete declarations fail
closed; they never trigger an
interactive fallback or parent-directory search. Setup returns success only
after every selected dependency, provider live check, Policy compatibility
check, and independent reviewer smoke reaches complete readiness. Use explicit
`research setup wizard` to choose the interactive path. See
`tiangong-auto-research/references/setup.md` and
`tiangong-auto-research/references/env.md`.

The bootstrap version is an explicit new-workspace choice, never `latest`, a
tag, or a range. After apply creates `runtime-lock.json`, the installed
orchestrator's bundled resolver runs exactly that locked version for all
workspace operations.

The catalog also offers the `tiangong-auto-research` workflow orchestrator,
default-baseline Brave internet evidence, optional Tiangong SCI/document/paper
companions, and optional Anthropic or PPT Master post-closure authoring Skills.
The workspace can be any user-selected directory. Every entry is external,
separately licensed, pinned, and explicitly confirmed/selected; nothing is
bundled or installed by a research package. See
`tiangong-auto-research/references/setup.md` and `external-skills.md`.
`tiangong-auto-research-workbuddy` is only a sandboxed-IDE adapter. It routes
back to the canonical orchestrator and its signed reviewer-bridge reference;
it does not define a second research workflow.
For PPT creation, prefer PPT Master; Anthropic PPTX remains compatible and may
be selected alongside it when its workflow fits the task.

For a top-journal goal, the orchestrator includes a conservative Research
Policy template pack for article type, field, journal class, project brief, and
four independent final-review roles. The CLI Policy Wizard copies the selected
Markdown into the research workspace for human review; it reports when generic
defaults remain, requires explicit approval of the exact content hash, and
invalidates approval after any edit or expiry. Exact-journal readiness requires
current official guidance and substantive human customization. These gates can
produce a reviewable submission candidate, never a promise of editorial
acceptance.

Before discovery, the current native Codex or Claude host must also provide a
closed, target-specific scientific design. The CLI validates and freezes the
design. Frozen model implementations and environment locks must first enter the
workspace through `research scientific object register`; the Skill never asks
the user to hand-copy them into `.tiangong-research`. The CLI then enforces independent `research-design`, real-record
`evidence-construct`, and `pilot-methods` reviews before discovery, acquisition,
and analysis respectively. After acquisition it also requires exact
decomposition records, evidence atoms, a typed-content snapshot, a passing
inference snapshot, a reproduced analysis, and a mechanically generated
Claim-Evidence Graph. Publication freeze requires the complete manuscript
sections and explicit cover/title/checklist/availability/source-data files;
four fresh reviews must use the configured agent family that differs from the
native producer. It reserves the complete early/final-review and revision
lifecycle, requires reapproval for every authoritative recovery generation,
and exports a semantic-chain-verified portable audit directory containing exact
formal evidence rather than host-local pointers. The CLI remains the
deterministic control plane; it never launches a nested producer to invent the
science. See
`tiangong-auto-research/references/publication-policy.md` and
`tiangong-auto-research/references/scientific-design.md`.
