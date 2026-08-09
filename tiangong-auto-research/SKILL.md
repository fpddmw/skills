---
name: tiangong-auto-research
description: Orchestrate open-ended, multi-source, evidence-backed research, especially when an ancestor directory contains `.tiangong-research` or the user asks to research, investigate, build on prior outputs, compare evidence, form a conclusion, or produce a reviewed research artifact. Also use for setup, preflight, execution, recovery, review, and closure. In an Auto Research workspace this Skill takes precedence over individual web, news, SCI, report, patent, download, or document Skills unless the user explicitly requests one isolated standalone operation outside the research workflow. Covers Chinese requests such as “研究一下”, “朝这个方向做一做”, “结合已有成果继续研究”, “查资料并形成结论”, and “系统梳理证据”.
---

# Tiangong Auto Research

For an existing workspace, use the bundled resolver for every CLI operation.
Resolve `AUTO_RESEARCH_CLI` from this loaded Skill's absolute directory; do not
guess a global Skill path. The resolver accepts only the CLI package and exact
stable version from the workspace's regular non-symlink runtime lock:

```bash
AUTO_RESEARCH_CLI=/absolute/path/to/installed/tiangong-auto-research/scripts/research_cli.mjs
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research --help
```

For a clean directory without a runtime lock, read
[references/setup.md](references/setup.md) and ask the user to choose one
reviewed exact bootstrap CLI version. Never substitute `latest`, a range, a
tag, a path, or a command fragment. After setup creates the runtime lock, use
the resolver above.

The CLI owns the setup catalog, immutable setup plan, capability policy, output
schemas, coverage gates, budgets, retries, evidence promotion, review, and
closure. Do not reproduce those contracts in an agent prompt or edit control
files by hand.

## Route to the right reference

- Read [references/setup.md](references/setup.md) for a new or clean directory,
  the guided Wizard, non-interactive setup, updates, or recovery.
- Read [references/external-skills.md](references/external-skills.md) before
  selecting a recommended external Skill, provider, license, or execution role.
- Read [references/env.md](references/env.md) before configuring credentials,
  agent authentication, provider checks, or wrappers.
- Read [references/production-research.md](references/production-research.md)
  before production preflight, execution, recovery, or closure.

## Choose the mode before spending budget

- `smoke-test` is for deterministic mocks, routing/eval checks, workflow demos,
  and explicitly accepted low-cost canaries.
- `production-research` is for conclusions a user may rely on. It requires a
  locked independent public-internet evidence profile. Local evidence and an
  owner database can supplement that profile but cannot replace it.

## Inspect before mutation

The workspace may be any user-selected directory; example paths are
placeholders, never defaults. Resolve paths to absolute paths, then inspect the
directory:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research context inspect \
  --path /absolute/path/to/workspace --json
```

Follow `role` and `allowedOperations`. Stop on `invalid`; never repair immutable
state, locks, journal events, evidence objects, receipts, or outputs manually.

For a clean directory, show the read-only ecosystem catalog and run the guided
setup only after the user asks to configure it:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research setup catalog \
  --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research setup \
  --workspace /absolute/path/to/workspace --json
```

The user must explicitly confirm the recommended project-local
`tiangong-auto-research` orchestrator and every external source, then accept its
pinned license. The Wizard defaults evidence to Brave web/news; context and
media remain visibly subscription-dependent choices. For each selected
credential it offers hidden input, an owner environment variable, preloaded
stdin/password-manager input, or an explicit skip; read
[references/env.md](references/env.md) for the exact safe paths. It may create a
plan without applying it. Never silently install a Skill, write globally,
substitute a provider, downgrade a profile, or accept a license. Missing
required credentials must block before any source download.

## Preserve execution boundaries

- Brave and owner-whitelisted SCI/report/patent sources are
  `evidence-capability` Skills only after their exact capabilities are locked.
  Discovery calls them only through the scoped broker and locked manifest
  method. Never execute their standalone shell examples from a research
  workflow or expose broker credentials to an agent.
- `document-granular-decompose` is an `input-preprocessor`. Run it explicitly
  through `research setup companion run`, then admit the exact hash-bound output
  as a project input. Its output is not evidence merely because parsing worked.
- `academic-paper-download` is an `acquisition-adapter`. Its automatic OA order
  remains Unpaywall, Semantic Scholar OA, then arXiv. If all are exhausted, the
  CLI reports an explicit browser handoff; it never launches or chooses a
  browser automatically.
- Document/PDF/spreadsheet/presentation Skills are `post-closure-authoring`.
  They may format a closed report but cannot produce or alter admitted evidence,
  analysis, review, or closure.
- For PPT creation, prefer `hugohe3.ppt-master`. Keep `anthropic.pptx` as a
  compatible situational option; both may be selected explicitly in one plan.

## Preflight and initialize a project

Prepare evidence requirements with `dimensions`, `sourceTypes`,
`requiredCapabilityIds`, `requiredDiscoveryScopes`, `minSources`,
`minFullTextSources`, `minDatedSources`, and nullable date bounds. Require each
owner database that the question must actually exercise; Brave or local files
cannot mask an undeclared report, patent, or other whitelisted capability. For
large local sources, create an immutable input plan with bounded context files
or ranges.

First require the current setup generation and its real production checks to
pass. One setup doctor invocation performs the nested workspace/capability and
agent capsule smokes, so do not repeat paid smokes unnecessarily:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research setup status \
  --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research setup doctor \
  --workspace /absolute/path/to/workspace --live \
  --agent-smoke --confirm-agent-smoke-cost --json
```

Stop unless readiness is `READY`. An explicitly requested smoke failure is
`BLOCKED`, not advisory.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project preflight \
  --workspace /absolute/path/to/workspace \
  --question "Research question" \
  --requirements /absolute/path/to/evidence-requirements.json \
  --input-plan /absolute/path/to/input-plan.json --json

node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project init PROJECT \
  --workspace /absolute/path/to/workspace \
  --question "Research question" \
  --requirements /absolute/path/to/evidence-requirements.json \
  --input-plan /absolute/path/to/input-plan.json \
  --confirm-budget --json
```

Stop when capability/input coverage is insufficient or the projected cost has
not been accepted. Use the same requirements and input plan for preflight and
initialization.

## Validate, run, and recover

Production requires explicit models/prices, different producer and reviewer
agent families, a current setup doctor report, and real capsule/provider smoke
attestations. After successful preflight and initialization, use dry-run before
the paid run:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research run \
  --workspace /absolute/path/to/workspace --project PROJECT --dry-run --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research run \
  --workspace /absolute/path/to/workspace --project PROJECT \
  --max-cycles 20 --progress-jsonl --json
```

Proceed only when doctor reports ready. Discovery uses only locked broker
capabilities; later stages are tool-free. Doctor, preflight, dependency,
provider, and evidence-coverage failures must stop the workflow. Never silently
downgrade a systematic task to a standalone SCI, report, patent, web, or paper
operation; only the user may explicitly narrow the request to one isolated
standalone operation.

Inspect state with `research status`. Use `research project retry` or
`research project fork` only with explicit user direction; do not reset or
delete state. A complete project has a passing independent review,
`outputs/report.md`, and `outputs/closure.json`. Return the permanent evidence
locators, review-packet binding, usage/cost, decision, and material limitations.
