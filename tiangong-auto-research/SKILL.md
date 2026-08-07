---
name: tiangong-auto-research
description: Run smoke-test or production Tiangong research workspaces with explicit evidence requirements, external evidence Skills for the public internet and owner-whitelisted databases, immutable inputs and broker evidence, pre-call budgets, isolated producer execution, independent review, and mechanical closure. Use when a user asks to plan, initialize, configure, execute, recover, inspect, or close a traceable research project. Do not use for a single Tiangong edge-search request.
---

# Tiangong Auto Research

Use the pinned CLI for every workspace operation:

```bash
npx --yes @tiangong-ai/cli@0.0.23 research --help
```

The CLI is the authority for the external capability catalog, output schemas,
coverage gates, scheduling, budgets, retries, evidence promotion, review, and
closure. Do not reimplement those rules in this skill or a coordinating agent.

## Choose the mode first

- Use `smoke-test` only for deterministic mocks, routing checks, workflow demos,
  or a low-cost canary the user explicitly accepts.
- Use `production-research` for conclusions the user intends to rely on.
  Production must include a locked external public-internet capability; local
  inputs alone cannot represent internet coverage.

Load the detailed references only when needed:

- Read [references/external-skills.md](references/external-skills.md) for a new
  or clean workspace, profile selection, external Skill installation, status
  interpretation, provider checks, or an owner-whitelisted database.
- Read [references/production-research.md](references/production-research.md)
  before initializing or resuming production work, configuring broker HTTP,
  recovering a project, or interpreting a blocked run.
- Read [references/env.md](references/env.md) before configuring credentials,
  agent authentication, or an agent wrapper.

## Inspect before mutation

Resolve workspace and input paths to absolute paths, then run:

```bash
npx --yes @tiangong-ai/cli@0.0.23 research context inspect \
  --path /absolute/path/to/workspace --json
```

Follow `role` and `allowedOperations` exactly:

- `unmanaged`: initialize only when the user wants a new workspace.
- `workspace`: continue through CLI commands.
- `invalid`: stop and report the violations; never repair state by hand.

## Prepare external evidence capabilities

For production, inspect the complete external recommendation and status catalog
before project initialization:

```bash
npx --yes @tiangong-ai/cli@0.0.23 research capability catalog \
  --path /absolute/path/to/workspace --json
```

Show the user the recommended, enhanced, conditional, and evaluated-but-not-
selected Skills, their provider/key requirements, and current installation
status. Run only the exact pinned project installation plan the catalog returns,
outside the research runtime and after the user has selected a profile. Never
install dependencies from an agent package or silently substitute a provider.

Initialize the workspace, configure the explicit profile, store the logical
credential from an owner environment variable, and test the selected endpoints:

```bash
npx --yes @tiangong-ai/cli@0.0.23 research workspace init \
  /absolute/path/to/workspace --name "Research name" \
  --mode production-research --json
npx --yes @tiangong-ai/cli@0.0.23 research capability configure \
  --profile internet-research \
  --skill-root /absolute/path/to/workspace/.agents/skills \
  --workspace /absolute/path/to/workspace --json
npx --yes @tiangong-ai/cli@0.0.23 research capability credential set \
  --id brave.search.api-key --from-env BRAVE_SEARCH_API_KEY \
  --workspace /absolute/path/to/workspace --json
npx --yes @tiangong-ai/cli@0.0.23 research capability doctor --live \
  --workspace /absolute/path/to/workspace --json
```

Stop on a missing Skill, missing key, unsupported subscription endpoint, drift,
or failed live check. Do not downgrade a selected profile without telling the
user. Import an owner-whitelisted database only from its reviewed external
definition, then rerun credential setup and live doctor as described in the
external-Skills reference.

## Preflight before project creation

Prepare evidence requirements with `dimensions`, `sourceTypes`, `minSources`,
`minFullTextSources`, `minDatedSources`, and nullable `publicationDateFrom` /
`publicationDateTo`. For large local sources, also prepare one immutable input
plan with bounded `contextPath` or `contextRanges` entries.

```bash
npx --yes @tiangong-ai/cli@0.0.23 research project preflight \
  --workspace /absolute/path/to/workspace \
  --question "Research question" \
  --requirements /absolute/path/to/evidence-requirements.json \
  --input-plan /absolute/path/to/input-plan.json --json
```

Stop when production requirements are missing, the capability/input plan cannot
cover them, or the projected budget needs confirmation. Pass `--confirm-budget`
only after the user explicitly accepts the threshold. Omit `--input-plan` only
when locked broker capabilities alone provide the declared acquisition plan.

Never edit `workspace.json`, `runtime-lock.json`, `journal.jsonl`, evidence
objects/receipts, project state, run records, locks, or outputs directly.

## Freeze capabilities and register the project

After every external tree or policy change, verify the content lock:

```bash
npx --yes @tiangong-ai/cli@0.0.23 research capability lock \
  --workspace /absolute/path/to/workspace --json
npx --yes @tiangong-ai/cli@0.0.23 research capability verify \
  --workspace /absolute/path/to/workspace --json
```

Register the project with the same verified requirements and input plan used by
preflight:

```bash
npx --yes @tiangong-ai/cli@0.0.23 research project init PROJECT \
  --workspace /absolute/path/to/workspace \
  --question "Research question" \
  --requirements /absolute/path/to/evidence-requirements.json \
  --input-plan /absolute/path/to/input-plan.json \
  --confirm-budget --json
```

Use `project input add` only for an additional small source that may be exposed
in full. Use `primary` for direct evidence, `reference` for context, and
`replication` for reproducibility material. Do not copy unregistered files into
the control directory.

## Validate and run

Configure explicit producer/reviewer models and pricing in the generated
workspace config. Production requires different agent families and both real
capsule and provider-capability smokes:

```bash
npx --yes @tiangong-ai/cli@0.0.23 research workspace doctor \
  --workspace /absolute/path/to/workspace \
  --agent-smoke --capability-smoke --json
npx --yes @tiangong-ai/cli@0.0.23 research run \
  --workspace /absolute/path/to/workspace --project PROJECT --dry-run --json
npx --yes @tiangong-ai/cli@0.0.23 research run \
  --workspace /absolute/path/to/workspace --project PROJECT \
  --max-cycles 20 --progress-jsonl --json
```

Proceed only when doctor reports `ready`. Prefer an explicit `--project`; omit
it only for an intentional workspace-wide batch. The runtime embeds locked
external Skill instructions and lets discovery call only the scoped broker.
Do not bypass it with direct agents, shell commands, or unbrokered web calls.
When coverage is insufficient, report the gaps and stop before downstream
packages consume budget.

## Inspect, recover, and hand off

```bash
npx --yes @tiangong-ai/cli@0.0.23 research status \
  --workspace /absolute/path/to/workspace --project PROJECT --json
```

Report the package, classified failure, retry time, split token usage, cost,
sanitized provider diagnostics, and limitations. Use `research project retry`
or `research project fork` only with explicit user direction; never reset state
or delete capsules/evidence by hand.

A complete project has passing independent review and
`outputs/closure.json`. Return the report path, permanent evidence receipts,
content-addressed review packet/context locators, usage, review decision, and
material limitations. Treat `outputs/report.md` as the deliverable and
`outputs/closure.json` as its mechanical completion receipt.
