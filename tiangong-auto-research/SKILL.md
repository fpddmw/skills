---
name: tiangong-auto-research
description: Run smoke-test or production Tiangong research workspaces with explicit evidence requirements, immutable inputs and broker evidence, capability locks, pre-call budgets, isolated producer execution, independent review, and mechanical closure. Use when a user asks to plan, initialize, configure, execute, recover, inspect, or close a traceable research project. Do not use for a single Tiangong edge-search request.
---

# Tiangong Auto Research

Use the pinned CLI for every workspace operation:

```bash
npx --yes @tiangong-ai/cli@0.0.21 research --help
```

The CLI is the only authority for output schemas, coverage gates, scheduling,
budget accounting, retries, evidence promotion, review, and closure. Do not
reimplement those rules in this skill or a coordinating agent.

## Choose the mode first

- Use `smoke-test` only for routing checks, deterministic mocks, workflow demos,
  or a low-cost canary the user explicitly accepts.
- Use `production-research` for conclusions the user intends to rely on. Never
  present a minimal Crossref/search capability as production coverage.

Read [references/production-research.md](references/production-research.md)
before initializing or resuming production work, configuring broker HTTP,
recovering a project, or interpreting a blocked run. Read
[references/env.md](references/env.md) before configuring credentials or agent
authentication or an agent wrapper.

## Inspect before mutation

Resolve workspace and input paths to absolute paths, then run:

```bash
npx --yes @tiangong-ai/cli@0.0.21 research context inspect \
  --path /absolute/path/to/workspace --json
```

Follow `role` and `allowedOperations` exactly:

- `unmanaged`: initialize only when the user wants a new workspace.
- `workspace`: continue through CLI commands.
- `invalid`: stop and report the violations; never repair state by hand.

## Initialize and preflight

```bash
npx --yes @tiangong-ai/cli@0.0.21 research workspace init \
  /absolute/path/to/workspace --name "Research name" \
  --mode production-research --json
```

Before project initialization, prepare an evidence-requirements JSON object
with `dimensions`, `sourceTypes`, `minSources`, `minFullTextSources`,
`minDatedSources`, and nullable `publicationDateFrom` / `publicationDateTo`
boundaries. For large local sources, also prepare one immutable input plan with
bounded `contextPath` or `contextRanges` entries as described in the production
reference. Then generate the required evidence/capability/gap/cost checklist:

```bash
npx --yes @tiangong-ai/cli@0.0.21 research project preflight \
  --workspace /absolute/path/to/workspace \
  --question "Research question" \
  --requirements /absolute/path/to/evidence-requirements.json \
  --input-plan /absolute/path/to/input-plan.json --json
```

Stop when production requirements are missing, the capability/input plan
cannot cover them, or the projected budget needs confirmation. Pass
`--confirm-budget` only after the user explicitly accepts the threshold.
Omit `--input-plan` only when locked broker capabilities alone provide the
declared acquisition plan; otherwise pass the identical verified plan to
preflight and `project init`.

Never edit `workspace.json`, `runtime-lock.json`, `journal.jsonl`, evidence
objects/receipts, project state, run records, locks, or outputs directly.

## Admit and freeze capabilities

Method skills remain external and use absolute `skillPath` declarations.
Current permissions are `project-read`, `candidate-write`,
`brokered-network`, and `controlled-command`.

For brokered HTTP, declare exact `allowedHosts` and one `http` policy with an
exact `accept`, allowed content types, maximum response bytes, and maximum
items. Declare capability `coverage` when preflight should match dimensions,
source types, full-text access, or publication dates. Workspace config applies
an additional estimated-token cap to staged broker context. Credential scopes
must be subsets of capability hosts. Lock after every tree or policy change:

```bash
npx --yes @tiangong-ai/cli@0.0.21 research capability lock \
  --workspace /absolute/path/to/workspace --json
npx --yes @tiangong-ai/cli@0.0.21 research capability verify \
  --workspace /absolute/path/to/workspace --json
```

## Register the project and inputs

```bash
npx --yes @tiangong-ai/cli@0.0.21 research project init gpu-resource-impact \
  --workspace /absolute/path/to/workspace \
  --question "Research question" \
  --requirements /absolute/path/to/evidence-requirements.json \
  --input-plan /absolute/path/to/input-plan.json \
  --confirm-budget --json

npx --yes @tiangong-ai/cli@0.0.21 research project input add gpu-resource-impact \
  --workspace /absolute/path/to/workspace \
  --path /absolute/path/to/inventory.csv --role primary --json
```

Use `primary` for direct evidence, `reference` for context, and `replication`
for reproducibility material. Use `project input add` only for an additional
small source that should be exposed in full; use the verified input plan when
producer context must be bounded. Do not copy unregistered files into the
control directory.

## Validate and run

Production work requires the real producer and reviewer capsule smoke:

```bash
npx --yes @tiangong-ai/cli@0.0.21 research workspace doctor \
  --workspace /absolute/path/to/workspace --agent-smoke --json
```

Proceed only when status is `ready`. Preview scheduling, then expose the JSONL
progress stream for long runs:

```bash
npx --yes @tiangong-ai/cli@0.0.21 research run \
  --workspace /absolute/path/to/workspace --project PROJECT --dry-run --json
npx --yes @tiangong-ai/cli@0.0.21 research run \
  --workspace /absolute/path/to/workspace --project PROJECT \
  --max-cycles 20 --progress-jsonl --json
```

Prefer an explicit `--project` for a single-project task. It scopes production
budget confirmation, scheduling, result summaries, progress events, and exit
status to that project, so older blocked siblings remain auditable without
contaminating the current run. Omit it only for an intentional workspace-wide
batch.

Do not bypass the runtime with direct agents or web calls. When evidence
coverage is insufficient, report the gaps and stop; do not spend analyze,
synthesize, or review budget. The production smoke attestation is valid for 24
hours and is bound to config, capabilities, schemas, and resolved runtimes;
rerun doctor after expiry or any drift instead of bypassing it.

## Inspect, recover, and hand off

```bash
npx --yes @tiangong-ai/cli@0.0.21 research status \
  --workspace /absolute/path/to/workspace --json
```

Report the package, classified failure, retry time, split token usage, cost,
sanitized provider diagnostics, and limitations. Use `research project retry`
or `research project fork` only with explicit user direction; these commands
preserve promoted artifacts and append management events. Never reset state or
delete capsules/evidence by hand.

A complete project must contain passing independent review and
`outputs/closure.json`. Return the status, artifact paths, permanent evidence
receipts, content-addressed review packet/context locators, usage, review
decision, and material limitations. Treat `outputs/report.md` as the
deliverable and `outputs/closure.json` as its mechanical completion receipt.
