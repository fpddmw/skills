---
name: tiangong-auto-research
description: Run bounded, multi-stage research through a Tiangong research workspace with immutable inputs, capability-locked method skills, hard budgets, isolated producer execution, independent review, provenance, and mechanical closure. Use when a user asks to initialize, configure, execute, resume, inspect, or close a traceable research project, or asks how to configure its research environment and capabilities. Do not use for a single Tiangong edge-search request.
---

# Tiangong Auto Research

Use the pinned CLI for every workspace operation:

```bash
npx --yes @tiangong-ai/cli@0.0.20 research --help
```

Do not reproduce scheduling, evidence collection, review, or closure in the
coordinating agent. The CLI runtime owns those actions.

## Route the request

1. Resolve every workspace and input path to an absolute path.
2. Inspect before mutation:

   ```bash
   npx --yes @tiangong-ai/cli@0.0.20 research context inspect \
     --path /absolute/path/to/workspace --json
   ```

3. Follow the returned role and `allowedOperations` exactly:
   - `unmanaged`: initialize only when the user wants a new workspace.
   - `workspace`: continue with workspace commands.
   - `invalid`: stop and report the violations. Do not repair state by hand.

## Initialize a workspace

```bash
npx --yes @tiangong-ai/cli@0.0.20 research workspace init \
  /absolute/path/to/workspace --name "Research name" --json
```

Use the generated defaults unless the user requests different budgets or agent
routes. When editing `.tiangong-research/config.json`, preserve schema version
1, keep all budgets positive, and use different agent families for `producer`
and `reviewer`. A binary must be the exact agent name or an absolute path.

Never edit `workspace.json`, `runtime-lock.json`, `journal.jsonl`, project state,
run receipts, locks, or outputs directly.

## Admit method capabilities

Method skills remain external and are admitted through
`.tiangong-research/capabilities.json`. Each `skillPath` must be absolute and
contain a valid `SKILL.md` whose name matches its directory.

Use only current permissions: `project-read`, `candidate-write`,
`brokered-network`, and `controlled-command`. A `brokered-network` capability
must declare exact HTTPS hosts in `allowedHosts`. Credential host scopes must be
subsets of the capability host scope.

After any declaration change, freeze and verify both the skill tree and policy:

```bash
npx --yes @tiangong-ai/cli@0.0.20 research capability lock \
  --workspace /absolute/path/to/workspace --json
npx --yes @tiangong-ai/cli@0.0.20 research capability verify \
  --workspace /absolute/path/to/workspace --json
```

Read [references/env.md](references/env.md) before configuring credentials.

## Register projects and inputs

Use one stable lowercase project ID per research question:

```bash
npx --yes @tiangong-ai/cli@0.0.20 research project init gpu-resource-impact \
  --workspace /absolute/path/to/workspace \
  --question "How do advanced GPU process nodes change environmental resource burdens?" \
  --json
```

Admit existing local evidence through the CLI so its digest becomes immutable:

```bash
npx --yes @tiangong-ai/cli@0.0.20 research project input add gpu-resource-impact \
  --workspace /absolute/path/to/workspace \
  --path /absolute/path/to/inventory.csv \
  --role primary --json
```

Use `primary` for direct evidence, `reference` for contextual material, and
`replication` for reproducibility inputs. Do not copy unregistered files into
the control directory.

## Validate and run

Run the doctor before execution:

```bash
npx --yes @tiangong-ai/cli@0.0.20 research workspace doctor \
  --workspace /absolute/path/to/workspace --json
```

Proceed only when status is `ready`. Preview scheduling when useful, then run:

```bash
npx --yes @tiangong-ai/cli@0.0.20 research run \
  --workspace /absolute/path/to/workspace --dry-run --json
npx --yes @tiangong-ai/cli@0.0.20 research run \
  --workspace /absolute/path/to/workspace \
  --max-parallel 2 --max-cycles 20 --json
```

The runtime executes at most one ready package per project per cycle. Separate
projects may run concurrently. Do not bypass the runtime with direct agent or
web calls.

## Inspect and hand off

```bash
npx --yes @tiangong-ai/cli@0.0.20 research status \
  --workspace /absolute/path/to/workspace --json
```

If a project is blocked, report the failed package, bounded error, attempts,
and recorded usage. Do not reset package state or budgets without explicit user
direction. A complete project must have a passing independent review and
`outputs/closure.json`.

Return the project status, artifact paths, usage totals, review decision, and
material limitations. Treat `outputs/report.md` as the research deliverable and
`outputs/closure.json` as the completion receipt.
