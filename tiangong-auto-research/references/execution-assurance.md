# Task assurance and efficient recovery

Load for original-task intake, pre-analysis evidence correction, a scope change,
or a completion claim. Keep the current native host as the producer. The CLI
owns schemas, authoritative state, evidence bindings and recovery; this reference
does not define a second workflow engine.

## Check the locked runtime once

Inspect help and the needed schemas when entering a workspace or after an
explicit runtime update, not before every record:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research --help
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research schema show task-contract --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research schema show task-acceptance --json
```

Use the new recipes only when that exact locked runtime exposes them. Missing
commands require an explicit reviewed upgrade or its existing supported
workflow; never switch to `latest`, rewrite a runtime lock, or invent a command.
An existing project without a task contract remains **unassessed** in this
dimension. Do not manufacture historical requirements, approvals or passing
checks. Existing Policy/evidence safeguards still apply.

## Record a small original-task checklist

After initializing a new project and before its first scientific review or
producer stage, preserve the user's scientific requirements and their acceptance
conditions. Use stable IDs at the level of actual user requirements, not one
requirement per file or tool call. Bind existing design-claim and coverage IDs
where applicable. Do not invent additional scientific thresholds or require
computation for a qualitative review or a theoretical proof.

Use the CLI-owned schema, then register the declaration:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project task define PROJECT --input /absolute/path/task-contract.json --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project task status PROJECT --workspace /absolute/path/to/workspace --json
```

The returned contract and requirement hashes are authoritative. Keep the original
request distinct from the currently approved scope. Cosmetic manuscript edits
are not a reason to rewrite this checklist. Markdown is a human-readable view,
not a second state file.

## Correct acquisition without unnecessary new projects

When acquire is complete, analysis has not started, and the question, Policy,
design and evidence requirements are unchanged, prefer the same-project
revision. Resolve an active session or human handoff through its existing
authorized command first. Inspect the current snapshot and use its exact hash:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project evidence acquisition revise PROJECT --expected-snapshot CURRENT_SNAPSHOT_SHA256 --reason "Add the missing readable derivative before analysis" --workspace /absolute/path/to/workspace --json
```

This preserves the project, original requirements, approved Policy/design and
research-design review. It reopens acquire, retains old snapshots and check
records, and invalidates evidence-construct/pilot-methods approvals that depend
on the acquisition. Unchanged files and parsing results are reusable. Do not
repeat paid search, download or model work merely because an acknowledgement
was lost; repeat the exact request to inspect its idempotent result.

If a genuinely new source must enter the unchanged study, explicitly include
discovery in the revision; do not add a source directly to a frozen audit:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project evidence acquisition revise PROJECT --expected-snapshot CURRENT_SNAPSHOT_SHA256 --reason "Admit an additional lawful source within the approved study" --include-discovery --workspace /absolute/path/to/workspace --json
```

This reopens discover then acquire. Preserve existing candidate/source IDs and
receipts, search only the remaining actionable gaps, and formally admit new
sources through the existing discovery commands. It does not reset the project
budget. A revision without this option is for files/derivatives of already
admitted sources and does not silently reopen search.

Then prepare the indicated stage, register exact files/derivatives, run one
forecast for the meaningful batch, submit the complete audit, update relevant
decompositions/atoms in batches and freeze typed content. A failed decomposition
may be superseded in the new acquisition snapshot; its historical record remains.
Atoms from deselected files must not count in current coverage. Complete the
applicable scientific gates before inference. Follow
[evidence-pipeline.md](evidence-pipeline.md) for exact evidence operations.

The preflight distinguishes `submissionGate` blockers from optimistic coverage.
Potential eligibility is not proof that files, evidence roles or scientific
claims have passed. A limited/stopped audit may be retained honestly; never move
a blocking gap into limitations to advance. Hash and structure checks still run
at admission and trust boundaries; do not add a full-corpus check after each atom.

Use the existing fork/new-generation route when analysis has started or when the
question, Policy, design or evidence contract changes. A pre-feature snapshot
without immutable acquisition records also uses that route; there is no automatic
in-place migration. See [scientific-design.md](scientific-design.md).

## Obtain authorization for a real scope change

Explain what will no longer be answered and inspect the exact before/after
requirements. A request to continue, increase a budget or create a fork is not
permission to change scientific scope. Preserve valid prior exact authorization;
do not ask again merely because a read-only acknowledgement is repeated.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research schema show task-scope-change --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project task scope propose PROJECT --input /absolute/path/scope-change.json --expected-contract CURRENT_CONTRACT_SHA256 --workspace /absolute/path/to/workspace --json
```

Show `changes.details` and the proposal hash to the user. Only after explicit
approval of that actual change:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project task scope approve PROJECT --proposal PROPOSAL_SHA256 --confirm-change PROPOSAL_SHA256 --workspace /absolute/path/to/workspace --json
```

The command records exact operator confirmation, not authenticated human identity;
the native host must honor the user's permission. Never put `approved: true` in
a producer declaration. Task-scope approval does not change the question,
Policy, design or evidence requirements. Changes to those contracts need their
formal new-generation process. A task-scope change invalidates scientific
reviews that no longer cover it; inspect status before paying for any re-review.
Withdrawn original requirements remain visible rather than becoming answered.

## Record checks actually performed by the native host

Perform the appropriate evidence examination, computation or proof work in the
current host. At an idle stage boundary after acquisition, record its actual
outcome using the schema above. Bind current source/atom/finding IDs and any
explicit external portable UTF-8 result files. The CLI supplies hashes and
immutable result copies; never scan a directory for the newest output or pass
control-store files as new native results.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- research project task acceptance record PROJECT --input /absolute/path/task-acceptance.json --workspace /absolute/path/to/workspace --json
```

Use `not-run`, `failed` or `inconclusive` honestly. A failed computation need not
invent an output file. A supported null effect or counterexample may be a valid
`negative-result`; unavailable data is not one. A changed record must name the
exact prior record hash; identical replay does not repeat the check.

These records have `trust=native-observation` and `executionCertified=false`.
Byte identity does not prove successful execution. The declared command is kept
by hash, not as a credential-bearing command line. Keep result files portable
and secret-free. Unchanged exact dependencies can be reused; changed requirement,
input, design or analysis bindings need revalidation. Do not invoke a generic
CLI producer or add another model solely to judge this bookkeeping.

## Review once at the existing gates and report separately

Before the existing independent review, give every current requirement an honest
check/disposition. The packet contains the original request, original/current
requirement versions and their bound checks. Its supplied response schema is
authoritative. Do not drop missing requirements, failed checks or counterevidence
to fit context; report capacity limits or make a reviewed context adjustment.
Shared results need not be pasted once per requirement.

The existing review assesses the exact `taskAcceptance` context through
`taskAssessment`; no additional fixed paid review round is needed. A producer's
claim is only `recorded` until reviewed. A reviewer cannot promote absent, stale,
inconclusive, failed or unexecuted checks into answered requirements. Final
publication reviewers also receive the task context. Keep the original scientific
and publication gates in [publication-policy.md](publication-policy.md).

Report workflow completion, publication verdict, original-scope completion and
current-scope completion separately. `research run` or base closure may be
complete while `task.currentScope` or `task.originalScope` is incomplete. Name
the remaining requirements and the legitimate next action; do not promise full
task completion or editorial acceptance. Export and verify the portable audit
with its task relationships before handoff. Audit integrity is not proof of
authorship, authenticated human approval or real-world scientific truth.
