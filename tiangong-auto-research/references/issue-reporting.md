# Report An Auto Research Problem Or Capability

Use this reference when the user asks to report a bug, provide feedback, or
prepare an issue about Auto Research. This is reporting, not a new research
request: it does not require setup, a rewritten scientific question, provider
checks, or a working research workspace. It implements
[reporting contract v1](https://github.com/tiangong-ai/workspace/blob/main/_docs/contracts/issue-reporting-policy.md).
The templates below are complete and usable offline after installing this Skill.

## Prepare The Report

1. Identify whether this is a bug or a requested capability. Search for an
   existing issue if GitHub access is available; if unavailable, state that the
   duplicate search was not performed and continue preparing a local draft.
2. Choose [Skills](https://github.com/tiangong-ai/skills/issues/new/choose) for
   instructions, orchestration, references, or agent routing. Choose
   [CLI](https://github.com/tiangong-ai/cli/issues/new/choose) for commands,
   setup/install, runtime errors, schemas, locks, or packages. Uncertain and
   cross-component reports enter CLI; `Unsure / 不确定` is a valid component.
   Maintain one report and let maintainers transfer it or link implementation tasks.
3. Use the current conversation and already available run evidence. Record
   actual CLI/Skill versions, native host/model, and environment when known.
   A managed runtime lock can differ from the global CLI. Read known regular
   version/lock files only as needed; label which run/source the version came
   from. Do not invoke setup, install/update, repair immutable state, rerun
   research, or spend provider/model budget to make a report more complete.
4. Fill the matching template with observed facts. Keep its English headings
   and order; Chinese or English body text is welcome. Use `Unknown` or
   `Not applicable` with a reason for missing required details. An intermittent
   or unreproducible failure is valid: record frequency and reproduction limits.
   Distinguish a suspected cause from an observed fact. A diagnosis or patch is
   not required. Ask only for missing information that materially helps triage.
5. Include minimal sanitized snippets. Remove credentials, authorization
   headers, private paths, private research inputs, and unrelated conversation.
   Never attach the entire research directory, secret/environment stores, or
   complete conversation. Review any selected audit export before sharing it.
6. Return a concise title, destination repository, and completed Markdown body.
   Prepare a local draft unless the user has authorized external submission.
   Existing explicit submission authorization remains valid: use an available
   authorized GitHub tool without asking again. If submission is unavailable,
   provide the draft and form link and accurately state that it was not posted.

## Bug Template

Use a short symptom as the issue title. For a CLI destination, optionally
append `### CLI diagnostics` with an already available invocation, exit code,
and sanitized diagnostics; do not invent them.

```md
### Summary
<One observable problem and its impact.>

### Goal
<What the user was trying to accomplish.>

### Component
<Auto Research Skill / CLI / Integration / Unsure / 不确定>

### Versions
<Actual CLI package/version and Skill revision/source from the affected run; Unknown with a reason when unavailable.>

### Environment
<OS, Node if known, installation method, native host and model if applicable.>

### Stage
<Setup, discovery, acquisition, analysis, review, closure, another command, or Unknown.>

### Reproduction
<Minimal steps or sanitized command/prompt; frequency and limits if not reproducible.>

### Expected result
<Expected behavior.>

### Actual result
<Observed behavior and exact sanitized error if available.>

### Evidence
<Optional minimal sanitized snippets, or None provided.>
```

## Feature Template

```md
### Summary
<The requested capability.>

### Component
<Auto Research Skill / CLI / Integration / Unsure / 不确定>

### Use case
<Who needs this and what task they are doing.>

### Current limitation
<What makes that task difficult today.>

### Proposed capability
<Desired user-facing behavior; no implementation design is required.>

### Success criteria
<An observable example of a successful outcome.>

### Alternatives
<Optional workarounds or alternatives, or None provided.>
```

Maintainers add development scope, TODOs, validation, and integration decisions
after triage. Reporting does not require the user to know the delivery process.
