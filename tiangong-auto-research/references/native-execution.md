# Native producer execution

Discover, acquire, analyze, and synthesize are performed by the current
interactive Codex app/session or Claude Code session. The CLI is not a
producer-agent launcher. It prepares a hash-bound packet, brokers authorized
evidence, registers exact acquired artifacts, admits the result, launches the
other agent family only for independent review, and closes mechanically.

## Identify the next action

Run the control plane after project initialization:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research run --workspace /absolute/path/to/workspace \
  --project PROJECT --max-cycles 20 --progress-jsonl --json
```

`stopReason=native-stage-required` means the current host must perform the next
producer stage. It is not an error and must not trigger a nested `codex exec` or
`claude -p` call.

For a top-journal project, `research status` can instead return a pending
`research-design`, `evidence-construct`, or `pilot-methods` gate. Complete that
gate before preparing a native stage. The current native host writes the
schema-bound assessment; only the configured other agent family performs the
fresh independent review. See [scientific-design.md](scientific-design.md).

The ordering is deliberate:

```text
design review → discover → real-record construct review → acquire
→ outcome-blind methods pilot review → analyze → synthesize
```

Do not replace the real-record canary with a synthetic schema example, inspect
outcome values while proving construction, or use repeated cells/rows as
independent resampling units. A later inherited package cannot bypass an earlier
gate.

Read `mechanicalAssessment.futureGateObligations` before continuing. Pending
source-derived parameter values, executable model bytes, and exact environment
locks are permitted only until their declared gate and only when an exact
planned Policy rule owns them. They are not usable results. Freeze replacements
through a new authoritative generation before the deadline; at the due gate the
CLI must stop on the corresponding mechanical error.

## Prepare the exact stage

Use the host agent selected by the immutable setup plan:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project stage prepare PROJECT \
  --stage discover --host-agent codex \
  --workspace /absolute/path/to/workspace --json
```

The returned packet binds the project, stage, inputs, prior outputs, runtime and
capability locks, schema, model, limits, prompt, and command argument arrays.
Use the packet verbatim. Do not edit `project.json`, the active session, locks,
journal, evidence store, or admitted outputs.

Preparation is idempotent while its exact session remains active. If the wrong
host, stage, model, project state, or hash is observed, stop on the structured
error.

## Fetch discovery evidence

For each broker request, write one new bounded JSON object matching
`commands.fetchEvidence.requestSchema`. It contains logical capability and
credential IDs, never credential values. Invoke the returned argv, replacing
only the request-file placeholder:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence fetch PROJECT \
  --request /absolute/path/to/request.json \
  --workspace /absolute/path/to/workspace --json
```

Use only the returned bounded context and exact receipt fields. The broker
enforces the locked endpoint/method/Accept policy, injects owner credentials,
limits calls/bytes/items/tokens, persists content-addressed raw evidence, and
records sanitized events. Standalone web/search/database tools cannot replace
required broker receipts.

Native Web or Browser work must first use the packet's `recordActivity` argv.
Its search/navigation input is sanitized and persisted only by SHA-256; counts,
challenge class, status, and linked candidate IDs remain auditable. Register
useful results as supplemental leads with `registerCandidate`. They remain
inadmissible until a broker receipt formalizes the same canonical URL/DOI.
Registered inputs are already formal candidates under their own content-hash
identity.

After each useful discovery batch, write at most 25 candidate judgments through
the packet's `recordAssessment` argv. The ledger keeps the latest judgment per
candidate and the CLI joins all deterministic provenance fields. The final
discover output must contain only the compact closeout schema returned by the
packet; do not repeat a source-sized evidence array.

## Register exact acquisition artifacts

When the next stage is `acquire`, use the packet's `bindDownload` argv before
registering every file obtained from a network source. Bind the exact completed
Download object or equivalent to one unique planned staging path, safe final
URL, suggested filename, and available non-secret identifier. A cancelled or
failed download must be recorded as such and cannot be promoted. Then follow
`registerArtifact`, passing the candidate, exact absolute path, and returned
download binding. The registry accepts no directory and performs no “latest
download” selection.

For a producer-readable derivative, register the exact output with
`--derived-from-artifact` naming its same-candidate parent instead of claiming a
second network download. Add source/license metadata only when explicitly
declared by the source.

Binary registration makes the exact file review-bound, but PDF/DOCX/PPTX/XLSX
alone does not claim producer-readable full text. Register a separately derived
UTF-8 text/JSON/CSV/Markdown artifact when one was legitimately produced
and should be embedded within the bounded producer context.

## Submit producer output

Save only the schema-conforming JSON object to a new regular non-symlink file.
Then use the packet's exact session and expected model:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project stage submit PROJECT \
  --session SESSION_ID \
  --output /absolute/path/to/stage-output.json \
  --confirm-model EXPECTED_MODEL \
  --workspace /absolute/path/to/workspace --json
```

File existence is not success. Submit rechecks the session and all bindings,
parses the authoritative schema, validates provenance and coverage, enforces
byte/token/wall/cost reservations, computes hashes, writes a run record, and
atomically promotes the output. Native-host usage is conservatively charged at
the reviewed package reservation because the host app does not expose trusted
per-stage usage telemetry to the CLI.

A rejected submission leaves the active session intact so the current host can
perform a bounded formatting correction or gather missing authorized evidence.
It does not silently retry research or invoke another model.

Login, MFA, CAPTCHA, Turnstile, paywall, security-warning, or authorization
activity cannot be submitted as ordinary completion. Record it as `blocked`
through `recordActivity`, create a `user-action-required` record with the
packet's `requestHandoff` argv, and stop. When the missing material requires an
institution or another third party to respond, request
`external-response-required` instead of searching indefinite substitutes. Both
states are durable and do not burn the prepared attempt. Resume only after an
operator explicitly runs `research project handoff resolve` with a non-secret
resolution note.

## Continue, review, or abort

After each successful submit, call `research run` again. Prepare/submit the
next native producer stage through discover, acquire, analyze, and synthesize.
The same run command may then launch only the configured independent reviewer
CLI and, after a passing review, perform mechanical closure.

For a top-journal project, this is the base research closure, not the final
publication verdict. Continue in the same current native host to author and
freeze the manuscript, then use four fresh independent publication-review
sessions. Follow [publication-policy.md](publication-policy.md). Do not ask
`research run` to launch a producer for manuscript authoring.

To discard an active native session explicitly:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project stage abort PROJECT --session SESSION_ID \
  --workspace /absolute/path/to/workspace --json
```

Abort removes only that CLI-created runtime capsule and active session. It
consumes the prepared attempt and never deletes admitted evidence or outputs.

Use `research status` to follow the authoritative project. A recovery fork
supersedes its source and is the only default-runnable descendant. Use
`research status --all` for lineage audit, `research project archive` for
complete/stale history, and `research project abandon` for unfinished history.

Before an external handoff or archival milestone, export and verify the project
audit bundle described in [scientific-design.md](scientific-design.md). A local
manifest or receipt hash without the referenced evidence bytes is not a
portable audit package.
