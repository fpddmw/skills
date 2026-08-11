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

Native Web or Browser results may be recorded only as supplemental leads with
the packet's `registerCandidate` argv. They remain inadmissible until a broker
receipt formalizes the same canonical URL/DOI. Registered inputs are already
formal candidates under their own content-hash identity.

## Register exact acquisition artifacts

When the next stage is `acquire`, follow the packet's `registerArtifact` argv
for each exact selected file. Pass its candidate ID and absolute staging path;
add source/license metadata only when explicitly declared by the source. The
registry accepts no directory and performs no “latest download” selection.
Binary registration makes the exact file review-bound, but PDF/DOCX/PPTX/XLSX
alone does not claim producer-readable full text. Register a separately derived
UTF-8 text/JSON/HTML/CSV/Markdown artifact when one was legitimately produced
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

## Continue, review, or abort

After each successful submit, call `research run` again. Prepare/submit the
next native producer stage through discover, acquire, analyze, and synthesize.
The same run command may then launch only the configured independent reviewer
CLI and, after a passing review, perform mechanical closure.

To discard an active native session explicitly:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project stage abort PROJECT --session SESSION_ID \
  --workspace /absolute/path/to/workspace --json
```

Abort removes only that CLI-created runtime capsule and active session. It
consumes the prepared attempt and never deletes admitted evidence or outputs.
