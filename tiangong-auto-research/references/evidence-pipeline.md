# Evidence-first research pipeline

Load this reference before discovery, acquisition, evidence refresh, or an
addendum. The operating sequence is:

```text
broad search -> strict admission -> gap filling -> evidence freeze -> inference
```

The sequence is a correctness boundary, not merely a preferred writing style.
Do not analyze while evidence is still mutable, and do not use analysis to
decide what the evidence ledger claims was retrieved.

## 1. Define the coverage contract

Before spending provider or model budget, describe the question as reviewed
evidence requirements: dimensions, source types, required capability IDs,
required discovery scopes, minimum source/full-text/dated counts, and date
boundaries. `research project preflight` is authoritative for capability,
context, reservation, and expected-cost gaps.

Production requires an independent public-internet capability. Add every
owner-whitelisted database whose contents matter to the question as an exact
required capability; a general web result cannot silently substitute for it.
For top-journal work, the frozen scientific design must also enumerate every
lawful, relevant route available in the configured environment: broker
capabilities, native Web/Browser channels, OA/download adapters, explicitly
authorized browsers, licensed or owner-provided material, external requests,
and field collection. Omitting an applicable route is a design/review defect,
not permission to stop early.

## 2. Search broadly in bounded batches

The discover packet contains a mechanically derived `discovery.plan` and live
progress. The working call budget scales with reviewed dimensions, source
types, required channels, minimum sources, and a separate gap-fill reserve. It
is no longer a fixed six-call allowance, but it never exceeds the workspace
hard ceiling of 256 in a new production workspace. Treat that ceiling as a
runaway guard, never a quota:

1. Exercise required first-pass capabilities.
2. Use broad, high-yield queries across distinct selected channels.
3. Inspect registered candidate IDs and coverage after each batch.
4. Stop early when the reviewed minimums are supportable.
5. Spend the remaining calls only on explicit gaps, counterevidence, missing
   dates, missing source types, applicability limits, or full-text candidates.

Exact repeated requests reuse the project evidence cache without another
provider network call. Every returned bounded view still consumes one reviewed
broker-view slot and its context reservation, preventing cache/pagination from
bypassing package limits. Use pagination or bounded JSON Pointers instead of
placing a large response wholesale into model context. The permanent raw object
remains content-addressed even when only a bounded view reaches the producer.

Every formal network occurrence must pass through the packet's broker command.
The broker owns endpoint policy, credentials, retries, response bounds,
sanitization, immutable bytes, and receipts. A standalone search result can
help find a lead but cannot replace a required broker occurrence.

## 3. Keep one immutable evidence ledger

Inputs, broker results, and native-app discoveries are normalized into stable
candidate IDs and deduplicated by canonical public URL, DOI, or input hash.
The append-only hash-chained ledger records discovery occurrences, admission or
rejection judgments, artifact registration and assessment, snapshots, claim
use, reviewer binding, and supersession.

The current native Codex or Claude app may use its own Web/Browser experience
to find additional leads. Record every material search, navigation, download,
or file-inspection occurrence through the packet's `recordActivity` command.
For a top-journal project, pass the exact frozen route as
`acquisitionRouteId`; the control plane rejects unbound or mismatched activity.
The control plane persists only a sanitized input hash, channel, counts, status,
challenge class, and candidate IDs. Then register safe, non-secret candidate
metadata through `registerCandidate`. Such a lead remains
`supplemental-not-admitted` until the same URL/DOI has an immutable broker
occurrence. Never cite or admit a native-only candidate. Registered inputs are
already formal candidates under their own content-hash identity. The frozen
snapshot includes the verified activity summary so native work and formal
broker evidence remain one auditable ledger rather than two hidden work logs.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence candidate register PROJECT \
  --record /absolute/path/to/candidate.json \
  --workspace /absolute/path/to/workspace --json
```

Assess candidates as they are found in batches of at most 25 through the
packet's `recordAssessment` command. Each append-only batch may replace the
latest judgment for a candidate without repeating deterministic source
metadata. The model does not generate locators, hashes, retrieval dates, URLs,
or receipt identity; the control plane joins those fields from the ledger. The
final discover submission is therefore only a compact closeout containing one
status per reviewed dimension, limitations, and remaining gaps. Omitted
candidates remain available for a later gap-fill pass. Record an explicit
rejection only when it is meaningful and supportable.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence assessment record PROJECT \
  --record /absolute/path/to/assessment-batch.json \
  --workspace /absolute/path/to/workspace --json
```

## 4. Audit acquisition before inference

After provisional admission, the native `acquire` stage assesses every source
exactly once. Use selected external acquisition/document Skills or an explicitly
authorized browser. For every network file, capture the exact browser Download
object or equivalent transport completion, save it to a unique planned staging
path, and first bind that event through `bindDownload`. A failed or cancelled
event creates no successful binding and cannot register an artifact. Never scan
a directory for the newest file and never infer success from file existence.
For a top-journal project, the binding record must carry the exact
`acquisitionRouteId`. Broker fetches use the corresponding snake-case
`acquisition_route_id` argument.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence download bind PROJECT \
  --candidate CANDIDATE_ID --record /absolute/path/to/download.json \
  --workspace /absolute/path/to/workspace --json
```

Register the exact downloaded file with the returned binding ID. A text or
structured derivative must instead name the same-candidate parent artifact;
this preserves a mechanical chain back to the bound network file.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence artifact register PROJECT \
  --candidate CANDIDATE_ID --path /absolute/path/to/exact-file.pdf \
  --download-binding DOWNLOAD_BINDING_ID \
  --source-url https://publisher.example/article \
  --workspace /absolute/path/to/workspace --json
```

When the project owner supplies the exact file for a network source that is
already admitted, preserve that source identity. Do not fabricate a browser
download event and do not admit the file again as a duplicate publication.
First register the file through `research project input add`, then register its
artifact against the existing candidate without `--source-url`. Append the
exact owner-input proof with the IDs returned by those commands:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence artifact adopt-input PROJECT \
  --candidate CANDIDATE_ID --artifact ARTIFACT_ID \
  --input PROJECT_INPUT_ID \
  --workspace /absolute/path/to/workspace --json
```

The command accepts only an active acquisition stage. It verifies that the
registered input and immutable artifact have identical size and SHA-256, then
records the relationship in the append-only evidence ledger. Repeating the
same command is safe; a different file or conflicting prior adoption stops.
Refresh the active acquisition packet after all new input/adoption records are
complete. A provenance-only adopted input does not need separate candidate
admission and does not create a second source in the frozen acquisition audit.

The registry verifies size, SHA-256, exact event/file binding, and structure. PDF
registration requires a parseable non-empty document with an EOF marker. ZIP
and Office Open XML registration verifies the central directory, safe paths,
supported compression, declared uncompressed sizes, and CRC; encrypted, ZIP64,
or unsupported compression is rejected. XLSX also records declared sheet
names. Optional license, license URL, host type, and article version are stored
only when the source explicitly declares them.

Keep these distinctions exact:

- `registeredFullFile`: an exact full file is hash-bound for audit/review.
- `producerContextLevel=full-input`: the registered input itself is available
  through its reviewed input contract.
- `producerContextLevel=bounded-text-artifact`: a registered UTF-8 text, JSON,
  CSV, or Markdown derivative is embedded within the context limit. HTML stays
  metadata-only because an error or challenge page must not masquerade as
  acquired full text.
- `producerContextLevel=metadata-only`: no producer-readable full text was
  admitted. A raw PDF/DOCX/PPTX/XLSX alone remains here.
- `reviewerBoundFullFile`: the independent review packet binds the exact full
  file; this does not mean the reviewer model read all binary bytes.
- `visuallyVerified`: false unless a future explicit visual-verification event
  records otherwise.

An accepted source may remain metadata/abstract-only when the requirements
allow it, but the audit must state that limitation. Unresolved blocking gaps
do not erase successfully acquired evidence: acquisition freezes the complete
source/artifact/gap audit and marks its separate `inferenceGate=stop`.
Continue only far enough to decompose everything already acquired and freeze
the typed-content record; then request the exact access/scope handoff. Never
report the stopped snapshot as inference-ready.

## 5. Freeze, then infer

Successful acquisition creates `outputs/evidence-snapshot.json` plus an
immutable project-local copy under `evidence/snapshots/`. The semantic snapshot
hash binds the question, evidence and acquisition records, ledger head,
receipts, selected artifacts, coverage, explicit gaps, the inference decision,
and parent/delta lineage. File existence is not readiness; inspect the semantic
hash and gate through `research status --json`.

Before any evidence-construct assessment or analysis, disposition every
acquired full-text/data artifact. Use the installed document/PDF/spreadsheet
tools to create legitimate producer-readable derivatives, register each exact
derived artifact with its parent, and record one decomposition object per
source artifact:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence decomposition record PROJECT \
  --record /absolute/path/to/decomposition.json \
  --workspace /absolute/path/to/workspace --json
```

A complete decomposition names the parser/version, exact parent and output
artifact IDs, content classes, and limitations. A limited or failed
decomposition is an explicit disposition, not permission to omit the file.
Never claim that a PDF, workbook, archive, or HTML challenge page was read
merely because it was downloaded.

Register claim-usable evidence as exact atoms from a producer-readable
UTF-8/JSON/CSV/Markdown artifact. Each atom binds the admitted source and
candidate, exact artifact hash, a one-based line range or JSON Pointer, the
control-plane-extracted excerpt and excerpt hash, evidence role/dimension,
support/counterevidence/method/limitation function, scope, and limitations:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence atom register PROJECT \
  --record /absolute/path/to/evidence-atom.json \
  --workspace /absolute/path/to/workspace --json
```

When a formally appended local source has `publicationDate=null` even though
its exact readable artifact states the date, do not hand-edit an acquisition
snapshot or infer a year from a filename. Add optional `sourcePublicationDate`
to an atom whose exact excerpt contains the asserted year. Use a separate date
atom when the strongest scientific excerpt is elsewhere in the source. Content
freeze resolves compatible precision (for example `2022` and `2022-12-08`),
stops on conflicting dates, and binds the resolved value into evidence-role,
scientific-review, and inference contexts.

Do not paste an invented excerpt or cite only a source-level ID when an exact
atom is required. Freeze the typed universe only after all acquired artifacts
have dispositions and all material claims have atoms:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence content freeze PROJECT \
  --workspace /absolute/path/to/workspace --json
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research status --project PROJECT \
  --workspace /absolute/path/to/workspace --json
```

The `evidencePipeline` status reports acquisition gaps/gate, decomposition and
atom counts, typed role gaps, inference identity, and Claim-Evidence Graph
identity. A stopped acquisition or content gate prohibits inference. It does
not justify further low-yield substitute search after lawful routes are
exhausted.

For top-journal work, the real-record evidence-construct canary and its exact
content-addressed JSON artifacts are reviewed against both frozen acquisition
and typed-content snapshots before the outcome-blind methods pilot. Only
snapshot source IDs and exact atoms count; full-text and date states are
re-derived mechanically. After those gates pass, preparing `analyze` freezes an
immutable `inference-snapshot.json` containing the exact sources, atoms, design
claims/edges, policy/review bindings, input artifacts, implementations, and
environment locks. Analyze schema v2 must bind that snapshot, one reproduced
analysis run, and for every finding the admitted source IDs, exact atom IDs,
design claim IDs, uncertainty, and applicability. Successful submit generates
`claim-evidence-graph.json` mechanically; do not hand-author it. Synthesis and
review may use only this verified chain and cannot fetch, register, or silently
substitute new evidence.

The review packet binds the current acquisition/content/inference/graph chain,
selected exact artifacts, permanent broker objects, bounded excerpts, analysis,
and report. Claim and review bindings are appended to the ledger. Mechanical
closure re-verifies all hashes and refuses a missing, changed, or stale
snapshot, graph, packet, context, receipt, artifact, or source.

## 6. Refresh through an addendum

Never mutate a closed project. When material new evidence exists, create a new
addendum project:

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project addendum CLOSED_PROJECT --to NEW_PROJECT \
  --workspace /absolute/path/to/workspace --json
```

The original closure remains byte-for-byte unchanged. The addendum inherits
the verified base ledger/evidence/audit/artifacts, starts again at discover,
freezes a child snapshot with a mechanical added/changed/removed/unchanged
delta, and reruns analysis, synthesis, independent review, and closure. The
superseded project becomes stale and is hidden from default `research status`;
use `research status --all` only for lineage audit. Recovery forks have the
same single-authority rule: the new fork supersedes its source immediately.
Use `research project archive` for complete/stale history and
`research project abandon` for unfinished history; never infer the latest
project from its name or version suffix.

Use the status response's `discovery`, `snapshot`, `evidencePipeline`,
`nativeStage`, `lineage`, and `recommendedAction` fields instead of inspecting
control files manually.
