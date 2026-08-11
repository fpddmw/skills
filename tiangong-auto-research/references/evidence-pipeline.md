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

## 2. Search broadly in bounded batches

The discover packet contains a mechanically derived `discovery.plan` and live
progress. Treat its broker-view ceiling as a maximum, never a quota:

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
to find an additional lead. Register only safe, non-secret metadata with the
packet's `registerCandidate` command. Such a lead remains
`supplemental-not-admitted` until the same URL/DOI has an immutable broker
occurrence. Never cite or admit a native-only candidate. Registered inputs are
already formal candidates under their own content-hash identity.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence candidate register PROJECT \
  --record /absolute/path/to/candidate.json \
  --workspace /absolute/path/to/workspace --json
```

Discovery output contains compact judgments keyed by `candidateId`. The model
does not generate locators, hashes, retrieval dates, URLs, or receipt identity;
the control plane joins those deterministic fields from the ledger. Omitted
candidates remain available for a later gap-fill pass. Record an explicit
rejection only when it is meaningful and supportable.

## 4. Audit acquisition before inference

After provisional admission, the native `acquire` stage assesses every source
exactly once. Use selected external acquisition/document Skills or an explicitly
authorized browser, then register each exact output file from its absolute
staging path. Never scan a directory for the newest file and never infer success
from file existence.

```bash
node "$AUTO_RESEARCH_CLI" --workspace /absolute/path/to/workspace -- \
  research project evidence artifact register PROJECT \
  --candidate CANDIDATE_ID --path /absolute/path/to/exact-file.pdf \
  --source-url https://publisher.example/article \
  --workspace /absolute/path/to/workspace --json
```

The registry verifies size, SHA-256, exact-file binding, and structure. PDF
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
  HTML, CSV, or Markdown derivative is embedded within the context limit.
- `producerContextLevel=metadata-only`: no producer-readable full text was
  admitted. A raw PDF/DOCX/PPTX/XLSX alone remains here.
- `reviewerBoundFullFile`: the independent review packet binds the exact full
  file; this does not mean the reviewer model read all binary bytes.
- `visuallyVerified`: false unless a future explicit visual-verification event
  records otherwise.

An accepted source may remain metadata/abstract-only when the requirements
allow it, but the audit must state that limitation. Unresolved blocking gaps
stop snapshot creation.

## 5. Freeze, then infer

Successful acquisition creates `outputs/evidence-snapshot.json` plus an
immutable project-local copy under `evidence/snapshots/`. The semantic snapshot
hash binds the question, evidence and acquisition records, ledger head,
receipts, selected artifacts, coverage, limitations, and parent/delta lineage.
Analysis and synthesis may use only this verified snapshot. They cannot fetch,
register, or silently substitute new evidence.

The review packet binds the current snapshot chain, selected exact artifacts,
permanent broker objects, bounded excerpts, analysis, and report. Claim and
review bindings are appended to the ledger. Mechanical closure re-verifies all
hashes and refuses a missing, changed, or stale snapshot, packet, context,
receipt, artifact, or source.

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
use `research status --all` only for lineage audit.

Use the status response's `discovery`, `snapshot`, `nativeStage`, `lineage`, and
`recommendedAction` fields instead of inspecting control files manually.
