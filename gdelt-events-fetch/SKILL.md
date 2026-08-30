---
name: gdelt-events-fetch
description: Retrieve bounded GDELT 2.0 Events rows through the Tiangong CLI from the latest snapshot or an exact UTC range. Use for machine-coded event records and source-document linkage; do not use for article bodies, GKG themes, mention-level provenance, event verification, polling, bulk archival mirroring, or causal claims.
---

# GDELT Events Fetch

Use the CLI-owned `gdelt.events` capability. This Skill supplies intent routing
and result-use boundaries only; the CLI owns source discovery, input/output
schemas, HTTP and archive handling, limits, validation, and receipts.

## Before running

1. Read `references/tiangong-data-binding.json`.
2. Use its exact `generatedWithCliVersion` in the package spec below. Never use
   `latest`, a tag, or a version range.
3. Run `data describe` and compare the returned capability version, execution
   manifest digest, operation version, and input/output schema digests with the
   binding. Stop on any mismatch.

```bash
npx --yes --package "@tiangong-ai/cli@<generatedWithCliVersion>" -- \
  tiangong-ai data describe gdelt.events --json
```

Use the returned Discovery Metadata to confirm current source coverage,
freshness, restrictions, `provides`, and `doesNotProvide`. Do not substitute
facts remembered from an older Skill revision.

## Prepare the request

Build a `tiangong.data.run-request.v1` envelope. Replace the two version
placeholders with the exact values in the binding. This example selects a
bounded range of source snapshots:

```json
{
  "schemaVersion": "tiangong.data.run-request.v1",
  "capabilityId": "gdelt.events",
  "capabilityVersion": "<binding.capabilityVersion>",
  "operationId": "fetch",
  "operationVersion": "<binding.operations[0].operationVersion>",
  "input": {
    "mode": "range",
    "startDateTime": "2026-03-01T12:00:00Z",
    "endDateTime": "2026-03-01T12:45:00Z",
    "maxFiles": 4
  }
}
```

Use the operation input schema returned by `data describe` when choosing
`latest` or `range`. Do not round timestamps, widen a range, or increase a
safety limit without the caller's approval.

## Run

```bash
npx --yes --package "@tiangong-ai/cli@<generatedWithCliVersion>" -- \
  tiangong-ai data run gdelt.events fetch \
  --input /absolute/path/to/request.json --json
```

The command emits a `tiangong.data.run-result.v1` envelope. Preserve its
`contract`, `warnings`, `errors`, and `receipt` with `data` when handing the
result to another workflow.

## Result boundaries

- Treat rows as machine-coded event records, not verified real-world events,
  unique incidents, legal findings, or causal evidence.
- A source URL is lineage metadata, not an article body or proof that every
  coded field is correct. Do not claim full-text acquisition.
- Preserve GDELT identifiers and source timestamps when deduplicating or joining
  downstream; do not collapse rows merely because labels look similar.
- The capability returns normalized in-memory rows and execution metadata. It
  does not create a durable ZIP mirror, expose the master file list, or perform
  polling and incremental state management.
- Surface `partial`, truncation warnings, archive-validation failures, and empty
  results. Never reinterpret them as complete absence of events.
- Use the dedicated GKG or Mentions Skill for their row types; this Skill must
  not invoke or combine other feeds automatically.
- Cross-source comparison, persistence, scheduling, and research evidence
  admission belong to the caller or Auto Research.

## Reference

- `references/tiangong-data-binding.json`: exact execution compatibility
  binding for the reviewed CLI release.
