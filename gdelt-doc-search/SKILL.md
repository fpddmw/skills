---
name: gdelt-doc-search
description: Search GDELT DOC 2.0 through the Tiangong CLI for bounded recent article metadata or timeline aggregates using GDELT query syntax. Use for news-source discovery and trend reconnaissance; do not use for article bodies, raw GDELT event/GKG/mention rows, representative media measurement, fact verification, sentiment ground truth, or causal inference.
---

# GDELT DOC Search

Use the CLI-owned `gdelt.doc-search` capability. This Skill supplies intent
routing and result-use boundaries only; the CLI owns source discovery,
input/output schemas, HTTP behavior, limits, validation, and receipts.

## Before running

1. Read `references/tiangong-data-binding.json`.
2. Use its exact `generatedWithCliVersion` in the package spec below. Never use
   `latest`, a tag, or a version range.
3. Run `data describe` and compare the returned capability version, execution
   manifest digest, operation version, and input/output schema digests with the
   binding. Stop on any mismatch.

```bash
npx --yes --package "@tiangong-ai/cli@<generatedWithCliVersion>" -- \
  tiangong-ai data describe gdelt.doc-search --json
```

Use the returned Discovery Metadata to confirm current source coverage,
freshness, restrictions, `provides`, and `doesNotProvide`. Do not substitute
facts remembered from an older Skill revision.

## Prepare the request

Build a `tiangong.data.run-request.v1` envelope. Replace the two version
placeholders with the exact values in the binding. This example requests an
article list for one bounded absolute UTC window:

```json
{
  "schemaVersion": "tiangong.data.run-request.v1",
  "capabilityId": "gdelt.doc-search",
  "capabilityVersion": "<binding.capabilityVersion>",
  "operationId": "search",
  "operationVersion": "<binding.operations[0].operationVersion>",
  "input": {
    "query": "(\"climate change\" OR pollution) sourcecountry:us",
    "mode": "artlist",
    "absoluteWindow": {
      "from": "2026-03-01T00:00:00Z",
      "to": "2026-03-07T23:59:59Z"
    },
    "maxRecords": 75,
    "sort": "hybridrel"
  }
}
```

Use the operation input schema returned by `data describe` to select a
supported mode and its mode-specific fields. Choose exactly one supported time
window form. Do not silently widen the requested window or pass arbitrary DOC
parameters that are absent from the schema.

## Run

```bash
npx --yes --package "@tiangong-ai/cli@<generatedWithCliVersion>" -- \
  tiangong-ai data run gdelt.doc-search search \
  --input /absolute/path/to/request.json --json
```

The command emits a `tiangong.data.run-result.v1` envelope. Preserve its
`contract`, `warnings`, `errors`, and `receipt` with `data` when handing the
result to another workflow.

## Result boundaries

- Treat article-list results as source metadata and links, not downloaded
  article bodies, verified claims, or independent evidence units.
- Treat timelines, tone, language, and source-country outputs as automated
  aggregates whose meaning depends on the selected mode; do not compare unlike
  measures or infer causality from them.
- GDELT source coverage and automated extraction can be uneven. Do not treat
  counts, tone, or rankings as representative measurements of public opinion,
  media prevalence, event truth, or sentiment ground truth.
- Surface `partial`, truncation warnings, and empty results. Never reinterpret
  them as complete absence of coverage.
- Use a dedicated GDELT Events, GKG, or Mentions Skill when structured feed rows
  are required; this Skill must not invoke or combine them automatically.
- Cross-source comparison, full-text acquisition, persistence, polling, and
  research evidence admission belong to the caller or Auto Research.

## Reference

- `references/tiangong-data-binding.json`: exact execution compatibility
  binding for the reviewed CLI release.
