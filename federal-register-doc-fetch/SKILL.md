---
name: federal-register-doc-fetch
description: Search official FederalRegister.gov document metadata through the Tiangong CLI for a bounded publication date and explicit regulatory filters. Use to identify notices, proposed rules, final rules, or presidential documents; do not use for document bodies, docket comments, legal interpretation, current-force determinations, or compliance advice.
---

# Federal Register Document Metadata

Use the CLI-owned `federal-register.documents` capability. This Skill supplies
intent routing and result-use boundaries only; the CLI owns source discovery,
input/output schemas, HTTP behavior, pagination, limits, validation, and
receipts.

## Before running

1. Read `references/tiangong-data-binding.json`.
2. Use its exact `generatedWithCliVersion` in the package spec below. Never use
   `latest`, a tag, or a version range.
3. Run `data describe` and compare the returned capability version, execution
   manifest digest, operation version, and input/output schema digests with the
   binding. Stop on any mismatch.

```bash
npx --yes --package "@tiangong-ai/cli@<generatedWithCliVersion>" -- \
  tiangong-ai data describe federal-register.documents --json
```

Use the returned Discovery Metadata to confirm current source coverage,
restrictions, `provides`, and `doesNotProvide`. Prefer another source when the
task needs full text, docket attachments, public comments, or legal analysis.

## Prepare the request

Build a `tiangong.data.run-request.v1` envelope and replace the two version
placeholders with the exact values in the binding. Under `input`, provide at
least one publication-date bound and at least one narrowing filter:

```json
{
  "schemaVersion": "tiangong.data.run-request.v1",
  "capabilityId": "federal-register.documents",
  "capabilityVersion": "<binding.capabilityVersion>",
  "operationId": "search",
  "operationVersion": "<binding.operations[0].operationVersion>",
  "limits": {
    "maxPages": 2,
    "maxRecords": 100
  },
  "input": {
    "term": "clean air",
    "publicationDate": {
      "from": "2026-01-01",
      "to": "2026-03-31"
    },
    "agencies": ["environmental-protection-agency"],
    "documentTypes": ["RULE", "PRORULE"],
    "order": "newest",
    "pageSize": 100
  }
}
```

Use the operation input schema returned by `data describe` for current agency,
document type, topic, docket, RIN, ordering, and page-size semantics under
`input`. Optional top-level `limits` may only reduce the operation's published
page, record, response-size, or timeout limits; they cannot raise them. Do not
invent provider slugs or broaden the publication window without the user's
intent.

## Run

```bash
npx --yes --package "@tiangong-ai/cli@<generatedWithCliVersion>" -- \
  tiangong-ai data run federal-register.documents search \
  --input /absolute/path/to/request.json --json
```

The command emits a `tiangong.data.run-result.v1` envelope. Preserve its
`contract`, `warnings`, `errors`, and `receipt` with `data` when handing the
result to another workflow.

## Result boundaries

- Treat `partial` and truncation stop reasons as incomplete coverage; report
  the page or record limit instead of implying an exhaustive search.
- Treat `blocked` as no usable business result and surface the structured
  errors instead of weakening the required bounds.
- Returned URLs are metadata links, not proof that document bodies were
  retrieved or reviewed.
- Verify legally consequential claims against an official edition and qualified
  legal guidance. This Skill does not determine current legal force.
- Cross-source comparison and research evidence admission belong to the caller
  or Auto Research, not this atomic Skill.

## Reference

- `references/tiangong-data-binding.json`: exact execution compatibility
  binding for the reviewed CLI release.
