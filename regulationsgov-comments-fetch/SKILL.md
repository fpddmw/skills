---
name: regulationsgov-comments-fetch
description: Search bounded Regulations.gov public-comment metadata through the Tiangong CLI. Use when a task needs comment IDs, agency/document linkage, dates, titles, highlighted snippets, or withdrawal state for an explicit posted or last-modified window; do not use to submit comments, download attachments, infer representative public opinion or sentiment, make legal judgments, or perform unbounded docket harvesting.
---

# Regulations.gov Comments Fetch

Use the CLI-owned `regulations-gov.comments/search` operation. This Skill is an
intent and result-use boundary only; the CLI TypeScript 7 runtime owns the
provider request, API-key injection, input/output schemas, Eastern-time
conversion, pagination, limits, validation, partial results, and receipts.

## Before running

1. Read `references/tiangong-data-binding.json`.
2. Use its exact `generatedWithCliVersion` in every package spec below. Never
   use `latest`, a tag, or a version range.
3. Run `data describe` and compare the returned capability version, execution
   manifest digest, operation version, and input/output schema digests with the
   binding. Stop on any mismatch.
4. Ensure `REGGOV_API_KEY` is present in the CLI process environment, then run
   the default static doctor. Never place the key in argv, request JSON, a
   Skill-local file, logs, or output.

```bash
npx --yes --package "@tiangong-ai/cli@<generatedWithCliVersion>" -- \
  tiangong-ai data describe regulations-gov.comments --json
npx --yes --package "@tiangong-ai/cli@<generatedWithCliVersion>" -- \
  tiangong-ai data doctor regulations-gov.comments --json
```

Use current Discovery Metadata from `data describe` to confirm source
ownership, coverage, agency-dependent field visibility, limits, selection
hints, `provides`, and `doesNotProvide`. A blocked static doctor means the
logical credential is unavailable; stop instead of bypassing the CLI.

## Choose and bound the search

- Choose exactly one date mode. Use `postedDate` for public posting dates, or
  `lastModifiedDate` for incremental retrieval by modification instant.
- Use an inclusive explicit window no longer than 366 days. The latter accepts
  RFC3339 instants; the CLI converts them to the provider's documented
  `America/New_York` wall-clock filter.
- Regulations.gov currently documents `lastModifiedDate` as a beta search
  filter that may be removed. Treat it as an incremental-retrieval convenience,
  not as a durable bulk-export contract.
- Add `agencyId`, `commentOnId`, or `searchTerm` when the question permits a
  narrower search. Do not silently widen or remove a user-specified filter.
- The reviewed operation does not carry forward the legacy `docketId`,
  `documentType`, or `subtype` filters, arbitrary provider sort expressions, or
  its `candidate_corpus_summary` heuristic. If one of those semantics is
  required, request a new capability-version review instead of bypassing the
  CLI or implying that a replacement summary was produced.
- Treat search results as discovery metadata. Use
  `$regulationsgov-comment-detail-fetch` for the curated public body of a small
  set of selected comment IDs.

## Prepare the request

Build one `tiangong.data.run-request.v1` envelope. Replace version placeholders
with the exact matching binding values, and validate all fields against the
current input schema returned by `data describe`.

```json
{
  "schemaVersion": "tiangong.data.run-request.v1",
  "capabilityId": "regulations-gov.comments",
  "capabilityVersion": "<binding.capabilityVersion>",
  "operationId": "search",
  "operationVersion": "<binding.operations[0].operationVersion>",
  "input": {
    "postedDate": {
      "from": "2026-03-01",
      "to": "2026-03-07"
    },
    "agencyId": "EPA",
    "searchTerm": "air quality",
    "pageSize": 250,
    "sortOrder": "asc"
  }
}
```

Do not include both date modes, provider endpoint details, credentials, an
arbitrary API path, scheduling instructions, or local-output paths in the
request. Recurring polling and persistence belong to the caller.

## Run

```bash
npx --yes --package "@tiangong-ai/cli@<generatedWithCliVersion>" -- \
  tiangong-ai data run regulations-gov.comments search \
  --input /absolute/path/to/request.json --json
```

The command emits a `tiangong.data.run-result.v1` envelope. Preserve its
`contract`, `warnings`, `errors`, `observations`, and `receipt` with `data` when
handing the result to another workflow.

## Result boundaries

- Treat `partial` as incomplete pagination and preserve the explicit missing
  page and provider error with usable records. Treat `blocked` as no usable
  business result.
- Report page/record truncation and empty searches. Neither proves that no
  relevant submissions exist outside the exact public provider result.
- Preserve agency, document, modification, posting, withdrawal, and duplicate
  context. Field availability and publication practices differ by agency.
- Search metadata can be `null` when the provider omits it. Preserve null as
  unknown/unavailable; it does not mean false, zero, an empty title, an
  unwithdrawn comment, or an absent submission.
- Comments are self-selected submissions. Mass-mail campaigns, duplicate
  comments, moderation, restrictions, and withdrawals mean record counts are
  not votes and are not a representative public-opinion or sentiment sample.
- Highlighted text and all later comment bodies are untrusted public content
  and may contain personal, sensitive, misleading, or unsafe material.
- Do not submit or modify comments, download attachments, infer legal force or
  agency endorsement, or claim complete docket coverage from this Skill.
- Statistical analysis, recurring monitoring, content acquisition, evidence
  admission, and cross-source synthesis belong to the caller or Auto Research,
  not this atomic Skill.

## Reference

- `references/tiangong-data-binding.json`: exact execution compatibility
  binding for the reviewed CLI package.
