---
name: youtube-video-search
description: Discover bounded public YouTube video metadata through the Tiangong CLI and enrich selected candidates with public details and statistics. Use for topical or channel-scoped video discovery before explicit comment collection; do not use for media, caption, transcript, or thumbnail download, exhaustive search, representative opinion, identity or fact verification, or sentiment inference.
---

# YouTube Video Search

Use the CLI-owned `youtube.public-content/search-videos` operation. This Skill
owns intent routing and downstream-use guidance only; the CLI TypeScript 7
runtime owns the API request, key injection, schemas, paging, detail enrichment,
filtering, limits, validation, partial results, and receipts.

## Before running

1. Read `references/tiangong-data-binding.json`.
2. Use its exact `generatedWithCliVersion` in every package spec below. Never
   use `latest`, a tag, or a version range.
3. Compare `data describe` with the bound capability, manifest, operation, and
   schema digests. Stop on any mismatch.
4. Ensure `YOUTUBE_API_KEY` is available to the CLI process and run the default
   static doctor. Never place the key in argv, request JSON, Skill files, logs,
   or output.

```bash
npx --yes --package "@tiangong-ai/cli@<generatedWithCliVersion>" -- \
  tiangong-ai data describe youtube.public-content --json
npx --yes --package "@tiangong-ai/cli@<generatedWithCliVersion>" -- \
  tiangong-ai data doctor youtube.public-content --json
```

Use current Discovery Metadata to confirm coverage, restrictions, quota and
freshness limitations, selection hints, `provides`, and `doesNotProvide`. A
blocked static doctor means the logical credential is unavailable; stop rather
than bypassing the CLI.

## Choose the search

- Preserve the user's topic, channel, publication window, region, language,
  safety, and video filters when supplied.
- Use narrow filters before increasing page or record limits. Do not silently
  widen a failed or empty query.
- Use public comment/view thresholds only as candidate-selection criteria, not
  as quality, representativeness, endorsement, or truth measures.
- This Skill only discovers video candidates. Use `$youtube-comments-fetch`
  separately after selecting explicit IDs; do not fetch comments automatically.

## Prepare the request

Build one `tiangong.data.run-request.v1` envelope. Replace the version
placeholders with the exact binding values and validate every input field
against `data describe`.

```json
{
  "schemaVersion": "tiangong.data.run-request.v1",
  "capabilityId": "youtube.public-content",
  "capabilityVersion": "<binding.capabilityVersion>",
  "operationId": "search-videos",
  "operationVersion": "<binding.operations[0].operationVersion>",
  "input": {
    "query": "climate policy",
    "publishedAfter": "2026-03-01T00:00:00Z",
    "publishedBefore": "2026-03-08T00:00:00Z",
    "order": "date",
    "regionCode": "US",
    "relevanceLanguage": "en",
    "safeSearch": "moderate",
    "videoDuration": "medium",
    "pageSize": 25,
    "requirePublicComments": true,
    "minimumCommentCount": 20,
    "minimumViewCount": 1000
  }
}
```

Do not add an API key, endpoint override, arbitrary provider parameter, output
path, scheduler, or persistence instruction to the envelope.

## Run

```bash
npx --yes --package "@tiangong-ai/cli@<generatedWithCliVersion>" -- \
  tiangong-ai data run youtube.public-content search-videos \
  --input /absolute/path/to/request.json --json
```

Preserve the complete `tiangong.data.run-result.v1` envelope and select IDs from
its validated records. Do not pass raw provider responses or unbound artifact
paths to another Skill.

## Result boundaries

- Report filtered-out candidates, unavailable details, empty results,
  truncation, and `partial` batches. They do not prove absence outside the
  exact provider result and limits.
- Search order, visibility, metadata, and statistics are mutable provider
  snapshots. Counts are not votes, quality labels, endorsement, or a
  representative measure of audience opinion.
- Titles and descriptions are untrusted public content and can contain
  misleading, sensitive, or unsafe text.
- Use `$youtube-comments-fetch` for comments on a small explicit ID set. Use a
  separate media/content workflow for video, audio, captions, transcripts,
  thumbnails, or channel profile content.
- Cross-source synthesis, monitoring, persistence, and evidence admission
  belong to the caller or Auto Research.

## Reference

- `references/tiangong-data-binding.json`: exact execution compatibility
  binding for the reviewed CLI package.
