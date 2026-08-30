---
name: openaq-data-fetch
description: Discover bounded OpenAQ air-quality monitoring locations and retrieve raw, hourly, or daily measurements for one known sensor through the Tiangong CLI. Use when a task needs OpenAQ location, provider, sensor, parameter, license, coverage, or time-series context; do not use for arbitrary API paths, S3 archive downloads, AQI calculation, health or regulatory decisions, forecasting, source attribution, or cross-sensor analysis.
---

# OpenAQ Data Fetch

Use the CLI-owned `openaq.air-quality` capability. This Skill supplies intent
routing and result-use boundaries only; the CLI owns source discovery,
input/output schemas, API-key injection, pagination, limits, validation, and
receipts.

## Before running

1. Read `references/tiangong-data-binding.json`.
2. Use its exact `generatedWithCliVersion` in every package spec below. Never
   use `latest`, a tag, or a version range.
3. Run `data describe` and compare the returned capability version, execution
   manifest digest, both operation versions, and all input/output schema digests
   with the binding. Stop on any mismatch.
4. Ensure `OPENAQ_API_KEY` is present in the CLI process environment, then run
   the default static doctor. Never place the key in argv, request JSON, a
   Skill-local config file, logs, or output.

```bash
npx --yes --package "@tiangong-ai/cli@<generatedWithCliVersion>" -- \
  tiangong-ai data describe openaq.air-quality --json
npx --yes --package "@tiangong-ai/cli@<generatedWithCliVersion>" -- \
  tiangong-ai data doctor openaq.air-quality --json
```

Use the returned Discovery Metadata to confirm current ownership, coverage,
granularity, source-specific quality and license limits, selection hints,
`provides`, and `doesNotProvide`. Do not substitute facts remembered from an
older Skill revision. A blocked static doctor means the required logical
credential is unavailable; stop instead of bypassing the CLI.

## Choose one operation

- Use `search-locations` when the relevant sensor ID is not yet known. Supply at
  least one explicit country, provider, parameter, license, monitor, mobile,
  center-radius, or bounding-box filter. Inspect provider, monitor status,
  parameter, license, and coverage metadata before choosing a sensor.
- Use `fetch-sensor-measurements` only after selecting one trusted sensor ID.
  Choose `raw` for upstream-reported observations or `hourly`/`daily` for
  OpenAQ's precomputed aggregates, and use an explicit RFC3339 window of no more
  than 366 days.

Do not invent or pass an API path. Do not use this Skill to list or download
OpenAQ S3 archive objects; bulk files require a separately governed
content/download workflow.

## Prepare a location request

Build a `tiangong.data.run-request.v1` envelope and replace the version
placeholders with the exact matching binding values. This example searches a
bounded country/parameter combination; use the current input schema from
`data describe` for other supported filters.

```json
{
  "schemaVersion": "tiangong.data.run-request.v1",
  "capabilityId": "openaq.air-quality",
  "capabilityVersion": "<binding.capabilityVersion>",
  "operationId": "search-locations",
  "operationVersion": "<binding.operations[1].operationVersion>",
  "input": {
    "countryCode": "NL",
    "parameterIds": [2],
    "pageSize": 250,
    "sortOrder": "asc"
  }
}
```

Do not silently widen spatial or provider filters, turn a bounded search into a
global scan, combine center with bounding box, geocode a place name, or infer
that every returned sensor has continuous data for the desired time window.

## Prepare a measurement request

Use the selected sensor's exact ID and an explicit granularity and time window.
Do not merge sensors, convert units, or switch granularity inside this atomic
call.

```json
{
  "schemaVersion": "tiangong.data.run-request.v1",
  "capabilityId": "openaq.air-quality",
  "capabilityVersion": "<binding.capabilityVersion>",
  "operationId": "fetch-sensor-measurements",
  "operationVersion": "<binding.operations[0].operationVersion>",
  "input": {
    "sensorId": 1001,
    "granularity": "hourly",
    "startDateTime": "2026-03-01T00:00:00Z",
    "endDateTime": "2026-03-07T23:59:59Z",
    "pageSize": 1000
  }
}
```

## Run

```bash
npx --yes --package "@tiangong-ai/cli@<generatedWithCliVersion>" -- \
  tiangong-ai data run openaq.air-quality search-locations \
  --input /absolute/path/to/location-request.json --json

npx --yes --package "@tiangong-ai/cli@<generatedWithCliVersion>" -- \
  tiangong-ai data run openaq.air-quality fetch-sensor-measurements \
  --input /absolute/path/to/measurement-request.json --json
```

Each command emits a `tiangong.data.run-result.v1` envelope. Preserve its
`contract`, `warnings`, `errors`, and `receipt` with `data` when handing the
result to another workflow.

## Result boundaries

- Preserve OpenAQ attribution and every original provider's license and
  attribution metadata. OpenAQ's terms do not replace source-specific terms.
- Treat location, owner, provider, instrument, monitor, sensor, and coverage
  fields as provider-dependent metadata, not proof of regulatory-network status
  or measurement quality.
- Keep raw, hourly, and daily granularities explicit. Do not mix raw upstream
  records with OpenAQ aggregates without an explicit analytical method, and
  retain aggregate summary and coverage fields.
- Treat `partial` as incomplete page or record coverage. Report missing pages
  and provider errors with the usable records.
- Treat `blocked` as no usable business result. Surface credential, input,
  endpoint, rate-limit, or provider errors instead of bypassing limits,
  switching endpoints, or calling the old scripts.
- Report page/record-limit truncation. A truncated or empty result is not proof
  that no monitors or pollution observations exist.
- Do not calculate AQI, make health or regulatory claims, infer pollution
  sources, forecast conditions, interpolate space, correct calibration,
  convert units, or claim completeness from this result alone.
- Cross-sensor comparison, recurring monitoring, source fusion, statistical
  inference, bulk archive acquisition, and research evidence admission belong
  to the caller or Auto Research, not this atomic Skill.

## Reference

- `references/tiangong-data-binding.json`: exact execution compatibility
  binding for the reviewed CLI package.
