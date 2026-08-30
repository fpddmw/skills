---
name: airnow-hourly-obs-fetch
description: Retrieve preliminary U.S. EPA AirNow HourlyAQObs monitoring-site records through the Tiangong CLI for an inclusive UTC-hour window, WGS84 bounding box, and pollutant list. Use for site-hour observations with source-file lineage; do not use for regulatory AQS compliance data, forecasts, health guidance, exposure estimates, or place-name geocoding.
---

# AirNow Hourly Observations

Use the CLI-owned `airnow.hourly-observations` capability. This Skill supplies
intent routing and result-use boundaries only; the CLI owns source discovery,
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
  tiangong-ai data describe airnow.hourly-observations --json
```

Use the returned Discovery Metadata to confirm current source coverage,
freshness, restrictions, `provides`, and `doesNotProvide`. Do not substitute
facts remembered from an older Skill revision.

## Prepare the request

Build a `tiangong.data.run-request.v1` envelope. Replace the two version
placeholders with the exact values in the binding. The operation input requires
hour-aligned UTC timestamps, a WGS84 bounding box, and one or more supported
pollutant identifiers:

```json
{
  "schemaVersion": "tiangong.data.run-request.v1",
  "capabilityId": "airnow.hourly-observations",
  "capabilityVersion": "<binding.capabilityVersion>",
  "operationId": "fetch-hourly",
  "operationVersion": "<binding.operations[0].operationVersion>",
  "input": {
    "startDateTimeUtc": "2026-03-22T00:00:00Z",
    "endDateTimeUtc": "2026-03-22T06:00:00Z",
    "boundingBox": {
      "minLongitude": -123.5,
      "minLatitude": 37.0,
      "maxLongitude": -121.5,
      "maxLatitude": 38.8
    },
    "parameters": ["PM25", "OZONE"]
  }
}
```

Use the operation input schema returned by `data describe` when selecting
fields or values under `input`. Do not infer unsupported aliases or silently
widen the requested time or geographic range.

## Run

```bash
npx --yes --package "@tiangong-ai/cli@<generatedWithCliVersion>" -- \
  tiangong-ai data run airnow.hourly-observations fetch-hourly \
  --input /absolute/path/to/request.json --json
```

The command emits a `tiangong.data.run-result.v1` envelope. Preserve its
`contract`, `warnings`, `errors`, and `receipt` with `data` when handing the
result to another workflow.

## Result boundaries

- Treat `partial` as incomplete coverage and report missing or invalid hourly
  files; never convert missing observations to zero.
- Treat `blocked` as no usable business result and surface the structured
  errors instead of retrying with broader inputs.
- Do not interpret AQI as health advice, estimate exposure, interpolate between
  sites, combine other sources, or use preliminary AirNow records for
  regulatory compliance.
- Cross-source comparison and research evidence admission belong to the caller
  or Auto Research, not this atomic Skill.

## Reference

- `references/tiangong-data-binding.json`: exact execution compatibility
  binding for the reviewed CLI release.
