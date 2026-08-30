---
name: usgs-water-iv-fetch
description: Retrieve USGS WaterServices instantaneous station observations through the Tiangong CLI for one bounded numeric area or an explicit site list and one bounded time selector. Use for streamflow, gage-height, or other USGS time-series measurements with qualifiers; do not use for station discovery, daily values, flood-stage classification, forecasts, hazard advice, or workflows that require guaranteed service after the legacy API retires.
---

# USGS Water Instantaneous Values

Use the CLI-owned `usgs.water-instantaneous-values` capability. This Skill
supplies intent routing and result-use boundaries only; the CLI owns source
discovery, input/output schemas, HTTP behavior, limits, validation, and
receipts.

## Before running

1. Read `references/tiangong-data-binding.json`.
2. Use its exact `generatedWithCliVersion` in the package spec below. Never use
   `latest`, a tag, or a version range.
3. Run `data describe` and compare the returned capability version, execution
   manifest digest, operation version, and input/output schema digests with the
   binding. Stop on any mismatch.
4. Run the default static doctor. Do not add `--live` unless the user explicitly
   asks for a provider probe.

```bash
npx --yes --package "@tiangong-ai/cli@<generatedWithCliVersion>" -- \
  tiangong-ai data describe usgs.water-instantaneous-values --json
npx --yes --package "@tiangong-ai/cli@<generatedWithCliVersion>" -- \
  tiangong-ai data doctor usgs.water-instantaneous-values --json
```

Use the returned Discovery Metadata to confirm current source coverage,
freshness, decommission status, restrictions, `provides`, and
`doesNotProvide`. Prefer a modern USGS Water Data capability when the task
requires operation beyond the legacy service lifetime.

## Prepare the request

Build a `tiangong.data.run-request.v1` envelope and replace the two version
placeholders with the exact values in the binding. Under `input`, choose exactly
one spatial selector (`boundingBox` or `siteNumbers`) and exactly one time
selector (`period` or a paired explicit start/end window):

```json
{
  "schemaVersion": "tiangong.data.run-request.v1",
  "capabilityId": "usgs.water-instantaneous-values",
  "capabilityVersion": "<binding.capabilityVersion>",
  "operationId": "fetch",
  "operationVersion": "<binding.operations[0].operationVersion>",
  "input": {
    "boundingBox": {
      "minLongitude": -77.3,
      "minLatitude": 38.8,
      "maxLongitude": -77.0,
      "maxLatitude": 39.1
    },
    "period": "P1D",
    "parameterCodes": ["00060", "00065"],
    "siteType": "ST",
    "siteStatus": "active"
  }
}
```

Use the operation input schema returned by `data describe` for current field
semantics and limits. Preserve leading zeroes in site and parameter identifiers.
Do not infer site numbers from place names, widen an area or time range without
the user's intent, or mix selectors that the schema declares exclusive.

## Run

```bash
npx --yes --package "@tiangong-ai/cli@<generatedWithCliVersion>" -- \
  tiangong-ai data run usgs.water-instantaneous-values fetch \
  --input /absolute/path/to/request.json --json
```

The command emits a `tiangong.data.run-result.v1` envelope. Preserve its
`contract`, `warnings`, `errors`, and `receipt` with `data` when handing the
result to another workflow.

## Result boundaries

- Treat qualifier `P` and `provisional: true` as provider warnings that values
  may change; retain qualifiers with any downstream use.
- Treat `partial` as incomplete normalization coverage and report the affected
  series or value paths. Do not convert missing or invalid observations to
  zero.
- Treat `blocked` as no usable business result and surface the structured
  errors instead of silently broadening the query or bypassing limits.
- Report record-limit truncation; do not imply that a truncated result is an
  exhaustive time series.
- Do not infer flood stage, return period, alert status, hazard, cause, or
  policy meaning from raw station observations.
- Cross-source comparison and research evidence admission belong to the caller
  or Auto Research, not this atomic Skill.

## Reference

- `references/tiangong-data-binding.json`: exact execution compatibility
  binding for the reviewed CLI release.
