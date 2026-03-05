# Source Feasibility and Cost Evaluation (as of 2026-03-03)

## Decision Summary
- Primary source: **OpenAQ API v3**
- Fallback source: **Copernicus CAMS via ADS**
- Cost constraint: **free-only**

Recommendation:
- Start with OpenAQ for station-level ground observations and low implementation complexity.
- Keep CAMS as fallback or complement when station coverage is sparse or gridded fields are required.

## OpenAQ API

Feasibility:
- Strong fit for station observations (PM2.5/NO2/O3 available via sensors/measurements endpoints).
- Supports geospatial/bbox and datetime-based filtering through v3 resources.

Cost and quota:
- Free tier is available.
- API key is required.
- Rate limits are enforced; 429 handling is mandatory.

Implementation notes:
- Use `locations -> sensors -> hours/measurements` traversal.
- Respect throttling (`OPENAQ_SLEEP_MS`) and pagination (`OPENAQ_MAX_PAGES`).
- Persist raw payload for reproducibility.

## Copernicus CAMS (ADS APIs)

Feasibility:
- Good for gridded global atmospheric datasets (analysis/forecast/reanalysis).
- Better than station APIs for consistent spatial coverage over large areas.

Cost:
- Data access under Copernicus open-data policy (free access), but account/token setup is required.

Complexity:
- Higher than OpenAQ:
  - authentication and token management
  - asynchronous retrieval workflows
  - NetCDF processing pipeline needed before SQLite flattening

Recommendation:
- Use CAMS only when gridded coverage or model products are mandatory.
- Keep CAMS as phase-2 extension after OpenAQ baseline is stable.

## Calling Rules for This Skill

1. Validate bbox and UTC time range.
2. Ingest from OpenAQ with API key and rate-limit sleep.
3. Enrich with profile-based threshold comparison (`who_2021` / `us_epa_core`).
4. Summarize with idempotent upsert into `physical_metrics`.
5. Emit status lines for data-scale observability.
