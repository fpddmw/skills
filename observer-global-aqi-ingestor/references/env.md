# Environment Variables

## Required (OpenAQ ingestion)
- `OPENAQ_API_KEY`: required API key for OpenAQ v3.

## Recommended defaults
- `OPENAQ_API_BASE_URL`: default `https://api.openaq.org/v3`
- `OPENAQ_TIMEOUT_SECONDS`: default `20`
- `OPENAQ_PAGE_LIMIT`: default `100`
- `OPENAQ_MAX_PAGES`: default `20`
- `OPENAQ_SLEEP_MS`: default `1100` (rate-limit friendly)
- `OPENAQ_USER_AGENT`: request user-agent label
- `OBSERVER_DB_PATH`: SQLite path for this skill

## Optional fallback source (CAMS planning)
- `CAMS_ADS_URL`: usually `https://ads.atmosphere.copernicus.eu/api`
- `CAMS_ADS_PAT`: personal access token for ADS APIs
- `CAMS_DATASET`: dataset short name when fallback is enabled
- `CAMS_FORMAT`: e.g. `netcdf`

## Pipeline commands
- `observer_ingest.py ingest`: call OpenAQ API and upsert raw observations
- `observer_enrich.py`: flatten + compare against standard profile
- `observer_summarize.py summarize`: idempotent upsert to `physical_metrics`
- `observer_summarize.py list-metrics`: read aggregated metrics
- `aqi_ingest.py`: compatibility entrypoint (legacy all-in-one CLI)
