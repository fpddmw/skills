---
name: observer-openmeteo-physical-ingestor
description: Ingest global gridded PM2.5/NO2/O3 physical signals from Open-Meteo Air Quality API into observer SQLite, then enrich/summarize with the same threshold pipeline. Use when OpenAQ coverage is sparse, API keys are unavailable, or moderator needs stable global patrol sampling.
---

# Observer Open-Meteo Physical Ingestor

## Workflow
1. Ingest gridded hourly data.
2. Reuse observer enrich stage.
3. Reuse observer summarize stage.

```bash
python3 scripts/openmeteo_ingest.py ingest \
  --db /abs/path/observer_physical.db \
  --bbox -80.62,40.79,-80.42,40.91 \
  --start-datetime 2026-03-08T12:00:00Z \
  --end-datetime 2026-03-09T12:00:00Z \
  --max-locations 9
```

```bash
python3 scripts/openmeteo_ingest.py enrich --db /abs/path/observer_physical.db --standard-profile auto
python3 scripts/openmeteo_ingest.py summarize --db /abs/path/observer_physical.db
```

## Status Lines
- `PHYSICAL_INGEST_OK`
- `PHYSICAL_ENRICH_OK`
- `PHYSICAL_SUMMARY_OK`

## Notes
- This skill calls Open-Meteo (no API key required for standard usage).
- Data is gridded model output; use for wide-area patrol and coverage backfill.

## Resources
- `scripts/openmeteo_ingest.py`
