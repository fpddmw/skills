---
name: observer-openaq-physical-ingestor
description: Ingest global air-quality observations (PM2.5/NO2/O3) from OpenAQ by bounding box and time window, enrich with WHO/EPA threshold variance, and idempotently upsert daily aggregates into SQLite physical_metrics. Use when building objective observer-layer baselines for environmental monitoring.
---

# Observer Global AQI Ingestor

## Core Positioning
- This skill is the "Observer" data foundation: capture objective physical-world signals.
- Primary source: OpenAQ API (global ground-station measurements).
- Optional fallback strategy notes: CAMS (Copernicus gridded atmospheric datasets).

## Execution Contract
- Keep this skill mode-agnostic.
- Accept only explicit command parameters (`--bbox`, `--start-datetime`, `--end-datetime`, limits, profile flags).
- Let upper-layer orchestration decide scenario:
  - rolling short windows => near-real-time hotspot monitoring
  - specified historical windows => retrospective event analysis
- Do not embed "realtime vs backfill" branching logic inside this skill.

## Skill Boundary: One Skill vs Three Skills
- Current recommendation: keep `ingest -> enrich -> summarize` in one skill.
- Reason:
  - shared schema and idempotency keys
  - strict pipeline coupling for quality control
  - lowest operational complexity for initial deployment
- Split into separate skills only when:
  - upstream sources diversify beyond OpenAQ/CAMS
  - enrichment standards become independent reusable components
  - ownership/release cadence needs separation across teams

## Pipeline Workflow

1. Initialize tables.

```bash
export OBSERVER_DB_PATH="/absolute/path/to/workspace/observer_physical.db"
python3 scripts/observer_ingest.py init-db --db "$OBSERVER_DB_PATH"
```

2. Ingest raw observations by bounding box and time range.

```bash
python3 scripts/observer_ingest.py ingest \
  --db "$OBSERVER_DB_PATH" \
  --bbox 103.5,1.1,104.2,1.6 \
  --start-datetime 2026-03-01T00:00:00Z \
  --end-datetime 2026-03-03T00:00:00Z
```

Offline E2E (no external network):

```bash
python3 scripts/observer_ingest.py ingest \
  --db "$OBSERVER_DB_PATH" \
  --bbox 103.5,1.1,104.2,1.6 \
  --start-datetime 2026-03-01T00:00:00Z \
  --end-datetime 2026-03-01T01:00:00Z \
  --fixture-json assets/sample_records.json
```

3. Enrich and flatten rows (unit normalization + threshold variance).

```bash
python3 scripts/observer_enrich.py \
  --db "$OBSERVER_DB_PATH" \
  --standard-profile auto \
  --limit 100000
```

4. Summarize and idempotent upsert to `physical_metrics`.

```bash
python3 scripts/observer_summarize.py summarize \
  --db "$OBSERVER_DB_PATH" \
  --group-limit 200000
```

5. Inspect metrics.

```bash
python3 scripts/observer_summarize.py list-metrics --db "$OBSERVER_DB_PATH" --limit 50
```

## Data Model
- Raw table: `aq_raw_observations`
- Enriched table: `aq_enriched_observations`
- Summary table: `physical_metrics`
- Idempotency:
  - raw unique key: `(source_name, sensor_id, parameter_code, observed_utc)`
  - summary unique key: `metric_key` (`date|country|parameter|profile`)

## Enrichment Logic
- Flatten payload and normalize units to `ug/m3` where possible.
- Profiles:
  - `who_2021`: PM2.5/NO2/O3 WHO guideline screening thresholds
  - `us_epa_core`: PM2.5/NO2/O3 EPA core thresholds
  - `auto`: `US -> us_epa_core`, others -> `who_2021`
- Compute:
  - `variance_ratio = value_ugm3 / threshold_ugm3`
  - `is_exceed = variance_ratio > 1`

## Output Status Lines
- `PHYSICAL_INGEST_OK ...`
- `PHYSICAL_ENRICH_OK ...`
- `PHYSICAL_SUMMARY_OK ...`

## Source Feasibility and Cost
- See `references/source-feasibility.md` for detailed OpenAQ/CAMS evaluation (free-only constraint).

## References
- `references/env.md`
- `references/schema.md`
- `references/source-feasibility.md`

## Assets
- `assets/config.example.json`
- `assets/config.example.env`

## Scripts
- `scripts/observer_ingest.py` (stage-1 ingest)
- `scripts/observer_enrich.py` (stage-2 enrich)
- `scripts/observer_summarize.py` (stage-3 summarize/list)
- `scripts/aqi_ingest.py` (compatibility entrypoint)
