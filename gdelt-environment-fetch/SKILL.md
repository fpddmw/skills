---
name: gdelt-environment-fetch
description: Pull environmental public-opinion records from GDELT DOC API and persist them into a dedicated SQLite table with optional LLM topic classification. Use when building an incremental environment monitoring dataset from GDELT by keyword, region, and time window.
---

# GDELT Environment Fetch

## Core Goal
- Fetch environment-related news/public-opinion records from GDELT.
- Persist records into a raw/enriched table: `gdelt_environment_events`.
- Summarize deduplicated records into `social_events` via idempotent upsert.
- Support incremental sync by time window and query string.
- Optionally classify each record with rule-based or LLM-based topic judgment.

## Triggering Conditions
- Need to ingest GDELT environment signals into a local database.
- Need a reproducible batch job for daily/weekly environment monitoring.
- Need to enrich raw records with relevance judgment (`is_environment`) for downstream summarization.

## Workflow
1. Initialize database schema.

```bash
export GDELT_ENV_DB_PATH="/absolute/path/to/workspace/gdelt_environment.db"
python3 scripts/gdelt_ingest.py init-db --db "$GDELT_ENV_DB_PATH"
```

2. Run incremental sync from GDELT DOC API and write into `gdelt_environment_events`.

```bash
python3 scripts/gdelt_ingest.py ingest \
  --db "$GDELT_ENV_DB_PATH" \
  --query "(climate OR pollution OR biodiversity) AND sourcecountry:CN" \
  --start-datetime 20260301000000 \
  --end-datetime 20260303235959 \
  --max-records 250 \
  --classify-mode llm
```

3. Enrich stored rows (URL canonicalization, AvgTone/GoldsteinScale extraction, optional LLM classification).

```bash
python3 scripts/gdelt_enrich.py \
  --db "$GDELT_ENV_DB_PATH" \
  --classify-mode llm \
  --limit 1000
```

4. Summarize into `social_events` using idempotent upsert.

```bash
python3 scripts/gdelt_summarize.py \
  --db "$GDELT_ENV_DB_PATH" \
  --only-relevant \
  --limit 2000
```

5. Query stored records.

```bash
python3 scripts/gdelt_ingest.py list-events \
  --db "$GDELT_ENV_DB_PATH" \
  --limit 50
```

## Data Contract
- Raw/enriched table: `gdelt_environment_events` (created by `init-db`).
- Summary table: `social_events` (created by `init-db`).
- URL dedup key: `url_key` (canonicalized URL).
- Core fields:
  - source metadata: `title`, `url`, `source_domain`, `source_country`, `language`, `seendate_utc`
  - enrichment metrics: `avg_tone`, `goldstein_scale`
  - query metadata: `query_text`, `start_datetime`, `end_datetime`
  - optional classification: `env_relevance`, `env_label`, `env_reason`, `classifier`
  - traceability: `raw_json`, `created_at`, `updated_at`

## Classification Modes
- `none`: do not classify.
- `rule`: keyword-based environment relevance detection.
- `llm`: OpenAI-compatible chat completion API for topic classification.

## Required Parameters
- `--db`: SQLite file path.
- `--query`: GDELT DOC query expression.
- `--start-datetime`: UTC `YYYYMMDDHHMMSS`.
- `--end-datetime`: UTC `YYYYMMDDHHMMSS`.
- `--max-records`: max articles for one request (1-250).

## LLM Classification Environment
- Non-secret defaults can be managed in `assets/config.example.json`:
  - `llm_api_base_url` (for example: `https://api.openai.com/v1`)
  - `llm_model` (for example: `gpt-4o-mini`)
- Secrets must be managed with environment variables (see `assets/config.example.env`):
  - `LLM_API_KEY`
  - `LLM_API_BASE_URL` (runtime override, optional when using JSON defaults externally)
  - `LLM_MODEL` (runtime override, optional when using JSON defaults externally)

## GDELT Connection Notes
- This skill uses GDELT DOC HTTP API, not direct SQL access to a GDELT database.
- Primary endpoint:
  - `https://api.gdeltproject.org/api/v2/doc/doc`
- Network egress is required from runtime to GDELT API.

## Error Handling
- GDELT API/network error: fail current run with actionable error.
- Invalid datetime format: fail fast before request.
- LLM response parse failure: keep raw row, fallback to `rule` classification.
- Duplicate records: handled by URL-level dedup (`url_key`) + idempotent upsert.

## References
- `references/env.md`
- `references/schema.md`

## Assets
- `assets/config.example.json` (non-secret config template)
- `assets/config.example.env` (secret/env template)

## Scripts
- `scripts/gdelt_ingest.py` (stage-1 ingest)
- `scripts/gdelt_enrich.py` (stage-2 enrich)
- `scripts/gdelt_summarize.py` (stage-3 summarize)
- `scripts/gdelt_fetch.py` (compatibility entrypoint)
