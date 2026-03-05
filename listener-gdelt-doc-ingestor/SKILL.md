---
name: listener-gdelt-doc-ingestor
description: Ingest GDELT DOC environmental/public-opinion news into SQLite with theme-enhanced queries, enrich sentiment/conflict metrics, and summarize idempotently for downstream Listener analysis. Use when building the upstream Listener feed before running local SARF/NIMBY semantic analysis.
---

# Listener Upstream: GDELT Ingestor

## Core Goal
- Pull high-relevance environmental social-signal news from GDELT DOC API.
- Apply query + `theme:*` filters to increase retrieval precision without CAMEO.
- Persist raw/enriched rows into `gdelt_environment_events`.
- Upsert deduplicated rows into `social_events` for downstream analyzer skills.

## Execution Contract
- Keep this skill mode-agnostic.
- Accept only explicit command parameters (`--query`, `--themes`, `--start-datetime`, `--end-datetime`, limits).
- Let upper-layer orchestration decide scenario:
  - rolling short windows => near-real-time local hotspot monitoring
  - specified historical windows => retrospective event analysis
- Do not embed "monitoring vs backfill" branching logic inside this skill.

## Workflow
1. Initialize schema.

```bash
export GDELT_ENV_DB_PATH="/absolute/path/to/workspace/gdelt_environment.db"
python3 scripts/gdelt_ingest.py init-db --db "$GDELT_ENV_DB_PATH"
```

2. Ingest with DOC query + theme filters.

```bash
python3 scripts/gdelt_ingest.py ingest \
  --db "$GDELT_ENV_DB_PATH" \
  --query "(protest OR petition OR contamination OR policy) AND sourcecountry:US" \
  --themes "ENV_GREENHOUSE,ENV_POLLUTION" \
  --start-datetime 20260301000000 \
  --end-datetime 20260303235959 \
  --max-records 250 \
  --classify-mode llm
```

Offline E2E (no external network):

```bash
python3 scripts/gdelt_ingest.py ingest \
  --db "$GDELT_ENV_DB_PATH" \
  --query "(protest OR policy) AND sourcecountry:US" \
  --start-datetime 20260301000000 \
  --end-datetime 20260301010000 \
  --classify-mode rule \
  --articles-json assets/sample_articles.json
```

Notes:
- Default built-in themes are enabled unless `--disable-default-themes` is set.
- Query built at runtime: `(query) AND (theme:... OR theme:...)`.

3. Enrich rows (URL key + AvgTone + Goldstein + classification).

```bash
python3 scripts/gdelt_enrich.py \
  --db "$GDELT_ENV_DB_PATH" \
  --classify-mode llm \
  --limit 1000
```

4. Summarize into downstream-ready table.

```bash
python3 scripts/gdelt_summarize.py \
  --db "$GDELT_ENV_DB_PATH" \
  --only-relevant \
  --limit 2000
```

5. Inspect recent source rows.

```bash
python3 scripts/gdelt_ingest.py list-events \
  --db "$GDELT_ENV_DB_PATH" \
  --limit 50
```

## Data Contract
- Source table: `gdelt_environment_events`
- Summary table: `social_events`
- Dedup key: `url_key` (canonicalized URL)
- Downstream handoff fields in `social_events`:
  - `article_summary`, `article_text`
  - `is_analyzed` (default `0`)
  - SARF/NIMBY analysis result columns populated by downstream analyzer

## Classification Modes
- `none`: keep topic labels empty
- `rule`: keyword-based environment relevance
- `llm`: OpenAI-compatible classification API

## Required Parameters
- `--db`: SQLite path
- `--query`: base DOC query
- `--start-datetime`, `--end-datetime`: UTC `YYYYMMDDHHMMSS`
- `--max-records`: `[1,250]`

## Environment
- Optional runtime env:
  - `GDELT_ENV_DB_PATH`
- Required only for LLM mode:
  - `LLM_API_BASE_URL`
  - `LLM_API_KEY`
  - `LLM_MODEL`

## References
- `references/env.md`
- `references/schema.md`

## Scripts
- `scripts/gdelt_ingest.py`
- `scripts/gdelt_enrich.py`
- `scripts/gdelt_summarize.py`
- `scripts/gdelt_fetch.py` (legacy compatibility entrypoint)
