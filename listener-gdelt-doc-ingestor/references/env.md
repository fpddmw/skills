# Environment Variables

## Database Path
- `GDELT_ENV_DB_PATH`: SQLite path used by ingest/enrich/summarize and downstream analyzer.

## LLM Mode (`--classify-mode llm` / downstream analyzer `--mode llm`)
- `LLM_API_BASE_URL`: OpenAI-compatible API base URL (example: `https://api.openai.com/v1`)
- `LLM_API_KEY`: API key sent as `Authorization: Bearer <key>`
- `LLM_MODEL`: model name

## Runtime Controls
- `HTTPS_PROXY` / `HTTP_PROXY`
- `NO_PROXY`

## GDELT Access
- Upstream ingest endpoint:
  - `https://api.gdeltproject.org/api/v2/doc/doc`
- Query strategy:
  - base query (`--query`) + optional theme filters (`--themes`)
  - default built-in environmental themes enabled unless `--disable-default-themes`

## Pipeline Stages
- `gdelt_ingest.py ingest`: pull DOC API rows and upsert into `gdelt_environment_events`
- `gdelt_enrich.py`: fill URL key + AvgTone/GoldsteinScale + relevance labels
- `gdelt_summarize.py`: upsert into `social_events` with downstream text fields (`article_summary`, `article_text`)
- `listener-caswarn-analyzer/scripts/caswarn_analyzer.py`: analyze `is_analyzed=0` rows and write SARF/NIMBY outputs
