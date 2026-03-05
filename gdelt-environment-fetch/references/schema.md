# SQLite Schema

Table names:
- `gdelt_environment_events` (raw + enriched layer)
- `social_events` (deduplicated summary layer)

Purpose:
- Store GDELT DOC records for environment public-opinion monitoring.
- Preserve raw source payload and optional classification outputs.
- Provide a summarized URL-level table for downstream consumption.

`gdelt_environment_events` columns:
- `id`: INTEGER PK
- `event_key`: UNIQUE hash key for idempotent upsert
- `url_key`: canonicalized URL key for URL-level dedup (unique when present)
- `query_text`: query used for this batch
- `start_datetime`, `end_datetime`: requested UTC window (`YYYYMMDDHHMMSS`)
- `title`, `url`, `source_domain`, `source_country`, `language`, `seendate_utc`
- `social_image_url`, `avg_tone`, `goldstein_scale`
- `env_relevance`: `1` yes / `0` no / `NULL` unclassified
- `env_label`: short classifier label
- `env_reason`: short reason text
- `classifier`: `rule` or `llm:<model>`
- `classified_at`: UTC ISO timestamp
- `raw_json`: full raw article JSON for traceability
- `created_at`, `updated_at`: UTC ISO timestamps

`social_events` columns:
- `id`: INTEGER PK
- `url_key`: UNIQUE URL-level key (upsert target)
- `event_key`, `title`, `url`, `source_domain`, `source_country`, `language`, `seendate_utc`
- `avg_tone`, `goldstein_scale`
- `tone_bucket`: `positive|neutral|negative`
- `conflict_bucket`: `low-conflict|mid-conflict|high-conflict`
- `env_relevance`, `env_label`, `env_reason`, `classifier`
- `raw_event_count`: count of source upsert touches
- `created_at`, `updated_at`

Indexes:
- `idx_gdelt_env_seen` for latest-event scan
- `idx_gdelt_env_relevance` for relevance filtering
- `idx_gdelt_env_query_seen` for per-query time analysis
- `idx_gdelt_env_url_key_unique` for URL dedup
- `idx_social_events_seen` for summary reads by time
- `idx_social_events_env` for relevant-only summary filtering
