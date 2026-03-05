# SQLite Schema

Tables:
- `gdelt_environment_events`: upstream raw + enriched layer
- `social_events`: deduplicated downstream-ready Listener events

## `gdelt_environment_events`
- Source identity: `event_key` (unique), `url_key` (canonical URL unique when present)
- Batch metadata: `query_text`, `start_datetime`, `end_datetime`
- Article metadata: `title`, `url`, `source_domain`, `source_country`, `language`, `seendate_utc`, `social_image_url`
- Built-in GDELT metrics: `avg_tone`, `goldstein_scale`
- Relevance labels: `env_relevance`, `env_label`, `env_reason`, `classifier`, `classified_at`
- Traceability: `raw_json`, `created_at`, `updated_at`

## `social_events`
- Key fields: `url_key` (unique), `event_key`
- Source snapshot: `title`, `url`, `source_domain`, `source_country`, `language`, `seendate_utc`
- Sentiment/conflict: `avg_tone`, `goldstein_scale`, `tone_bucket`, `conflict_bucket`
- Relevance: `env_relevance`, `env_label`, `env_reason`, `classifier`
- Downstream analyzer input: `article_summary`, `article_text`, `is_analyzed`
- Downstream analyzer output: `analyzed_at`, `analysis_model`, `sarf_label`, `sarf_reason`, `dominant_emotion`, `nimby_risk_score`, `risk_frame`
- Housekeeping: `raw_event_count`, `created_at`, `updated_at`

## Indexes
- Upstream scans: `idx_gdelt_env_seen`, `idx_gdelt_env_relevance`, `idx_gdelt_env_query_seen`
- Dedup and metrics: `idx_gdelt_env_url_key_unique`, `idx_gdelt_env_goldstein`, `idx_gdelt_env_avg_tone`
- Downstream reads: `idx_social_events_seen`, `idx_social_events_env`, `idx_social_events_conflict`, `idx_social_events_analyzed`
