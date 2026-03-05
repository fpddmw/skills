# Schema Contract

Analyzer reads from `social_events` (shared SQLite DB):
- `id`, `title`, `article_text`, `article_summary`, `seendate_utc`
- filter: `is_analyzed=0` and `seendate_utc >= now-<hours>`

Analyzer writes back:
- `is_analyzed=1`
- `analyzed_at`
- `analysis_model`
- `sarf_label`
- `sarf_reason`
- `dominant_emotion`
- `nimby_risk_score`
- `risk_frame`
- `updated_at`

If columns are missing in existing DBs, analyzer creates them with `ALTER TABLE`.
