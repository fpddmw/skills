---
name: observer-openaq-historical-query
description: Query historical physical observations from archive SQLite and upsert targeted windows into active observer SQLite for retrospective event analysis. Use when moderator runs directional historical investigations rather than real-time polling.
---

# Observer OpenAQ Historical Query

## Workflow
1. Query archive DB by bbox + time window.
2. Upsert selected `aq_raw_observations` rows into active observer DB.
3. Run existing enrich/summarize stages from observer-openaq-physical-ingestor.

```bash
python3 scripts/historical_query.py ingest \
  --db /abs/path/observer_physical.db \
  --archive-db /abs/path/observer_archive.db \
  --bbox -80.62,40.79,-80.42,40.91 \
  --start-datetime 2024-01-01T00:00:00Z \
  --end-datetime 2024-01-07T00:00:00Z \
  --limit 10000
```

## Status Line
- `PHYSICAL_INGEST_OK source=openaq_archive_db ...`

## Resources
- `scripts/historical_query.py`
