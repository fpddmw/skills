---
name: listener-caswarn-analyzer
description: Analyze recent unanalyzed GDELT social events from local SQLite and label NIMBY/SARF amplification risk with dominant emotion and risk score. Use after listener-gdelt-doc-ingestor summarize stage when downstream Listener intelligence needs deep semantic and emotional assessment.
---

# CASWARN Analyzer

## Core Goal
- Read `social_events` rows where `is_analyzed=0` in the latest time window.
- Run semantic risk analysis (LLM or rule fallback) for NIMBY/SARF patterns.
- Write back `sarf_label`, `sarf_reason`, `dominant_emotion`, `nimby_risk_score`, `risk_frame`.
- Emit deterministic CLI success line for orchestration.

## Workflow
1. Ensure upstream GDELT summarize already populated `social_events`.
2. Run analyzer on last 24 hours.

```bash
python3 scripts/caswarn_analyzer.py \
  --db "$GDELT_ENV_DB_PATH" \
  --hours 24 \
  --limit 50 \
  --mode llm
```

3. For offline/local smoke tests, switch to rule mode.

```bash
python3 scripts/caswarn_analyzer.py \
  --db "$GDELT_ENV_DB_PATH" \
  --hours 24 \
  --limit 20 \
  --mode rule
```

## Runtime Contract
- Input query:
  - last `--hours` rows where `is_analyzed=0`
- Output update:
  - set `is_analyzed=1`
  - populate analysis columns and `analyzed_at`
- CLI output format:
  - `[SUCCESS] Analyzed 50 news items | {"dominant_emotion":"fear","nimby_risk_score":0.85}`

## Environment
- Required:
  - `GDELT_ENV_DB_PATH` (or pass `--db`)
- Required for `--mode llm`:
  - `LLM_API_BASE_URL`
  - `LLM_API_KEY`
  - `LLM_MODEL`

## Resources
- `scripts/caswarn_analyzer.py`
- `references/schema.md`
- `references/env.md`
