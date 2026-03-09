---
name: moderator-observer-listener-orchestrator
description: "Act as the direct supervisor of observer and listener skills: generate LLM-based directives, execute downstream CLI chains, collect status reports, run escalation review, and iterate multi-cycle reconnaissance planning. Use when OpenClaw only launches moderator processes and moderator must independently control the full observer-listener logic chain."
---

# Moderator Observer-Listener Orchestrator

## Core Contract
- Use this skill as the only entrypoint from OpenClaw.
- Let moderator generate task parameters, dispatch child CLI calls, and evaluate results.
- Return strict JSON only (single object) for every method.
- Keep child skills as parameterized executors.

## Methods
- `patrol`: Build passive patrol directive JSON.
- `recon`: Build active reconnaissance directive JSON.
- `plan-llm`: Call LLM and return normalized task plan JSON.
- `review`: Analyze child status lines and return escalation decision JSON.
- `orchestrate`: End-to-end closed loop (LLM planning -> execute observer/listener/analyzer -> review -> optional next cycle).
  - Also runs eco-council terminal report (`ingest -> enrich -> summarize`) per cycle unless `--skip-eco-council`.

## OpenClaw Recommended Entry
Use `orchestrate` as the default method so OpenClaw only needs to invoke moderator.

```bash
python3 scripts/moderator_router.py orchestrate \
  --objective "investigate potential chemical leak and social panic" \
  --context "initial patrol alert from Midwest US" \
  --observer-db data/observer_physical.db \
  --listener-db data/gdelt_environment.db \
  --planner llm \
  --initial-mode patrol \
  --max-cycles 3
```

## Offline E2E Smoke Test
Use fixtures to validate orchestration without external APIs.

```bash
python3 scripts/moderator_router.py orchestrate \
  --objective "offline smoke test" \
  --planner preset \
  --dry-run
```

Or run real child scripts with local fixtures:

```bash
python3 scripts/moderator_router.py orchestrate \
  --objective "offline fixture run" \
  --planner preset \
  --observer-db data/observer_physical.db \
  --listener-db data/gdelt_environment.db \
  --observer-fixture-json observer-openaq-physical-ingestor/assets/sample_records.json \
  --listener-articles-json listener-gdelt-doc-ingestor/assets/sample_articles.json \
  --classify-mode rule \
  --initial-mode patrol
```

## LLM Planning Requirements
- Required env for `plan-llm` and `orchestrate --planner llm`:
  - `LLM_API_BASE_URL`
  - `LLM_API_KEY`
  - `LLM_MODEL`
- Default config loading order:
  - `listener-gdelt-doc-ingestor/assets/config.env` (for shared key/model)
  - `moderator-observer-listener-orchestrator/assets/config.env`
  - `moderator-observer-listener-orchestrator/assets/config.json`
- Override paths with:
  - `--config-env <path>`
  - `--config-json <path>`
- LLM output is normalized and bounded by moderator before dispatch:
  - mode in `{passive_patrol, active_reconnaissance}`
  - valid bbox
  - timespan hours clamped to `1..72`
  - max records clamped to `20..250`

## Report/Data Feedback
- `orchestrate` returns per-cycle `report` block containing:
  - raw status lines from observer/listener/analyzer
  - parsed status metrics (`upserted`, `exceeded`, `nimby_risk_score`, etc.)
  - SQLite snapshots from observer/listener DBs (`physical_metrics_count`, `social_events_count`, pending analysis)
- This ensures moderator receives both process reports and data-level summaries before next-cycle planning.
- `orchestrate` also returns `execution.eco_council` status lines and report artifact paths under `directive.eco_council.artifacts`.

## Child Skills and Status Contracts
- Observer:
  - `observer-openaq-physical-ingestor/scripts/aqi_ingest.py`
  - status: `PHYSICAL_INGEST_OK`, `PHYSICAL_ENRICH_OK`, `PHYSICAL_SUMMARY_OK`
- Listener:
  - `listener-gdelt-doc-ingestor/scripts/gdelt_ingest.py`
  - `listener-gdelt-doc-ingestor/scripts/gdelt_enrich.py`
  - `listener-gdelt-doc-ingestor/scripts/gdelt_summarize.py`
  - status: `GDELT_SYNC_OK`, `GDELT_ENRICH_OK`, `SOCIAL_SUMMARIZE_OK`
- Analyzer:
  - `listener-caswarn-analyzer/scripts/caswarn_analyzer.py`
  - status: `[SUCCESS] ... {"nimby_risk_score":...}`
- Eco Council Reviewer:
  - `skill-eco-council-reviewer/scripts/eco_council_report.py`
  - status: `ECO_COUNCIL_INGEST_OK`, `ECO_COUNCIL_ENRICH_OK`, `ECO_COUNCIL_SUMMARY_OK`

## References
- `references/moderator-json-contract.md`
