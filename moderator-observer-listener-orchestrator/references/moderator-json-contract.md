# Moderator JSON Contract

## 1) plan-llm output

```json
{
  "mode": "llm_plan",
  "objective": "investigate risk",
  "context": "...",
  "plan": {
    "mode": "active_reconnaissance",
    "observer_source": "openaq_realtime",
    "expected_env_type": "air",
    "target_country": "US",
    "bbox": "-80.62,40.79,-80.42,40.91",
    "query": "(east palestine OR derailment OR evacuation)",
    "theme": "ENV_POLLUTION,ENV_CHEMICAL",
    "pollutant_type": "pm25,no2,o3",
    "timespan_hours": 24,
    "max_records": 250,
    "analyzer_mode": "llm",
    "analyzer_limit": 80,
    "reason": "...",
    "scenario": "ohio-chemical-leak"
  }
}
```

## 2) patrol/recon directive output
- Both return strict JSON with:
  - `mode`
  - `observer.params.source`
  - `window`
  - `observer.params`
  - `observer.commands[*].argv`
  - `listener.params`
  - `listener.commands[*].argv`
  - `analyzer.commands[*].argv` (always present; execution depends on orchestrator policy)

## 3) review output

```json
{
  "mode": "moderator_review",
  "decision": "switch_to_active_recon",
  "reason": "risk_signal_detected",
  "metrics": {
    "observer_exceed_rate": 0.31,
    "observer_exceeded": 14,
    "listener_upserted": 33,
    "caswarn_risk": 0.82
  },
  "thresholds": {
    "observer_exceed_rate_gte": 0.25,
    "observer_exceeded_gte": 8,
    "listener_upserted_gte": 20,
    "caswarn_risk_gte": 0.7
  },
  "next": {
    "method": "recon",
    "run_at": "immediate"
  }
}
```

## 4) orchestrate output

```json
{
  "mode": "moderator_orchestrate",
  "objective": "...",
  "planner": "llm",
  "dry_run": false,
  "cycles": [
    {
      "cycle": 1,
      "plan": {"mode": "passive_patrol", "...": "..."},
      "directive": {"observer": {"commands": []}, "listener": {"commands": []}},
      "execution": {
        "observer": [{"id": "observer_ingest", "returncode": 0, "status_line": "PHYSICAL_INGEST_OK ..."}],
        "listener": [{"id": "listener_summarize", "returncode": 0, "status_line": "SOCIAL_SUMMARIZE_OK ..."}],
        "analyzer": [{"id": "caswarn_analyze", "returncode": 0, "status_line": "[SUCCESS] ..."}],
        "eco_council": [
          {"id": "eco_council_ingest", "returncode": 0, "status_line": "ECO_COUNCIL_INGEST_OK ... ready_for_summary=True"},
          {"id": "eco_council_enrich", "returncode": 0, "status_line": "ECO_COUNCIL_ENRICH_OK ..."},
          {"id": "eco_council_summarize", "returncode": 0, "status_line": "ECO_COUNCIL_SUMMARY_OK ..."}
        ]
      },
      "review": {"decision": "switch_to_active_recon"},
      "report": {
        "status": {"observer": "...", "listener": "...", "analyzer": "..."},
        "parsed_status": {"observer": {}, "listener": {}, "analyzer": {}},
        "db_snapshot": {"observer": {}, "listener": {}},
        "eco_readiness": {"ready_for_summary": true, "status": "ready"}
      }
    }
  ],
  "final_decision": {"decision": "sleep"}
}
```

## 5) eco-council gating behavior
- `orchestrate` runs eco-council with fixed gate sequence:
  1. execute `eco_council_ingest`
  2. read `alignment_status.data_sufficiency.ready_for_summary` from ingest JSON
  3. run `enrich/summarize` only when ready
  4. if not ready, emit `ECO_COUNCIL_SKIP ...` status lines and set review to:
     - `decision=collect_more_data`
     - `reason=insufficient_aligned_data`
- Optional strictness flags:
  - `--require-geo-alignment` (default true)
  - `--require-category-alignment` (default true)

## Status line sources consumed by review
- Observer: `PHYSICAL_ENRICH_OK ... exceeded=...` or `PHYSICAL_SUMMARY_OK ...`
- Observer guard path: `PHYSICAL_SKIP reason=unsupported_env_type ...`
- Listener: `SOCIAL_SUMMARIZE_OK ... upserted=...`
- Analyzer: `[SUCCESS] ... {"nimby_risk_score":...}`
- Any line containing `ERR` leads to `retry_or_manual_check`.
- Eco council readiness directly affects escalation: not-ready -> `collect_more_data`.

## LLM config
- `plan-llm` / `orchestrate --planner llm` load config in this order:
  1. `listener-gdelt-doc-ingestor/assets/config.env`
  2. `moderator-observer-listener-orchestrator/assets/config.env`
  3. `moderator-observer-listener-orchestrator/assets/config.json`
- You can override with `--config-env` and `--config-json`.
