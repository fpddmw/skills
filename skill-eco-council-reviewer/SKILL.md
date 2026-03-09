---
name: skill-eco-council-reviewer
description: 聚合生态事件下的 SQLite 状态（物理事实与社会舆论），将结构化 JSON 转换为 OpenAI/Claude 推理输入并执行逻辑对齐检查，最终输出终端可读 Markdown 简报。用于 moderator 需要生成事件级“生态议会共识研判”报告时。
---

# Skill Eco Council Reviewer

## Goal
- Serve as the terminal reporting layer in the moderator skill group.
- Keep a fixed 3-stage workflow: `ingest -> enrich -> summarize`.
- Mark missing modules explicitly: historical experience and projection curve are placeholders for future skills.

## Inputs
- Observer SQLite DB (`physical_metrics` expected).
- Listener/Analyzer SQLite DB (`social_events` expected).
- Event identity (`--event-id`) and optional UTC window.
- Moderator task profile metadata:
  - `--observer-source`
  - `--target-bbox`
  - `--target-country`
  - `--expected-env-type`
  - sufficiency thresholds: `--min-physical-rows`, `--min-social-rows`

## Workflow
1. Ingest structured state into JSON.
2. Enrich with LLM logical alignment checks (OpenAI/Claude) or rule fallback.
3. Summarize into a complete Markdown briefing.

## Commands

```bash
python3 scripts/eco_council_report.py ingest \
  --event-id evt-20260309-001 \
  --observer-db /abs/path/observer_physical.db \
  --listener-db /abs/path/gdelt_environment.db \
  --start-datetime 2026-03-01T00:00:00Z \
  --end-datetime 2026-03-03T00:00:00Z \
  --output-json /tmp/eco_ingest.json
```

```bash
# provider=auto: OPENAI_API_KEY -> ANTHROPIC_API_KEY -> rule fallback
python3 scripts/eco_council_report.py enrich \
  --ingest-json /tmp/eco_ingest.json \
  --provider auto \
  --output-json /tmp/eco_enrich.json
```

```bash
python3 scripts/eco_council_report.py summarize \
  --ingest-json /tmp/eco_ingest.json \
  --enrich-json /tmp/eco_enrich.json \
  --output-md /tmp/eco_brief.md
```

## Environment
- OpenAI path:
  - `OPENAI_API_KEY` (required)
  - `OPENAI_MODEL` (optional, default `gpt-4.1-mini`)
  - `OPENAI_BASE_URL` (optional, default `https://api.openai.com/v1`)
- Claude path:
  - `ANTHROPIC_API_KEY` (required)
  - `ANTHROPIC_MODEL` (optional, default `claude-3-5-sonnet-latest`)
  - `ANTHROPIC_BASE_URL` (optional, default `https://api.anthropic.com/v1`)
- Compatibility fallback:
  - `LLM_API_KEY` / `LLM_MODEL` can be reused by OpenAI-compatible endpoint.

## Output Contract
- Ingest output: event-level structured JSON with four state blocks:
  - `physical_facts`
  - `social_opinion`
  - `task_profile`
  - `data_labels` (region/category labels)
  - `alignment_status` (geographic/category/sufficiency gate)
  - `historical_experience` (`pending_skill`)
  - `projection_curve` (`pending_skill`)
- Enrich output: LLM-aligned JSON with:
  - `executive_summary`
  - `evidence_alignment`
  - `key_risks`
  - `key_actions`
  - `uncertainty_and_gaps`
  - `confidence`
  - `report_markdown`
- Summarize output: full Markdown briefing for terminal delivery.
  - includes stage descriptions and alignment-status section for moderator/human review.

## Resources
- `scripts/eco_council_report.py`
- `references/data-contract.md`
