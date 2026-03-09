#!/usr/bin/env python3
"""Build eco-council terminal report via ingest/enrich/summarize pipeline."""

from __future__ import annotations

import argparse
import copy
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_TIMEOUT = 30.0
DEFAULT_USER_AGENT = "skill-eco-council-reviewer/1.0"
DEFAULT_CONFIG_ENV = "moderator-observer-listener-orchestrator/assets/config.env"
DEFAULT_CONFIG_JSON = "moderator-observer-listener-orchestrator/assets/config.json"
DEFAULT_LISTENER_ENV = "listener-gdelt-doc-ingestor/assets/config.env"
COUNTRY_ALIASES = {
    "US": {"US", "USA", "UNITED STATES", "UNITED STATES OF AMERICA", "AMERICA"},
    "CN": {"CN", "CHINA", "PRC", "PEOPLE'S REPUBLIC OF CHINA"},
    "JP": {"JP", "JAPAN"},
    "TH": {"TH", "THAILAND"},
    "IN": {"IN", "INDIA"},
}
SOCIAL_DOMAIN_KEYWORDS = {
    "air": ("air", "smog", "pm2.5", "ozone", "no2"),
    "water": ("water", "wastewater", "river", "sewage", "ocean", "marine"),
    "soil": ("soil", "landfill", "contaminated land", "ground"),
    "radiation": ("radiation", "radioactive", "nuclear"),
    "waste": ("waste", "garbage", "incinerator", "dump"),
}


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def ensure_parent(path: Path) -> None:
    if path.parent and str(path.parent) not in ("", "."):
        path.parent.mkdir(parents=True, exist_ok=True)


def _parse_env_line(line: str) -> tuple[str, str] | None:
    raw = normalize_text(line)
    if not raw or raw.startswith("#") or "=" not in raw:
        return None
    key, value = raw.split("=", 1)
    key = normalize_text(key)
    value = value.strip().strip("'").strip('"')
    if not key:
        return None
    return key, value


def load_env_file(path: str) -> int:
    file_path = Path(path).expanduser()
    if not file_path.exists():
        return 0
    loaded = 0
    for line in file_path.read_text(encoding="utf-8").splitlines():
        parsed = _parse_env_line(line)
        if not parsed:
            continue
        key, value = parsed
        if key not in os.environ and value:
            os.environ[key] = value
            loaded += 1
    return loaded


def load_json_config(path: str) -> dict[str, Any]:
    file_path = Path(path).expanduser()
    if not file_path.exists():
        return {}
    parsed = json.loads(file_path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError(f"--config-json must be an object: {path}")
    return parsed


def load_runtime_config(config_env: str, config_json: str) -> dict[str, Any]:
    # Keep consistent with moderator/listener loading order.
    load_env_file(DEFAULT_LISTENER_ENV)
    load_env_file(normalize_text(config_env) or DEFAULT_CONFIG_ENV)
    config = load_json_config(normalize_text(config_json) or DEFAULT_CONFIG_JSON)

    # Reuse moderator/listener JSON key format.
    if "LLM_API_BASE_URL" not in os.environ:
        value = normalize_text(config.get("llm_api_base_url") or "")
        if value:
            os.environ["LLM_API_BASE_URL"] = value
    if "LLM_API_KEY" not in os.environ:
        value = normalize_text(config.get("llm_api_key") or "")
        if value:
            os.environ["LLM_API_KEY"] = value
    if "LLM_MODEL" not in os.environ:
        value = normalize_text(config.get("llm_model") or "")
        if value:
            os.environ["LLM_MODEL"] = value
    return config


def load_json(path: str) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"expected JSON object: {path}")
    return data


def dump_json(path: str, payload: dict[str, Any]) -> None:
    output = Path(path)
    ensure_parent(output)
    with output.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def parse_iso_date(value: str) -> str:
    raw = normalize_text(value)
    if not raw:
        return ""
    candidate = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError(f"invalid ISO datetime: {raw}") from exc
    return dt.date().isoformat()


def parse_utc_compact(value: str) -> str:
    raw = normalize_text(value)
    if not raw:
        return ""
    if len(raw) == 14 and raw.isdigit():
        return raw
    candidate = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError(f"invalid datetime for social window: {raw}") from exc
    return dt.strftime("%Y%m%d%H%M%S")


def fetch_physical_snapshot(
    db_path: str,
    *,
    start_datetime: str,
    end_datetime: str,
    metric_limit: int,
) -> dict[str, Any]:
    output: dict[str, Any] = {
        "db_path": db_path,
        "table_present": False,
        "rows_in_scope": 0,
        "top_country_exceedance": [],
        "top_parameters": [],
        "recent_metrics": [],
    }
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        if not table_exists(conn, "physical_metrics"):
            return output

        output["table_present"] = True

        where = []
        params: list[Any] = []
        start_date = parse_iso_date(start_datetime)
        end_date = parse_iso_date(end_datetime)
        if start_date:
            where.append("metric_date >= ?")
            params.append(start_date)
        if end_date:
            where.append("metric_date <= ?")
            params.append(end_date)
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        row = conn.execute(
            f"SELECT COUNT(*) AS c FROM physical_metrics {where_sql}",
            params,
        ).fetchone()
        output["rows_in_scope"] = int(row["c"] if row else 0)

        country_rows = conn.execute(
            f"""
            SELECT
                country_code,
                COUNT(*) AS metric_rows,
                ROUND(AVG(exceed_rate), 4) AS avg_exceed_rate,
                ROUND(MAX(max_variance_ratio), 4) AS max_variance_ratio
            FROM physical_metrics
            {where_sql}
            GROUP BY country_code
            ORDER BY avg_exceed_rate DESC, metric_rows DESC
            LIMIT 8
            """,
            params,
        ).fetchall()
        output["top_country_exceedance"] = [dict(r) for r in country_rows]

        parameter_rows = conn.execute(
            f"""
            SELECT
                parameter_code,
                COUNT(*) AS metric_rows,
                ROUND(AVG(avg_value_ugm3), 4) AS avg_value_ugm3,
                ROUND(AVG(exceed_rate), 4) AS avg_exceed_rate
            FROM physical_metrics
            {where_sql}
            GROUP BY parameter_code
            ORDER BY metric_rows DESC
            LIMIT 8
            """,
            params,
        ).fetchall()
        output["top_parameters"] = [dict(r) for r in parameter_rows]

        recent_rows = conn.execute(
            f"""
            SELECT
                metric_date,
                country_code,
                parameter_code,
                standard_profile,
                sample_count,
                avg_value_ugm3,
                max_value_ugm3,
                exceed_rate,
                max_variance_ratio
            FROM physical_metrics
            {where_sql}
            ORDER BY metric_date DESC, id DESC
            LIMIT ?
            """,
            [*params, metric_limit],
        ).fetchall()
        output["recent_metrics"] = [dict(r) for r in recent_rows]
        return output
    finally:
        conn.close()


def fetch_social_snapshot(
    db_path: str,
    *,
    start_datetime: str,
    end_datetime: str,
    event_limit: int,
) -> dict[str, Any]:
    output: dict[str, Any] = {
        "db_path": db_path,
        "table_present": False,
        "rows_in_scope": 0,
        "env_relevance_distribution": {},
        "sarf_distribution": {},
        "risk_summary": {},
        "top_domains": [],
        "recent_events": [],
    }
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        if not table_exists(conn, "social_events"):
            return output

        output["table_present"] = True

        where = []
        params: list[Any] = []
        seen_norm = (
            "substr("
            "replace(replace(replace(replace(COALESCE(seendate_utc, ''), 'T', ''), 'Z', ''), '-', ''), ':', ''),"
            "1, 14)"
        )
        start_utc = parse_utc_compact(start_datetime)
        end_utc = parse_utc_compact(end_datetime)
        if start_utc:
            where.append(f"{seen_norm} >= ?")
            params.append(start_utc)
        if end_utc:
            where.append(f"{seen_norm} <= ?")
            params.append(end_utc)
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        row = conn.execute(
            f"SELECT COUNT(*) AS c FROM social_events {where_sql}",
            params,
        ).fetchone()
        output["rows_in_scope"] = int(row["c"] if row else 0)

        env_rows = conn.execute(
            f"""
            SELECT COALESCE(env_relevance, 0) AS env_relevance, COUNT(*) AS c
            FROM social_events
            {where_sql}
            GROUP BY COALESCE(env_relevance, 0)
            """,
            params,
        ).fetchall()
        output["env_relevance_distribution"] = {
            str(int(r["env_relevance"])): int(r["c"]) for r in env_rows
        }

        sarf_rows = conn.execute(
            f"""
            SELECT COALESCE(sarf_label, 'unknown') AS sarf_label, COUNT(*) AS c
            FROM social_events
            {where_sql}
            GROUP BY COALESCE(sarf_label, 'unknown')
            ORDER BY c DESC
            """,
            params,
        ).fetchall()
        output["sarf_distribution"] = {str(r["sarf_label"]): int(r["c"]) for r in sarf_rows}

        risk_row = conn.execute(
            f"""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN COALESCE(is_analyzed, 0)=1 THEN 1 ELSE 0 END) AS analyzed_count,
                ROUND(AVG(nimby_risk_score), 4) AS avg_risk_score,
                ROUND(MAX(nimby_risk_score), 4) AS max_risk_score
            FROM social_events
            {where_sql}
            """,
            params,
        ).fetchone()
        output["risk_summary"] = {
            "total": int(risk_row["total"] or 0),
            "analyzed_count": int(risk_row["analyzed_count"] or 0),
            "avg_risk_score": float(risk_row["avg_risk_score"] or 0.0),
            "max_risk_score": float(risk_row["max_risk_score"] or 0.0),
        }

        domain_rows = conn.execute(
            f"""
            SELECT COALESCE(source_domain, 'unknown') AS source_domain, COUNT(*) AS c
            FROM social_events
            {where_sql}
            GROUP BY COALESCE(source_domain, 'unknown')
            ORDER BY c DESC
            LIMIT 8
            """,
            params,
        ).fetchall()
        output["top_domains"] = [dict(r) for r in domain_rows]

        recent_rows = conn.execute(
            f"""
            SELECT
                id,
                seendate_utc,
                title,
                source_country,
                source_domain,
                env_relevance,
                avg_tone,
                goldstein_scale,
                sarf_label,
                dominant_emotion,
                nimby_risk_score,
                risk_frame
            FROM social_events
            {where_sql}
            ORDER BY seendate_utc DESC, id DESC
            LIMIT ?
            """,
            [*params, event_limit],
        ).fetchall()
        output["recent_events"] = [dict(r) for r in recent_rows]
        return output
    finally:
        conn.close()


def build_prompt(ingest_payload: dict[str, Any]) -> str:
    payload_text = json.dumps(ingest_payload, ensure_ascii=False, indent=2)
    return (
        "你是生态议会终端报告审阅员。请基于给定 JSON 进行思辨与逻辑对齐检查，"
        "生成结构化 JSON，不要输出其他文本。\n"
        "要求:\n"
        "1) 明确物理事实与社会舆论之间的一致/冲突点。\n"
        "2) 对证据强度与不确定性做分级。\n"
        "3) 产出可执行的决策建议。\n"
        "4) 历史经验/推演曲线当前缺失时，必须标注数据缺口影响。\n"
        "输出 JSON 键:\n"
        "- executive_summary: string\n"
        "- evidence_alignment: array of {point, status, rationale}\n"
        "- key_risks: array of string\n"
        "- key_actions: array of string\n"
        "- uncertainty_and_gaps: array of string\n"
        "- confidence: {level, reason}\n"
        "- report_markdown: string (完整 markdown 简报正文，不含 ``` 代码块)\n\n"
        f"输入 JSON:\n{payload_text}\n"
    )


def compact_ingest_payload(ingest_payload: dict[str, Any]) -> dict[str, Any]:
    compact = copy.deepcopy(ingest_payload)
    physical = compact.get("physical_facts")
    if isinstance(physical, dict):
        recent = physical.get("recent_metrics")
        if isinstance(recent, list):
            physical["recent_metrics"] = recent[:12]
    social = compact.get("social_opinion")
    if isinstance(social, dict):
        recent = social.get("recent_events")
        if isinstance(recent, list):
            social["recent_events"] = recent[:12]
    return compact


def api_json(
    url: str,
    *,
    headers: dict[str, str],
    body: dict[str, Any],
    timeout: float,
) -> dict[str, Any]:
    req = Request(
        url=url,
        method="POST",
        headers={"User-Agent": DEFAULT_USER_AGENT, **headers},
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
    )
    try:
        with urlopen(req, timeout=timeout) as resp:
            text = resp.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"http_error status={exc.code} detail={detail[:500]}") from exc
    except URLError as exc:
        raise RuntimeError(f"network_error detail={exc.reason}") from exc
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("invalid_json_response") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("unexpected_response_shape")
    return payload


def parse_json_string(content: str) -> dict[str, Any]:
    parsed = json.loads(content)
    if not isinstance(parsed, dict):
        raise RuntimeError("llm_content_not_object")
    return parsed


def extract_responses_text(payload: dict[str, Any]) -> str:
    output = payload.get("output")
    if not isinstance(output, list):
        raise RuntimeError("responses_missing_output")
    chunks: list[str] = []
    for item in output:
        if not isinstance(item, dict):
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict):
                continue
            # Common shape for Responses API text segments.
            text = part.get("text")
            if isinstance(text, str) and text.strip():
                chunks.append(text)
                continue
            # Some providers wrap text under a nested object.
            text_obj = part.get("text")
            if isinstance(text_obj, dict):
                value = text_obj.get("value")
                if isinstance(value, str) and value.strip():
                    chunks.append(value)
    merged = "\n".join(chunks).strip()
    if not merged:
        raise RuntimeError("responses_empty_text")
    return merged


def call_openai(prompt: str, *, timeout: float) -> dict[str, Any]:
    base_url = normalize_text(
        os.environ.get("OPENAI_BASE_URL") or os.environ.get("LLM_API_BASE_URL") or "https://api.openai.com/v1"
    ).rstrip("/")
    api_key = normalize_text(os.environ.get("OPENAI_API_KEY") or os.environ.get("LLM_API_KEY") or "")
    model = normalize_text(os.environ.get("OPENAI_MODEL") or os.environ.get("LLM_MODEL") or "gpt-4.1-mini")
    if not api_key:
        raise RuntimeError("missing OPENAI_API_KEY")

    try:
        response = api_json(
            f"{base_url}/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            timeout=timeout,
            body={
                "model": model,
                "messages": [
                    {"role": "system", "content": "Return JSON only."},
                    {"role": "user", "content": prompt},
                ],
                "response_format": {"type": "json_object"},
            },
        )
        choices = response.get("choices")
        if not isinstance(choices, list) or not choices:
            raise RuntimeError("openai_invalid_choices")
        message = choices[0].get("message") if isinstance(choices[0], dict) else None
        content = message.get("content") if isinstance(message, dict) else None
        if not isinstance(content, str) or not content.strip():
            raise RuntimeError("openai_empty_content")
        payload = parse_json_string(content)
    except RuntimeError:
        # Fallback to Responses API for newer model families (including GPT-5).
        response = api_json(
            f"{base_url}/responses",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            timeout=timeout,
            body={
                "model": model,
                "input": [
                    {"role": "system", "content": [{"type": "input_text", "text": "Return JSON only."}]},
                    {"role": "user", "content": [{"type": "input_text", "text": prompt}]},
                ],
            },
        )
        payload = parse_json_string(extract_responses_text(response))
    payload["provider"] = "openai"
    payload["model"] = model
    return payload


def call_claude(prompt: str, *, timeout: float) -> dict[str, Any]:
    base_url = normalize_text(os.environ.get("ANTHROPIC_BASE_URL") or "https://api.anthropic.com/v1").rstrip("/")
    api_key = normalize_text(os.environ.get("ANTHROPIC_API_KEY") or "")
    model = normalize_text(os.environ.get("ANTHROPIC_MODEL") or "claude-3-5-sonnet-latest")
    if not api_key:
        raise RuntimeError("missing ANTHROPIC_API_KEY")

    response = api_json(
        f"{base_url}/messages",
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        timeout=timeout,
        body={
            "model": model,
            "max_tokens": 2200,
            "temperature": 0.2,
            "system": "Return JSON only.",
            "messages": [{"role": "user", "content": prompt}],
        },
    )
    content = response.get("content")
    if not isinstance(content, list) or not content:
        raise RuntimeError("claude_invalid_content")
    text_chunks = [item.get("text", "") for item in content if isinstance(item, dict)]
    merged = "\n".join(chunk for chunk in text_chunks if chunk)
    if not merged.strip():
        raise RuntimeError("claude_empty_content")
    payload = parse_json_string(merged)
    payload["provider"] = "claude"
    payload["model"] = model
    return payload


def run_llm_with_retry(provider: str, prompt: str, *, timeout: float, max_attempts: int) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, max(1, max_attempts) + 1):
        try:
            if provider == "openai":
                return call_openai(prompt, timeout=timeout)
            if provider == "claude":
                return call_claude(prompt, timeout=timeout)
            raise RuntimeError(f"unsupported_provider:{provider}")
        except Exception as exc:  # pylint: disable=broad-except
            last_error = exc
            if attempt >= max_attempts:
                break
            time.sleep(min(8.0, 1.5 * attempt))
    raise RuntimeError(f"llm_retry_exhausted detail={last_error}")


def fallback_rule_analysis(ingest_payload: dict[str, Any]) -> dict[str, Any]:
    physical = ingest_payload.get("physical_facts") or {}
    social = ingest_payload.get("social_opinion") or {}
    risk_summary = social.get("risk_summary") if isinstance(social, dict) else {}
    avg_risk = 0.0
    if isinstance(risk_summary, dict):
        try:
            avg_risk = float(risk_summary.get("avg_risk_score") or 0.0)
        except Exception:
            avg_risk = 0.0

    status = "aligned" if avg_risk < 0.4 else "tension"
    summary = "社会风险温和，物理与舆情整体可对齐。" if status == "aligned" else "社会风险偏高，需加强物理-舆情证据对齐。"

    report_markdown = (
        "## 生态议会共识初稿（规则回退）\n"
        f"- 结论: {summary}\n"
        "- 说明: 当前未接入 LLM，以下为规则回退结果。\n"
    )
    return {
        "provider": "rule",
        "model": "rule-fallback",
        "executive_summary": summary,
        "evidence_alignment": [
            {
                "point": "物理事实 vs 社会舆论",
                "status": status,
                "rationale": "依据 nimby_risk_score 均值进行粗粒度判断。",
            }
        ],
        "key_risks": ["历史经验与推演曲线尚未接入，结论置信度受限。"],
        "key_actions": ["补齐历史经验库与推演曲线 skill，再做联动复核。"],
        "uncertainty_and_gaps": ["目前仅使用物理事实与社会舆论两类状态。"],
        "confidence": {
            "level": "medium" if status == "aligned" else "low",
            "reason": "规则回退未进行深度语义推理。",
        },
        "report_markdown": report_markdown,
    }


def render_markdown(
    ingest_payload: dict[str, Any],
    enrich_payload: dict[str, Any],
    *,
    title: str,
) -> str:
    event_id = ingest_payload.get("event_id", "unknown")
    generated_at = now_iso()

    social = ingest_payload.get("social_opinion") if isinstance(ingest_payload, dict) else {}
    physical = ingest_payload.get("physical_facts") if isinstance(ingest_payload, dict) else {}
    task_profile = ingest_payload.get("task_profile") if isinstance(ingest_payload, dict) else {}
    alignment = ingest_payload.get("alignment_status") if isinstance(ingest_payload, dict) else {}

    executive_summary = normalize_text(enrich_payload.get("executive_summary") or "")
    report_markdown = str(enrich_payload.get("report_markdown") or "").strip()

    lines = [
        f"# {title}",
        "",
        f"- Event ID: `{event_id}`",
        f"- Generated At (UTC): `{generated_at}`",
        f"- Enrich Provider: `{enrich_payload.get('provider', 'unknown')}`",
        f"- Enrich Model: `{enrich_payload.get('model', 'unknown')}`",
        "",
        "## 阶段说明",
        "1. `Ingest`: 聚合 Observer/Listener SQLite 状态，形成事件级结构化输入。",
        "2. `Enrich`: 调用大模型进行证据对齐、风险辨析与建议生成。",
        "3. `Summarize`: 产出终端可读 Markdown，供 moderator 与人工快速复核。",
        "",
        "## 任务画像",
        f"- Observer Source: `{(task_profile or {}).get('observer_source', 'unknown')}`",
        f"- 目标区域 bbox: `{(task_profile or {}).get('target_bbox', '') or 'n/a'}`",
        f"- 目标国家: `{(task_profile or {}).get('target_country', '') or 'n/a'}`",
        f"- 期望环境类别: `{(task_profile or {}).get('expected_env_type', '') or 'general'}`",
        "",
        "## 对齐状态",
        f"- 地域对齐: `{((alignment or {}).get('geographic_alignment') or {}).get('status', 'unknown')}` - {((alignment or {}).get('geographic_alignment') or {}).get('reason', '')}",
        f"- 类别对齐: `{((alignment or {}).get('category_alignment') or {}).get('status', 'unknown')}` - {((alignment or {}).get('category_alignment') or {}).get('reason', '')}",
        f"- 数据充足: `{((alignment or {}).get('data_sufficiency') or {}).get('status', 'unknown')}` - {json.dumps(((alignment or {}).get('data_sufficiency') or {}).get('reasons', []), ensure_ascii=False)}",
        "",
        "## 执行摘要",
        executive_summary or "（无）",
        "",
        "## 1) 物理事实（Observer）",
        f"- 数据行数: `{physical.get('rows_in_scope', 0)}`",
        f"- 主要污染参数: `{', '.join(str((item or {}).get('parameter_code', 'n/a')) for item in (physical.get('top_parameters') or [])[:5]) or 'n/a'}`",
        "",
        "## 2) 社会舆论（Listener/Analyzer）",
        f"- 数据行数: `{social.get('rows_in_scope', 0)}`",
        f"- SARF分布: `{json.dumps(social.get('sarf_distribution') or {}, ensure_ascii=False)}`",
        f"- 风险统计: `{json.dumps(social.get('risk_summary') or {}, ensure_ascii=False)}`",
        "",
        "## 3) 历史经验与推演曲线",
        "- 当前状态: `未接入`",
        "- 影响: 对趋势可解释性与反事实检验能力存在缺口。",
        "",
        "## 4) 逻辑对齐与共识研判",
    ]

    evidence_alignment = enrich_payload.get("evidence_alignment")
    if isinstance(evidence_alignment, list) and evidence_alignment:
        for item in evidence_alignment[:10]:
            if not isinstance(item, dict):
                continue
            lines.append(
                f"- {normalize_text(item.get('point'))}: `{normalize_text(item.get('status'))}` - {normalize_text(item.get('rationale'))}"
            )
    else:
        lines.append("- （无结构化对齐点）")

    lines.extend([
        "",
        "## 5) 建议行动",
    ])
    key_actions = enrich_payload.get("key_actions")
    if isinstance(key_actions, list) and key_actions:
        for action in key_actions[:10]:
            lines.append(f"- {normalize_text(action)}")
    else:
        lines.append("- （无）")

    lines.extend([
        "",
        "## 6) 不确定性与数据缺口",
    ])
    gaps = enrich_payload.get("uncertainty_and_gaps")
    if isinstance(gaps, list) and gaps:
        for gap in gaps[:10]:
            lines.append(f"- {normalize_text(gap)}")
    else:
        lines.append("- （无）")

    if report_markdown:
        lines.extend([
            "",
            "## 7) LLM 原始简报正文",
            report_markdown,
        ])

    return "\n".join(lines).strip() + "\n"


def cmd_ingest(args: argparse.Namespace) -> int:
    physical_facts = fetch_physical_snapshot(
        args.observer_db,
        start_datetime=args.start_datetime,
        end_datetime=args.end_datetime,
        metric_limit=max(1, args.metric_limit),
    )
    social_opinion = fetch_social_snapshot(
        args.listener_db,
        start_datetime=args.start_datetime,
        end_datetime=args.end_datetime,
        event_limit=max(1, args.event_limit),
    )

    physical_rows = int(physical_facts.get("rows_in_scope") or 0)
    social_rows = int(social_opinion.get("rows_in_scope") or 0)
    physical_countries = sorted(
        {
            normalize_country_token(str(item.get("country_code") or ""))
            for item in (physical_facts.get("top_country_exceedance") or [])
            if normalize_country_token(str(item.get("country_code") or ""))
        }
    )
    social_countries = sorted(
        {
            normalize_country_token(str(item.get("source_country") or ""))
            for item in (social_opinion.get("recent_events") or [])
            if normalize_country_token(str(item.get("source_country") or ""))
        }
    )
    social_domains = infer_social_domains(social_opinion.get("recent_events") or [])
    alignment_status = compute_alignment_status(
        physical_rows=physical_rows,
        social_rows=social_rows,
        physical_countries=physical_countries,
        social_countries=social_countries,
        social_domains=social_domains,
        target_country=args.target_country,
        target_bbox=args.target_bbox,
        expected_env_type=args.expected_env_type,
        min_physical_rows=max(0, args.min_physical_rows),
        min_social_rows=max(0, args.min_social_rows),
    )

    payload = {
        "event_id": normalize_text(args.event_id) or "unknown-event",
        "generated_at": now_iso(),
        "task_profile": {
            "observer_source": normalize_text(args.observer_source) or "unknown",
            "target_bbox": normalize_text(args.target_bbox),
            "target_country": normalize_text(args.target_country),
            "expected_env_type": normalize_text(args.expected_env_type) or "general",
        },
        "window": {
            "start_datetime": normalize_text(args.start_datetime),
            "end_datetime": normalize_text(args.end_datetime),
        },
        "physical_facts": physical_facts,
        "social_opinion": social_opinion,
        "data_labels": {
            "physical": {
                "domain": "air",
                "data_types": ["pm25", "no2", "o3"],
                "countries": physical_countries,
            },
            "social": {
                "domains_detected": social_domains,
                "countries": social_countries,
            },
        },
        "alignment_status": alignment_status,
        "historical_experience": {
            "status": "pending_skill",
            "note": "历史经验技能尚未接入。",
        },
        "projection_curve": {
            "status": "pending_skill",
            "note": "推演曲线技能尚未接入。",
        },
    }
    dump_json(args.output_json, payload)
    print(
        f"ECO_COUNCIL_INGEST_OK event_id={payload['event_id']} output={args.output_json} "
        f"physical_rows={payload['physical_facts']['rows_in_scope']} "
        f"social_rows={payload['social_opinion']['rows_in_scope']} "
        f"ready_for_summary={payload['alignment_status']['data_sufficiency']['ready_for_summary']}"
    )
    return 0


def resolve_provider(provider: str) -> str:
    p = normalize_text(provider).lower()
    if p in {"openai", "claude", "rule"}:
        return p
    if p != "auto":
        raise ValueError("--provider must be one of auto,openai,claude,rule")
    if normalize_text(os.environ.get("OPENAI_API_KEY") or os.environ.get("LLM_API_KEY") or ""):
        return "openai"
    if normalize_text(os.environ.get("ANTHROPIC_API_KEY") or ""):
        return "claude"
    return "rule"


def should_skip_llm(ingest_payload: dict[str, Any]) -> bool:
    physical = ingest_payload.get("physical_facts")
    social = ingest_payload.get("social_opinion")
    physical_rows = 0
    social_rows = 0
    if isinstance(physical, dict):
        try:
            physical_rows = int(physical.get("rows_in_scope") or 0)
        except Exception:
            physical_rows = 0
    if isinstance(social, dict):
        try:
            social_rows = int(social.get("rows_in_scope") or 0)
        except Exception:
            social_rows = 0
    return physical_rows <= 0 and social_rows <= 0


def normalize_country_token(raw: str) -> str:
    text = normalize_text(raw).upper()
    if not text:
        return ""
    for code, aliases in COUNTRY_ALIASES.items():
        if text in aliases:
            return code
    return text


def infer_social_domains(events: list[dict[str, Any]]) -> list[str]:
    detected: set[str] = set()
    for event in events:
        if not isinstance(event, dict):
            continue
        text = " ".join(
            [
                normalize_text(event.get("title") or ""),
                normalize_text(event.get("risk_frame") or ""),
            ]
        ).lower()
        for domain, keywords in SOCIAL_DOMAIN_KEYWORDS.items():
            if any(word in text for word in keywords):
                detected.add(domain)
    return sorted(detected)


def compute_alignment_status(
    *,
    physical_rows: int,
    social_rows: int,
    physical_countries: list[str],
    social_countries: list[str],
    social_domains: list[str],
    target_country: str,
    target_bbox: str,
    expected_env_type: str,
    min_physical_rows: int,
    min_social_rows: int,
) -> dict[str, Any]:
    physical_domain = "air"
    expected = normalize_text(expected_env_type).lower() or "general"
    target_country_norm = normalize_country_token(target_country)

    geo_status = "unknown"
    geo_reason = "country labels missing"
    if target_country_norm:
        if target_country_norm in social_countries and physical_rows > 0 and normalize_text(target_bbox):
            geo_status = "aligned"
            geo_reason = "social countries include target country and physical bbox sampling exists"
        elif social_countries and target_country_norm not in social_countries:
            geo_status = "mismatch"
            geo_reason = "social countries do not include target country"
        elif physical_rows > 0 and normalize_text(target_bbox):
            geo_status = "partial"
            geo_reason = "physical bbox exists but social country evidence is weak"
    elif physical_countries and social_countries:
        overlap = sorted(set(physical_countries).intersection(social_countries))
        if overlap:
            geo_status = "aligned"
            geo_reason = f"country overlap={overlap}"
        else:
            geo_status = "mismatch"
            geo_reason = "no overlap between physical and social countries"

    category_status = "unknown"
    category_reason = "expected environment type not provided"
    if expected in {"air", "water", "soil", "radiation", "waste"}:
        if expected != physical_domain:
            category_status = "mismatch"
            category_reason = f"observer currently provides {physical_domain} data only"
        elif social_domains and expected not in social_domains:
            category_status = "partial"
            category_reason = f"social domains={social_domains}, expected={expected}"
        else:
            category_status = "aligned"
            category_reason = f"observer domain={physical_domain}, expected={expected}"
    elif expected in {"general", "multi"}:
        category_status = "partial" if social_domains else "unknown"
        category_reason = "general/multi objective requires manual domain confirmation"

    ready = True
    reasons: list[str] = []
    if physical_rows < min_physical_rows:
        ready = False
        reasons.append(f"physical_rows<{min_physical_rows}")
    if social_rows < min_social_rows:
        ready = False
        reasons.append(f"social_rows<{min_social_rows}")
    if category_status == "mismatch":
        ready = False
        reasons.append("category_mismatch")
    if geo_status == "mismatch":
        ready = False
        reasons.append("geographic_mismatch")

    return {
        "geographic_alignment": {"status": geo_status, "reason": geo_reason},
        "category_alignment": {"status": category_status, "reason": category_reason},
        "data_sufficiency": {
            "ready_for_summary": ready,
            "status": "ready" if ready else "insufficient",
            "reasons": reasons,
            "counts": {"physical_rows": physical_rows, "social_rows": social_rows},
            "thresholds": {"min_physical_rows": min_physical_rows, "min_social_rows": min_social_rows},
        },
    }


def cmd_enrich(args: argparse.Namespace) -> int:
    load_runtime_config(args.config_env, args.config_json)
    ingest_payload = load_json(args.ingest_json)
    prompt_payload = compact_ingest_payload(ingest_payload) if args.compact_prompt else ingest_payload
    prompt = build_prompt(prompt_payload)
    provider = resolve_provider(args.provider)

    if args.skip_llm_when_empty and should_skip_llm(ingest_payload):
        result = fallback_rule_analysis(ingest_payload)
        result["skip_llm_reason"] = "empty_input_rows"
    elif provider == "openai":
        result = run_llm_with_retry("openai", prompt, timeout=args.timeout, max_attempts=args.retry)
    elif provider == "claude":
        result = run_llm_with_retry("claude", prompt, timeout=args.timeout, max_attempts=args.retry)
    else:
        result = fallback_rule_analysis(ingest_payload)

    result["event_id"] = ingest_payload.get("event_id", "unknown-event")
    result["generated_at"] = now_iso()
    result["input_file"] = args.ingest_json
    dump_json(args.output_json, result)
    print(f"ECO_COUNCIL_ENRICH_OK provider={result.get('provider')} output={args.output_json}")
    return 0


def cmd_summarize(args: argparse.Namespace) -> int:
    ingest_payload = load_json(args.ingest_json)
    enrich_payload = load_json(args.enrich_json)
    markdown = render_markdown(ingest_payload, enrich_payload, title=args.title)

    output = Path(args.output_md)
    ensure_parent(output)
    output.write_text(markdown, encoding="utf-8")

    print(f"ECO_COUNCIL_SUMMARY_OK output={args.output_md} bytes={len(markdown.encode('utf-8'))}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate eco council consensus report from observer/listener SQLite states."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_ingest = subparsers.add_parser("ingest", help="Aggregate structured event state from SQLite DBs.")
    p_ingest.add_argument("--event-id", required=True, help="Event identifier for this report run.")
    p_ingest.add_argument("--observer-db", required=True, help="Path to observer SQLite DB.")
    p_ingest.add_argument("--listener-db", required=True, help="Path to listener/analyzer SQLite DB.")
    p_ingest.add_argument("--start-datetime", default="", help="Optional UTC lower bound (ISO-8601).")
    p_ingest.add_argument("--end-datetime", default="", help="Optional UTC upper bound (ISO-8601).")
    p_ingest.add_argument("--observer-source", default="unknown", help="Observer source selected by moderator.")
    p_ingest.add_argument("--target-bbox", default="", help="Target bbox expected for event localization.")
    p_ingest.add_argument("--target-country", default="", help="Target country code/name expected by scenario.")
    p_ingest.add_argument("--expected-env-type", default="general", help="Expected env type: air/water/soil/radiation/waste/general.")
    p_ingest.add_argument("--min-physical-rows", type=int, default=6, help="Minimum physical rows required for summary.")
    p_ingest.add_argument("--min-social-rows", type=int, default=4, help="Minimum social rows required for summary.")
    p_ingest.add_argument("--metric-limit", type=int, default=20, help="Recent physical metrics to include.")
    p_ingest.add_argument("--event-limit", type=int, default=20, help="Recent social events to include.")
    p_ingest.add_argument("--output-json", required=True, help="Output path for ingest JSON.")
    p_ingest.set_defaults(func=cmd_ingest)

    p_enrich = subparsers.add_parser("enrich", help="Run LLM reasoning alignment on ingest JSON.")
    p_enrich.add_argument("--ingest-json", required=True, help="Input JSON generated by ingest.")
    p_enrich.add_argument(
        "--provider",
        default="auto",
        help="LLM provider: auto, openai, claude, rule.",
    )
    p_enrich.add_argument("--config-env", default=DEFAULT_CONFIG_ENV, help="Path to .env config file.")
    p_enrich.add_argument("--config-json", default=DEFAULT_CONFIG_JSON, help="Path to JSON config file.")
    p_enrich.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="HTTP timeout seconds.")
    p_enrich.add_argument("--retry", type=int, default=3, help="LLM retry attempts on transient failures.")
    p_enrich.add_argument(
        "--compact-prompt",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Trim large recent arrays before building LLM prompt.",
    )
    p_enrich.add_argument(
        "--skip-llm-when-empty",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip external LLM call when both physical/social rows are zero.",
    )
    p_enrich.add_argument("--output-json", required=True, help="Output path for enrich JSON.")
    p_enrich.set_defaults(func=cmd_enrich)

    p_sum = subparsers.add_parser("summarize", help="Render terminal markdown briefing.")
    p_sum.add_argument("--ingest-json", required=True, help="Input ingest JSON.")
    p_sum.add_argument("--enrich-json", required=True, help="Input enrich JSON.")
    p_sum.add_argument("--title", default="生态议会终端简报", help="Markdown report title.")
    p_sum.add_argument("--output-md", required=True, help="Output markdown path.")
    p_sum.set_defaults(func=cmd_summarize)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return int(args.func(args))
    except sqlite3.Error as exc:
        print(f"ECO_COUNCIL_ERR reason=sqlite_error detail={exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"ECO_COUNCIL_ERR reason=value_error detail={exc}", file=sys.stderr)
        return 1
    except RuntimeError as exc:
        print(f"ECO_COUNCIL_ERR reason=runtime_error detail={exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ECO_COUNCIL_ERR reason=unexpected detail={exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
