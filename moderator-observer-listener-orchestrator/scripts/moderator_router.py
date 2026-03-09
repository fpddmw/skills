#!/usr/bin/env python3
"""Moderator control-plane for planning, dispatching, and reviewing observer/listener workflows."""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_OBSERVER_ENTRYPOINT = "observer-openaq-physical-ingestor/scripts/aqi_ingest.py"
DEFAULT_LISTENER_INGEST = "listener-gdelt-doc-ingestor/scripts/gdelt_ingest.py"
DEFAULT_LISTENER_ENRICH = "listener-gdelt-doc-ingestor/scripts/gdelt_enrich.py"
DEFAULT_LISTENER_SUMMARIZE = "listener-gdelt-doc-ingestor/scripts/gdelt_summarize.py"
DEFAULT_CASWARN = "listener-caswarn-analyzer/scripts/caswarn_analyzer.py"
DEFAULT_ECO_COUNCIL = "skill-eco-council-reviewer/scripts/eco_council_report.py"
DEFAULT_TIMEOUT = 30.0
DEFAULT_CONFIG_ENV = "moderator-observer-listener-orchestrator/assets/config.env"
DEFAULT_CONFIG_JSON = "moderator-observer-listener-orchestrator/assets/config.json"
DEFAULT_LISTENER_ENV = "listener-gdelt-doc-ingestor/assets/config.env"


@dataclass(frozen=True)
class ScenarioPreset:
    name: str
    bbox: str
    query: str
    themes: str
    pollutant_type: str


PRESETS: tuple[tuple[re.Pattern[str], ScenarioPreset], ...] = (
    (
        re.compile(r"\b(ohio|east\s+palestine|derail(?:ment|ed)?|chemical\s+spill)\b", re.IGNORECASE),
        ScenarioPreset(
            name="ohio-chemical-leak",
            bbox="-80.62,40.79,-80.42,40.91",
            query="(east palestine OR ohio OR derailment OR toxic plume OR evacuation)",
            themes="ENV_POLLUTION,ENV_CHEMICAL,WB_2167_POLLUTION,CRISISLEX_C07_SAFETY",
            pollutant_type="pm25,no2,o3",
        ),
    ),
    (
        re.compile(r"\b(japan|fukushima|wastewater|nuclear\s+water|radioactive)\b", re.IGNORECASE),
        ScenarioPreset(
            name="japan-nuclear-water",
            bbox="140.90,37.30,141.20,37.60",
            query="(fukushima OR wastewater OR discharge OR fishing ban OR radiation)",
            themes="ENV_POLLUTION,ENV_WATER,WB_2167_POLLUTION,TAX_FNCACT_RADIATION",
            pollutant_type="pm25,o3",
        ),
    ),
)


def normalize_space(value: Any) -> str:
    return " ".join(str(value or "").split())


def _safe_float(raw: str | None, default: float = 0.0) -> float:
    try:
        return float(str(raw or ""))
    except Exception:
        return default


def _safe_int(raw: str | None, default: int = 0) -> int:
    try:
        return int(float(str(raw or "")))
    except Exception:
        return default


def parse_iso_utc(raw: str | None, label: str) -> datetime:
    text = normalize_space(raw)
    if not text:
        return datetime.now(timezone.utc).replace(microsecond=0)
    candidate = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError(f"{label} must be ISO-8601 datetime, got {raw!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0)


def to_iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def to_gdelt_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S")


def parse_bbox(raw: str) -> str:
    parts = [normalize_space(p) for p in str(raw).split(",")]
    if len(parts) != 4:
        raise ValueError("bbox must be min_lon,min_lat,max_lon,max_lat")
    values = [float(p) for p in parts]
    min_lon, min_lat, max_lon, max_lat = values
    if not (-180 <= min_lon <= 180 and -180 <= max_lon <= 180 and -90 <= min_lat <= 90 and -90 <= max_lat <= 90):
        raise ValueError("bbox coordinates out of WGS84 bounds")
    if min_lon >= max_lon or min_lat >= max_lat:
        raise ValueError("bbox must satisfy min_lon<max_lon and min_lat<max_lat")
    return ",".join(str(v) for v in values)


def shell_safe_argv(argv: list[str]) -> list[str]:
    return [str(x) for x in argv if normalize_space(x) != ""]


def parse_kv_line(line: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for token in normalize_space(line).split(" "):
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        out[normalize_space(key)] = normalize_space(value.strip("'\""))
    return out


def find_preset(context: str) -> ScenarioPreset | None:
    text = normalize_space(context)
    for pattern, preset in PRESETS:
        if pattern.search(text):
            return preset
    return None


def _parse_env_line(line: str) -> tuple[str, str] | None:
    raw = normalize_space(line)
    if not raw or raw.startswith("#") or "=" not in raw:
        return None
    key, value = raw.split("=", 1)
    key = normalize_space(key)
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


def load_runtime_config(args: argparse.Namespace) -> dict[str, Any]:
    config_env = normalize_space(getattr(args, "config_env", "")) or DEFAULT_CONFIG_ENV
    config_json = normalize_space(getattr(args, "config_json", "")) or DEFAULT_CONFIG_JSON
    # Load listener env first so moderator can reuse the same LLM key/model defaults.
    load_env_file(DEFAULT_LISTENER_ENV)
    load_env_file(config_env)
    return load_json_config(config_json)


def require_llm_env(config: dict[str, Any] | None = None) -> tuple[str, str, str]:
    cfg = config or {}
    base_url = normalize_space(os.environ.get("LLM_API_BASE_URL") or "")
    api_key = normalize_space(os.environ.get("LLM_API_KEY") or "")
    model = normalize_space(os.environ.get("LLM_MODEL") or "")
    if not base_url:
        base_url = normalize_space(cfg.get("llm_api_base_url") or "")
    if not api_key:
        api_key = normalize_space(cfg.get("llm_api_key") or "")
    if not model:
        model = normalize_space(cfg.get("llm_model") or "")
    if not base_url:
        raise ValueError("LLM_API_BASE_URL is required")
    if not api_key:
        raise ValueError("LLM_API_KEY is required")
    if not model:
        raise ValueError("LLM_MODEL is required")
    return base_url.rstrip("/"), api_key, model


def call_json_api(
    url: str,
    *,
    body: dict[str, Any],
    api_key: str,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = Request(
        url=url,
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": "moderator-observer-listener-orchestrator/1.0",
        },
        data=payload,
    )
    try:
        with urlopen(req, timeout=timeout) as response:
            text = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"http_error status={exc.code} detail={detail[:500]}") from exc
    except URLError as exc:
        raise RuntimeError(f"network_error detail={exc.reason}") from exc

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("invalid_json_response") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("unexpected_response_shape")
    return parsed


def extract_chat_json(payload: dict[str, Any]) -> dict[str, Any]:
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("llm_invalid_choices")
    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = message.get("content") if isinstance(message, dict) else None
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("llm_empty_content")
    parsed = json.loads(content)
    if not isinstance(parsed, dict):
        raise RuntimeError("llm_invalid_json")
    return parsed


def build_observer_commands(
    *,
    db: str,
    bbox: str,
    start_iso: str,
    end_iso: str,
    fixture_json: str,
) -> list[dict[str, Any]]:
    ingest = [
        "python3",
        DEFAULT_OBSERVER_ENTRYPOINT,
        "ingest",
        "--db",
        db,
        f"--bbox={bbox}",
        "--start-datetime",
        start_iso,
        "--end-datetime",
        end_iso,
    ]
    if normalize_space(fixture_json):
        ingest.extend(["--fixture-json", fixture_json])
    return [
        {
            "id": "observer_ingest",
            "argv": shell_safe_argv(ingest),
            "expect_status_prefix": "PHYSICAL_INGEST_OK",
        },
        {
            "id": "observer_enrich",
            "argv": shell_safe_argv(
                [
                    "python3",
                    DEFAULT_OBSERVER_ENTRYPOINT,
                    "enrich",
                    "--db",
                    db,
                    "--start-datetime",
                    start_iso,
                    "--end-datetime",
                    end_iso,
                    "--standard-profile",
                    "auto",
                ]
            ),
            "expect_status_prefix": "PHYSICAL_ENRICH_OK",
        },
        {
            "id": "observer_summarize",
            "argv": shell_safe_argv(
                [
                    "python3",
                    DEFAULT_OBSERVER_ENTRYPOINT,
                    "summarize",
                    "--db",
                    db,
                    "--start-datetime",
                    start_iso,
                    "--end-datetime",
                    end_iso,
                ]
            ),
            "expect_status_prefix": "PHYSICAL_SUMMARY_OK",
        },
    ]


def build_listener_commands(
    *,
    db: str,
    query: str,
    themes: str,
    start_gdelt: str,
    end_gdelt: str,
    classify_mode: str,
    max_records: int,
    articles_json: str,
) -> list[dict[str, Any]]:
    ingest = [
        "python3",
        DEFAULT_LISTENER_INGEST,
        "ingest",
        "--db",
        db,
        "--query",
        query,
        "--themes",
        themes,
        "--start-datetime",
        start_gdelt,
        "--end-datetime",
        end_gdelt,
        "--classify-mode",
        classify_mode,
        "--max-records",
        str(max_records),
    ]
    if normalize_space(articles_json):
        ingest.extend(["--articles-json", articles_json])

    return [
        {
            "id": "listener_ingest",
            "argv": shell_safe_argv(ingest),
            "expect_status_prefix": "GDELT_SYNC_OK",
        },
        {
            "id": "listener_enrich",
            "argv": shell_safe_argv(
                [
                    "python3",
                    DEFAULT_LISTENER_ENRICH,
                    "--db",
                    db,
                    "--classify-mode",
                    classify_mode,
                    "--limit",
                    str(max_records),
                ]
            ),
            "expect_status_prefix": "GDELT_ENRICH_OK",
        },
        {
            "id": "listener_summarize",
            "argv": shell_safe_argv(
                [
                    "python3",
                    DEFAULT_LISTENER_SUMMARIZE,
                    "--db",
                    db,
                    "--only-relevant",
                    "--since-datetime",
                    start_gdelt,
                    "--limit",
                    str(max(max_records * 2, 50)),
                ]
            ),
            "expect_status_prefix": "SOCIAL_SUMMARIZE_OK",
        },
    ]


def build_caswarn_command(*, db: str, hours: int, limit: int, mode: str) -> dict[str, Any]:
    return {
        "id": "caswarn_analyze",
        "argv": shell_safe_argv(
            [
                "python3",
                DEFAULT_CASWARN,
                "--db",
                db,
                "--hours",
                str(hours),
                "--limit",
                str(limit),
                "--mode",
                mode,
            ]
        ),
        "expect_status_prefix": "[SUCCESS]",
    }


def build_eco_council_commands(
    *,
    event_id: str,
    observer_db: str,
    listener_db: str,
    start_iso: str,
    end_iso: str,
    provider: str,
    timeout: float,
    output_dir: str,
    config_env: str,
    config_json: str,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    base_dir = Path(output_dir).expanduser()
    artifacts = {
        "ingest_json": str(base_dir / f"{event_id}.ingest.json"),
        "enrich_json": str(base_dir / f"{event_id}.enrich.json"),
        "summary_md": str(base_dir / f"{event_id}.brief.md"),
    }
    commands = [
        {
            "id": "eco_council_ingest",
            "argv": shell_safe_argv(
                [
                    "python3",
                    DEFAULT_ECO_COUNCIL,
                    "ingest",
                    "--event-id",
                    event_id,
                    "--observer-db",
                    observer_db,
                    "--listener-db",
                    listener_db,
                    "--start-datetime",
                    start_iso,
                    "--end-datetime",
                    end_iso,
                    "--output-json",
                    artifacts["ingest_json"],
                ]
            ),
            "expect_status_prefix": "ECO_COUNCIL_INGEST_OK",
        },
        {
            "id": "eco_council_enrich",
            "argv": shell_safe_argv(
                [
                    "python3",
                    DEFAULT_ECO_COUNCIL,
                    "enrich",
                    "--ingest-json",
                    artifacts["ingest_json"],
                    "--provider",
                    provider,
                    "--timeout",
                    str(timeout),
                    "--config-env",
                    config_env,
                    "--config-json",
                    config_json,
                    "--output-json",
                    artifacts["enrich_json"],
                ]
            ),
            "expect_status_prefix": "ECO_COUNCIL_ENRICH_OK",
        },
        {
            "id": "eco_council_summarize",
            "argv": shell_safe_argv(
                [
                    "python3",
                    DEFAULT_ECO_COUNCIL,
                    "summarize",
                    "--ingest-json",
                    artifacts["ingest_json"],
                    "--enrich-json",
                    artifacts["enrich_json"],
                    "--output-md",
                    artifacts["summary_md"],
                ]
            ),
            "expect_status_prefix": "ECO_COUNCIL_SUMMARY_OK",
        },
    ]
    return commands, artifacts


def extract_status_line(stdout: str, stderr: str, expected_prefix: str) -> str:
    lines: list[str] = []
    lines.extend([normalize_space(x) for x in stdout.splitlines() if normalize_space(x)])
    lines.extend([normalize_space(x) for x in stderr.splitlines() if normalize_space(x)])
    for line in lines:
        if line.startswith(expected_prefix):
            return line
    return lines[-1] if lines else ""


def run_command(command: dict[str, Any], *, dry_run: bool, timeout: int) -> dict[str, Any]:
    argv = [str(x) for x in command.get("argv", [])]
    if dry_run:
        return {
            "id": command.get("id"),
            "argv": argv,
            "returncode": 0,
            "stdout": "",
            "stderr": "",
            "status_line": f"DRY_RUN {command.get('id')}",
            "ok": True,
        }
    proc = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, check=False)
    status_line = extract_status_line(proc.stdout, proc.stderr, str(command.get("expect_status_prefix") or ""))
    ok = proc.returncode == 0 and bool(status_line)
    return {
        "id": command.get("id"),
        "argv": argv,
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "status_line": status_line,
        "ok": ok,
    }


def evaluate_review(
    *,
    observer_status: str,
    listener_status: str,
    caswarn_status: str,
    exceed_rate_threshold: float,
    exceed_count_threshold: int,
    social_upsert_threshold: int,
    analyzer_risk_threshold: float,
) -> dict[str, Any]:
    observer_fields = parse_kv_line(observer_status)
    listener_fields = parse_kv_line(listener_status)

    caswarn_risk = 0.0
    caswarn_error = False
    normalized_caswarn = normalize_space(caswarn_status)
    if normalized_caswarn:
        caswarn_error = "ERR" in normalized_caswarn
        match = re.search(r'"nimby_risk_score"\s*:\s*([0-9]*\.?[0-9]+)', normalized_caswarn)
        if match:
            caswarn_risk = _safe_float(match.group(1), 0.0)

    has_error = (
        "ERR" in normalize_space(observer_status)
        or "ERR" in normalize_space(listener_status)
        or caswarn_error
    )

    observer_exceed_rate = _safe_float(observer_fields.get("exceed_rate"), 0.0)
    observer_exceeded = _safe_int(observer_fields.get("exceeded"), 0)
    listener_upserted = _safe_int(listener_fields.get("upserted"), 0)

    should_escalate = (
        observer_exceed_rate >= exceed_rate_threshold
        or observer_exceeded >= exceed_count_threshold
        or listener_upserted >= social_upsert_threshold
        or caswarn_risk >= analyzer_risk_threshold
    )

    decision = "sleep"
    reason = "normal_window"
    if has_error:
        decision = "retry_or_manual_check"
        reason = "downstream_error"
    elif should_escalate:
        decision = "switch_to_active_recon"
        reason = "risk_signal_detected"

    return {
        "mode": "moderator_review",
        "decision": decision,
        "reason": reason,
        "metrics": {
            "observer_exceed_rate": round(observer_exceed_rate, 6),
            "observer_exceeded": observer_exceeded,
            "listener_upserted": listener_upserted,
            "caswarn_risk": round(caswarn_risk, 6),
        },
        "thresholds": {
            "observer_exceed_rate_gte": exceed_rate_threshold,
            "observer_exceeded_gte": exceed_count_threshold,
            "listener_upserted_gte": social_upsert_threshold,
            "caswarn_risk_gte": analyzer_risk_threshold,
        },
        "next": {
            "method": "recon" if decision == "switch_to_active_recon" else "patrol",
            "run_at": "immediate" if decision != "sleep" else "next_schedule",
        },
    }


def _sqlite_one(db_path: str, sql: str) -> dict[str, Any]:
    db_file = Path(db_path).expanduser()
    if not db_file.exists():
        return {}
    conn = sqlite3.connect(str(db_file))
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(sql).fetchone()
        if row is None:
            return {}
        return dict(row)
    except sqlite3.Error:
        return {}
    finally:
        conn.close()


def _parse_caswarn_status(line: str) -> dict[str, Any]:
    text = normalize_space(line)
    if not text:
        return {}
    out: dict[str, Any] = {"status_line": text}
    match_count = re.search(r"Analyzed\\s+(\\d+)\\s+news\\s+items", text)
    if match_count:
        out["analyzed_count"] = _safe_int(match_count.group(1), 0)
    match_json = re.search(r"(\\{.*\\})", text)
    if match_json:
        try:
            parsed = json.loads(match_json.group(1))
            if isinstance(parsed, dict):
                out.update(parsed)
        except json.JSONDecodeError:
            pass
    return out


def build_cycle_report(
    *,
    observer_db: str,
    listener_db: str,
    observer_status: str,
    listener_status: str,
    caswarn_status: str,
) -> dict[str, Any]:
    observer_status_kv = parse_kv_line(observer_status)
    listener_status_kv = parse_kv_line(listener_status)
    observer_db_snapshot = _sqlite_one(
        observer_db,
        (
            "SELECT COUNT(1) AS physical_metrics_count, "
            "MAX(metric_date) AS latest_metric_date, "
            "MAX(exceed_rate) AS max_exceed_rate "
            "FROM physical_metrics"
        ),
    )
    listener_db_snapshot = _sqlite_one(
        listener_db,
        (
            "SELECT COUNT(1) AS social_events_count, "
            "SUM(CASE WHEN COALESCE(is_analyzed,0)=0 THEN 1 ELSE 0 END) AS pending_analysis_count, "
            "MAX(seendate_utc) AS latest_social_event_utc "
            "FROM social_events"
        ),
    )
    return {
        "status": {
            "observer": observer_status,
            "listener": listener_status,
            "analyzer": caswarn_status,
        },
        "parsed_status": {
            "observer": observer_status_kv,
            "listener": listener_status_kv,
            "analyzer": _parse_caswarn_status(caswarn_status),
        },
        "db_snapshot": {
            "observer": observer_db_snapshot,
            "listener": listener_db_snapshot,
        },
    }


def build_directive(
    *,
    mode: str,
    bbox: str,
    pollutant_type: str,
    query: str,
    themes: str,
    hours: int,
    max_records: int,
    observer_db: str,
    listener_db: str,
    classify_mode: str,
    analyzer_mode: str,
    analyzer_limit: int,
    observer_fixture_json: str,
    listener_articles_json: str,
    context: str,
    scenario: str,
    now_utc: datetime,
) -> dict[str, Any]:
    start_utc = now_utc - timedelta(hours=max(hours, 1))
    start_iso = to_iso_utc(start_utc)
    end_iso = to_iso_utc(now_utc)
    start_gdelt = to_gdelt_utc(start_utc)
    end_gdelt = to_gdelt_utc(now_utc)

    payload = {
        "mode": mode,
        "context": context,
        "scenario": scenario,
        "window": {
            "hours": int(max(hours, 1)),
            "start_iso_utc": start_iso,
            "end_iso_utc": end_iso,
            "start_gdelt_utc": start_gdelt,
            "end_gdelt_utc": end_gdelt,
        },
        "observer": {
            "params": {
                "bbox": bbox,
                "pollutant_type": pollutant_type,
            },
            "commands": build_observer_commands(
                db=observer_db,
                bbox=bbox,
                start_iso=start_iso,
                end_iso=end_iso,
                fixture_json=observer_fixture_json,
            ),
        },
        "listener": {
            "params": {
                "query": query,
                "theme": themes,
                "timespan": f"last_{int(max(hours, 1))}h",
            },
            "commands": build_listener_commands(
                db=listener_db,
                query=query,
                themes=themes,
                start_gdelt=start_gdelt,
                end_gdelt=end_gdelt,
                classify_mode=classify_mode,
                max_records=max_records,
                articles_json=listener_articles_json,
            ),
        },
        "analyzer": {
            "commands": [
                build_caswarn_command(
                    db=listener_db,
                    hours=max(hours, 1),
                    limit=max(analyzer_limit, 1),
                    mode=analyzer_mode,
                )
            ]
        },
    }
    return payload


def llm_plan(
    *,
    objective: str,
    context: str,
    mode_hint: str,
    fallback_bbox: str,
    fallback_query: str,
    fallback_themes: str,
    timeout: float,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    base_url, api_key, model = require_llm_env(config)
    prompt = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a strict moderator planner. Return JSON only with keys: "
                    "mode,bbox,pollutant_type,query,theme,timespan_hours,max_records,analyzer_mode,analyzer_limit,reason. "
                    "mode must be passive_patrol or active_reconnaissance. "
                    "bbox must be min_lon,min_lat,max_lon,max_lat. "
                    "timespan_hours integer 1..72. max_records integer 20..250. "
                    "analyzer_mode is rule or llm."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"objective={json.dumps(objective, ensure_ascii=False)}\n"
                    f"context={json.dumps(context, ensure_ascii=False)}\n"
                    f"mode_hint={json.dumps(mode_hint, ensure_ascii=False)}\n"
                    f"fallback_bbox={json.dumps(fallback_bbox, ensure_ascii=False)}\n"
                    f"fallback_query={json.dumps(fallback_query, ensure_ascii=False)}\n"
                    f"fallback_themes={json.dumps(fallback_themes, ensure_ascii=False)}\n"
                    "Return valid JSON only."
                ),
            },
        ],
        "response_format": {"type": "json_object"},
    }
    payload = call_json_api(f"{base_url}/chat/completions", body=prompt, api_key=api_key, timeout=timeout)
    return extract_chat_json(payload)


def normalize_plan(
    *,
    raw: dict[str, Any] | None,
    objective: str,
    context: str,
    mode_hint: str,
    fallback_bbox: str,
    fallback_query: str,
    fallback_themes: str,
) -> dict[str, Any]:
    preset = find_preset(context or objective)

    mode = normalize_space((raw or {}).get("mode") if raw else "")
    if mode not in {"passive_patrol", "active_reconnaissance"}:
        mode = "active_reconnaissance" if mode_hint == "recon" else "passive_patrol"

    bbox_candidate = normalize_space((raw or {}).get("bbox") if raw else "")
    if not bbox_candidate:
        bbox_candidate = preset.bbox if preset else fallback_bbox
    bbox = parse_bbox(bbox_candidate)

    query = normalize_space((raw or {}).get("query") if raw else "")
    if not query:
        query = preset.query if preset else fallback_query

    themes = normalize_space((raw or {}).get("theme") if raw else "")
    if not themes:
        themes = preset.themes if preset else fallback_themes

    pollutant_type = normalize_space((raw or {}).get("pollutant_type") if raw else "")
    if not pollutant_type:
        pollutant_type = preset.pollutant_type if preset else "pm25,no2,o3"

    timespan_hours = _safe_int((raw or {}).get("timespan_hours") if raw else 4, 4)
    timespan_hours = max(1, min(72, timespan_hours))

    max_records = _safe_int((raw or {}).get("max_records") if raw else 120, 120)
    max_records = max(20, min(250, max_records))

    analyzer_mode = normalize_space((raw or {}).get("analyzer_mode") if raw else "llm").lower() or "llm"
    if analyzer_mode not in {"llm", "rule"}:
        analyzer_mode = "llm"

    analyzer_limit = _safe_int((raw or {}).get("analyzer_limit") if raw else 80, 80)
    analyzer_limit = max(1, min(500, analyzer_limit))

    reason = normalize_space((raw or {}).get("reason") if raw else "")

    return {
        "mode": mode,
        "bbox": bbox,
        "query": query,
        "theme": themes,
        "pollutant_type": pollutant_type,
        "timespan_hours": timespan_hours,
        "max_records": max_records,
        "analyzer_mode": analyzer_mode,
        "analyzer_limit": analyzer_limit,
        "reason": reason,
        "scenario": preset.name if preset else "custom",
    }


def print_json(payload: dict[str, Any]) -> int:
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    return 0


def cmd_patrol(args: argparse.Namespace) -> int:
    now_utc = parse_iso_utc(args.now_utc, "--now-utc")
    directive = build_directive(
        mode="passive_patrol",
        bbox=parse_bbox(args.bbox),
        pollutant_type=args.pollutant_type,
        query=args.query,
        themes=args.themes,
        hours=max(args.hours, 1),
        max_records=max(args.max_records, 20),
        observer_db=args.observer_db,
        listener_db=args.listener_db,
        classify_mode=args.classify_mode,
        analyzer_mode=args.analyzer_mode,
        analyzer_limit=args.analyzer_limit,
        observer_fixture_json=args.observer_fixture_json,
        listener_articles_json=args.listener_articles_json,
        context="",
        scenario="patrol-default",
        now_utc=now_utc,
    )
    directive["review_policy"] = {
        "next_method": "review",
        "escalate_if": {
            "observer_exceed_rate_gte": args.exceed_rate_threshold,
            "listener_upserted_gte": args.social_upsert_threshold,
        },
    }
    return print_json(directive)


def cmd_recon(args: argparse.Namespace) -> int:
    now_utc = parse_iso_utc(args.now_utc, "--now-utc")
    preset = find_preset(args.context)
    bbox = parse_bbox(args.bbox) if normalize_space(args.bbox) else parse_bbox(preset.bbox if preset else args.fallback_bbox)
    query = normalize_space(args.query) or (preset.query if preset else args.fallback_query)
    themes = normalize_space(args.themes) or (preset.themes if preset else args.fallback_themes)
    pollutant_type = normalize_space(args.pollutant_type) or (preset.pollutant_type if preset else "pm25,no2,o3")

    directive = build_directive(
        mode="active_reconnaissance",
        bbox=bbox,
        pollutant_type=pollutant_type,
        query=query,
        themes=themes,
        hours=max(args.hours, 1),
        max_records=max(args.max_records, 20),
        observer_db=args.observer_db,
        listener_db=args.listener_db,
        classify_mode=args.classify_mode,
        analyzer_mode=args.analyzer_mode,
        analyzer_limit=args.analyzer_limit,
        observer_fixture_json=args.observer_fixture_json,
        listener_articles_json=args.listener_articles_json,
        context=normalize_space(args.context),
        scenario=preset.name if preset else "custom",
        now_utc=now_utc,
    )
    return print_json(directive)


def cmd_review(args: argparse.Namespace) -> int:
    payload = evaluate_review(
        observer_status=args.observer_status,
        listener_status=args.listener_status,
        caswarn_status=args.caswarn_status,
        exceed_rate_threshold=args.exceed_rate_threshold,
        exceed_count_threshold=args.exceed_count_threshold,
        social_upsert_threshold=args.social_upsert_threshold,
        analyzer_risk_threshold=args.analyzer_risk_threshold,
    )
    return print_json(payload)


def cmd_plan_llm(args: argparse.Namespace) -> int:
    runtime_config = load_runtime_config(args)
    raw_plan = llm_plan(
        objective=args.objective,
        context=args.context,
        mode_hint=args.mode_hint,
        fallback_bbox=args.fallback_bbox,
        fallback_query=args.fallback_query,
        fallback_themes=args.fallback_themes,
        timeout=args.llm_timeout,
        config=runtime_config,
    )
    plan = normalize_plan(
        raw=raw_plan,
        objective=args.objective,
        context=args.context,
        mode_hint=args.mode_hint,
        fallback_bbox=args.fallback_bbox,
        fallback_query=args.fallback_query,
        fallback_themes=args.fallback_themes,
    )
    payload = {
        "mode": "llm_plan",
        "objective": args.objective,
        "context": args.context,
        "plan": plan,
    }
    return print_json(payload)


def _collect_primary_status(records: list[dict[str, Any]], fallback: str) -> str:
    for record in records:
        if record.get("id") == fallback and normalize_space(record.get("status_line")):
            return normalize_space(record.get("status_line"))
    for record in reversed(records):
        line = normalize_space(record.get("status_line"))
        if line:
            return line
    return ""


def cmd_orchestrate(args: argparse.Namespace) -> int:
    if args.max_cycles < 1:
        raise ValueError("--max-cycles must be >= 1")

    runtime_config = load_runtime_config(args)
    current_context = normalize_space(args.context)
    current_mode_hint = args.initial_mode
    cycles: list[dict[str, Any]] = []

    for cycle_idx in range(1, args.max_cycles + 1):
        now_utc = parse_iso_utc(args.now_utc, "--now-utc")

        if args.planner == "llm":
            raw_plan = llm_plan(
                objective=args.objective,
                context=current_context,
                mode_hint=current_mode_hint,
                fallback_bbox=args.fallback_bbox,
                fallback_query=args.fallback_query,
                fallback_themes=args.fallback_themes,
                timeout=args.llm_timeout,
                config=runtime_config,
            )
        else:
            raw_plan = {}

        plan = normalize_plan(
            raw=raw_plan,
            objective=args.objective,
            context=current_context,
            mode_hint=current_mode_hint,
            fallback_bbox=args.fallback_bbox,
            fallback_query=args.fallback_query,
            fallback_themes=args.fallback_themes,
        )

        directive = build_directive(
            mode=plan["mode"],
            bbox=plan["bbox"],
            pollutant_type=plan["pollutant_type"],
            query=plan["query"],
            themes=plan["theme"],
            hours=plan["timespan_hours"],
            max_records=plan["max_records"],
            observer_db=args.observer_db,
            listener_db=args.listener_db,
            classify_mode=args.classify_mode,
            analyzer_mode=plan["analyzer_mode"],
            analyzer_limit=plan["analyzer_limit"],
            observer_fixture_json=args.observer_fixture_json,
            listener_articles_json=args.listener_articles_json,
            context=current_context,
            scenario=plan["scenario"],
            now_utc=now_utc,
        )
        cycle_event_id = f"eco-{cycle_idx:02d}-{to_gdelt_utc(now_utc)}"
        eco_commands, eco_artifacts = build_eco_council_commands(
            event_id=cycle_event_id,
            observer_db=args.observer_db,
            listener_db=args.listener_db,
            start_iso=directive["window"]["start_iso_utc"],
            end_iso=directive["window"]["end_iso_utc"],
            provider=args.eco_provider,
            timeout=args.llm_timeout,
            output_dir=args.eco_output_dir,
            config_env=args.config_env,
            config_json=args.config_json,
        )
        directive["eco_council"] = {
            "event_id": cycle_event_id,
            "provider": args.eco_provider,
            "commands": eco_commands,
            "artifacts": eco_artifacts,
        }

        observer_exec = [run_command(c, dry_run=args.dry_run, timeout=args.command_timeout) for c in directive["observer"]["commands"]]
        listener_exec = [run_command(c, dry_run=args.dry_run, timeout=args.command_timeout) for c in directive["listener"]["commands"]]

        analyzer_exec: list[dict[str, Any]] = []
        if args.run_analyzer_always or plan["mode"] == "active_reconnaissance":
            analyzer_exec = [
                run_command(c, dry_run=args.dry_run, timeout=args.command_timeout)
                for c in directive.get("analyzer", {}).get("commands", [])
            ]
        eco_exec: list[dict[str, Any]] = []
        if not args.skip_eco_council:
            eco_exec = [
                run_command(c, dry_run=args.dry_run, timeout=args.command_timeout)
                for c in directive.get("eco_council", {}).get("commands", [])
            ]

        observer_status = _collect_primary_status(observer_exec, "observer_enrich")
        listener_status = _collect_primary_status(listener_exec, "listener_summarize")
        caswarn_status = _collect_primary_status(analyzer_exec, "caswarn_analyze")

        review = evaluate_review(
            observer_status=observer_status,
            listener_status=listener_status,
            caswarn_status=caswarn_status,
            exceed_rate_threshold=args.exceed_rate_threshold,
            exceed_count_threshold=args.exceed_count_threshold,
            social_upsert_threshold=args.social_upsert_threshold,
            analyzer_risk_threshold=args.analyzer_risk_threshold,
        )
        report = build_cycle_report(
            observer_db=args.observer_db,
            listener_db=args.listener_db,
            observer_status=observer_status,
            listener_status=listener_status,
            caswarn_status=caswarn_status,
        )

        cycle = {
            "cycle": cycle_idx,
            "plan": plan,
            "directive": directive,
            "execution": {
                "observer": observer_exec,
                "listener": listener_exec,
                "analyzer": analyzer_exec,
                "eco_council": eco_exec,
            },
            "review": review,
            "report": report,
        }
        cycles.append(cycle)

        if review["decision"] == "switch_to_active_recon" and cycle_idx < args.max_cycles:
            current_mode_hint = "recon"
            current_context = normalize_space(
                f"{current_context} observer_status={observer_status} listener_status={listener_status} caswarn_status={caswarn_status}"
            )
            continue
        break

    final_review = cycles[-1]["review"] if cycles else {}
    payload = {
        "mode": "moderator_orchestrate",
        "objective": args.objective,
        "planner": args.planner,
        "dry_run": bool(args.dry_run),
        "cycles": cycles,
        "final_decision": final_review,
    }
    return print_json(payload)


def add_common_dispatch_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--observer-db", default="observer_physical.db", help="Observer SQLite path.")
    parser.add_argument("--listener-db", default="gdelt_environment.db", help="Listener SQLite path.")
    parser.add_argument("--classify-mode", choices=("rule", "llm", "none"), default="rule")
    parser.add_argument("--analyzer-mode", choices=("rule", "llm"), default="llm")
    parser.add_argument("--analyzer-limit", type=int, default=80)
    parser.add_argument("--observer-fixture-json", default="", help="Optional fixture for observer ingest.")
    parser.add_argument("--listener-articles-json", default="", help="Optional fixture for listener ingest.")
    parser.add_argument("--now-utc", default="", help="Optional override ISO-8601 UTC clock.")


def add_review_threshold_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--exceed-rate-threshold", type=float, default=0.25)
    parser.add_argument("--exceed-count-threshold", type=int, default=8)
    parser.add_argument("--social-upsert-threshold", type=int, default=20)
    parser.add_argument("--analyzer-risk-threshold", type=float, default=0.7)


def add_llm_config_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config-env", default=DEFAULT_CONFIG_ENV, help="Path to .env config file.")
    parser.add_argument("--config-json", default=DEFAULT_CONFIG_JSON, help="Path to JSON config file.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate plans, execute observer/listener chains, and review escalation decisions."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    patrol = subparsers.add_parser("patrol", help="Build passive-patrol directives.")
    add_common_dispatch_args(patrol)
    patrol.add_argument("--bbox", default="-179,-89,179,89", help="min_lon,min_lat,max_lon,max_lat")
    patrol.add_argument("--pollutant-type", default="pm25")
    patrol.add_argument(
        "--query",
        default="(pollution OR contamination OR toxic OR air quality OR water quality)",
    )
    patrol.add_argument(
        "--themes",
        default="ENV_POLLUTION,ENV_AIR,ENV_WATER,ENV_CHEMICAL,WB_2167_POLLUTION",
    )
    patrol.add_argument("--hours", type=int, default=4)
    patrol.add_argument("--max-records", type=int, default=150)
    patrol.add_argument("--exceed-rate-threshold", type=float, default=0.25)
    patrol.add_argument("--social-upsert-threshold", type=int, default=20)
    patrol.set_defaults(func=cmd_patrol)

    recon = subparsers.add_parser("recon", help="Build active-recon directives from context.")
    add_common_dispatch_args(recon)
    recon.add_argument("--context", required=True)
    recon.add_argument("--hours", type=int, default=24)
    recon.add_argument("--bbox", default="")
    recon.add_argument("--query", default="")
    recon.add_argument("--themes", default="")
    recon.add_argument("--pollutant-type", default="")
    recon.add_argument("--max-records", type=int, default=250)
    recon.add_argument("--fallback-bbox", default="-179,-89,179,89")
    recon.add_argument("--fallback-query", default="(environment OR pollution OR contamination OR protest OR evacuation)")
    recon.add_argument(
        "--fallback-themes",
        default="ENV_POLLUTION,ENV_CHEMICAL,ENV_WATER,WB_2167_POLLUTION,CRISISLEX_C07_SAFETY",
    )
    recon.set_defaults(func=cmd_recon)

    review = subparsers.add_parser("review", help="Evaluate status lines and decide escalation.")
    review.add_argument("--observer-status", required=True)
    review.add_argument("--listener-status", required=True)
    review.add_argument("--caswarn-status", default="")
    add_review_threshold_args(review)
    review.set_defaults(func=cmd_review)

    plan_llm = subparsers.add_parser("plan-llm", help="Use LLM to generate structured directives.")
    add_llm_config_args(plan_llm)
    plan_llm.add_argument("--objective", required=True)
    plan_llm.add_argument("--context", default="")
    plan_llm.add_argument("--mode-hint", choices=("patrol", "recon"), default="recon")
    plan_llm.add_argument("--fallback-bbox", default="-179,-89,179,89")
    plan_llm.add_argument("--fallback-query", default="(environment OR pollution OR contamination OR protest OR evacuation)")
    plan_llm.add_argument(
        "--fallback-themes",
        default="ENV_POLLUTION,ENV_CHEMICAL,ENV_WATER,WB_2167_POLLUTION,CRISISLEX_C07_SAFETY",
    )
    plan_llm.add_argument("--llm-timeout", type=float, default=DEFAULT_TIMEOUT)
    plan_llm.set_defaults(func=cmd_plan_llm)

    orchestrate = subparsers.add_parser("orchestrate", help="Plan with LLM, execute chains, and iterate reviews.")
    add_common_dispatch_args(orchestrate)
    add_review_threshold_args(orchestrate)
    add_llm_config_args(orchestrate)
    orchestrate.add_argument("--objective", required=True)
    orchestrate.add_argument("--context", default="")
    orchestrate.add_argument("--planner", choices=("llm", "preset"), default="llm")
    orchestrate.add_argument("--initial-mode", choices=("patrol", "recon"), default="patrol")
    orchestrate.add_argument("--max-cycles", type=int, default=2)
    orchestrate.add_argument("--run-analyzer-always", action="store_true")
    orchestrate.add_argument("--fallback-bbox", default="-179,-89,179,89")
    orchestrate.add_argument("--fallback-query", default="(environment OR pollution OR contamination OR protest OR evacuation)")
    orchestrate.add_argument(
        "--fallback-themes",
        default="ENV_POLLUTION,ENV_CHEMICAL,ENV_WATER,WB_2167_POLLUTION,CRISISLEX_C07_SAFETY",
    )
    orchestrate.add_argument("--llm-timeout", type=float, default=DEFAULT_TIMEOUT)
    orchestrate.add_argument("--command-timeout", type=int, default=180)
    orchestrate.add_argument("--eco-provider", choices=("auto", "openai", "claude", "rule"), default="auto")
    orchestrate.add_argument("--eco-output-dir", default="/tmp/eco_council_reports")
    orchestrate.add_argument("--skip-eco-council", action="store_true")
    orchestrate.add_argument("--dry-run", action="store_true")
    orchestrate.set_defaults(func=cmd_orchestrate)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return int(args.func(args))
    except subprocess.TimeoutExpired as exc:
        print(json.dumps({"error": "command_timeout", "detail": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1
    except ValueError as exc:
        print(json.dumps({"error": "value_error", "detail": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1
    except RuntimeError as exc:
        print(json.dumps({"error": "runtime_error", "detail": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1
    except Exception as exc:  # pylint: disable=broad-except
        print(json.dumps({"error": "unexpected", "detail": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
