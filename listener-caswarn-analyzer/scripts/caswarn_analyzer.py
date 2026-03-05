#!/usr/bin/env python3
"""Analyze pending social-events rows for NIMBY/SARF signals and write back to SQLite."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_DB_FILENAME = "gdelt_environment.db"
DEFAULT_DB_PATH = os.environ.get("GDELT_ENV_DB_PATH", DEFAULT_DB_FILENAME)
DEFAULT_TIMEOUT = 25.0
DEFAULT_USER_AGENT = "listener-caswarn-analyzer/1.0 (+https://github.com/tiangong-ai/skills)"
ALLOWED_MODES = {"rule", "llm"}


@dataclass(frozen=True)
class AnalysisResult:
    sarf_label: str
    sarf_reason: str
    dominant_emotion: str
    nimby_risk_score: float
    risk_frame: str
    model_name: str


def normalize_space(value: Any) -> str:
    return " ".join(str(value or "").split())


def resolve_db_path(db_path: str) -> Path:
    raw = normalize_space(db_path)
    if not raw:
        raw = DEFAULT_DB_PATH
    path = Path(raw).expanduser()
    if raw.endswith(("/", "\\")) or path.is_dir() or path.suffix == "":
        path = path / DEFAULT_DB_FILENAME
    return path


def connect_db(db_path: str) -> sqlite3.Connection:
    db_file = resolve_db_path(db_path)
    if db_file.parent and str(db_file.parent) not in ("", "."):
        db_file.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_file))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def table_has_column(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(str(row["name"]) == column_name for row in rows)


def ensure_social_analysis_columns(conn: sqlite3.Connection) -> None:
    required_columns: tuple[tuple[str, str], ...] = (
        ("article_summary", "TEXT"),
        ("article_text", "TEXT"),
        ("is_analyzed", "INTEGER NOT NULL DEFAULT 0"),
        ("analyzed_at", "TEXT"),
        ("analysis_model", "TEXT"),
        ("sarf_label", "TEXT"),
        ("sarf_reason", "TEXT"),
        ("dominant_emotion", "TEXT"),
        ("nimby_risk_score", "REAL"),
        ("risk_frame", "TEXT"),
    )
    for name, spec in required_columns:
        if not table_has_column(conn, "social_events", name):
            conn.execute(f"ALTER TABLE social_events ADD COLUMN {name} {spec}")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_social_events_analyzed ON social_events(is_analyzed, seendate_utc DESC)"
    )
    conn.commit()


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def utc_threshold(hours: int) -> str:
    point = datetime.now(timezone.utc) - timedelta(hours=hours)
    return point.strftime("%Y%m%d%H%M%S")


def require_llm_env() -> tuple[str, str, str]:
    base_url = normalize_space(os.environ.get("LLM_API_BASE_URL") or "")
    api_key = normalize_space(os.environ.get("LLM_API_KEY") or "")
    model = normalize_space(os.environ.get("LLM_MODEL") or "")
    if not base_url:
        raise ValueError("LLM_API_BASE_URL is required when --mode llm")
    if not api_key:
        raise ValueError("LLM_API_KEY is required when --mode llm")
    if not model:
        raise ValueError("LLM_MODEL is required when --mode llm")
    return base_url.rstrip("/"), api_key, model


def call_json_api(
    url: str,
    *,
    method: str = "POST",
    timeout: float = DEFAULT_TIMEOUT,
    headers: dict[str, str] | None = None,
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: bytes | None = None
    req_headers = {"User-Agent": DEFAULT_USER_AGENT}
    if headers:
        req_headers.update(headers)
    if body is not None:
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        req_headers["Content-Type"] = "application/json"
    req = Request(url=url, method=method, headers=req_headers, data=payload)
    try:
        with urlopen(req, timeout=timeout) as response:
            text = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"http_error status={exc.code} detail={detail[:500]}") from exc
    except URLError as exc:
        raise RuntimeError(f"network_error detail={exc.reason}") from exc

    try:
        payload_json = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("invalid_json_response") from exc
    if not isinstance(payload_json, dict):
        raise RuntimeError("unexpected_response_shape")
    return payload_json


def clamp_risk(raw_value: Any) -> float:
    try:
        value = float(str(raw_value))
    except Exception:
        value = 0.0
    if value < 0:
        return 0.0
    if value > 1:
        return 1.0
    return round(value, 4)


def analyze_rule(article_text: str, article_summary: str) -> AnalysisResult:
    text = normalize_space(f"{article_text} {article_summary}").lower()
    nimby_terms = ("protest", "petition", "residents", "community", "nuclear", "incinerator", "landfill", "pollution")
    fear_terms = ("fear", "panic", "toxic", "health risk", "contamination")
    anger_terms = ("anger", "outrage", "accuse", "boycott")
    policy_terms = ("policy", "permit", "regulation", "hearing")

    nimby_hit = any(term in text for term in nimby_terms)
    fear_hit = any(term in text for term in fear_terms)
    anger_hit = any(term in text for term in anger_terms)
    policy_hit = any(term in text for term in policy_terms)

    if fear_hit:
        emotion = "fear"
    elif anger_hit:
        emotion = "anger"
    else:
        emotion = "neutral"

    if nimby_hit and (fear_hit or anger_hit):
        sarf_label = "high"
        risk_score = 0.85
    elif nimby_hit or policy_hit:
        sarf_label = "medium"
        risk_score = 0.55
    else:
        sarf_label = "low"
        risk_score = 0.2

    if "greenwashing" in text:
        risk_frame = "greenwashing allegation"
    elif fear_hit:
        risk_frame = "health panic"
    elif policy_hit:
        risk_frame = "policy trust crisis"
    else:
        risk_frame = "general concern"

    reason = "rule_signal:nimby" if nimby_hit else "rule_signal:weak"
    return AnalysisResult(sarf_label, reason, emotion, risk_score, risk_frame, "rule")


def analyze_llm(
    article_text: str,
    article_summary: str,
    *,
    llm_base_url: str,
    llm_api_key: str,
    llm_model: str,
    timeout: float,
) -> AnalysisResult:
    body = {
        "model": llm_model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a strict social-risk analyzer. "
                    "Return JSON only with keys: "
                    "sarf_label,sarf_reason,dominant_emotion,nimby_risk_score,risk_frame."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Analyze whether the report indicates NIMBY-like environmental protest or fear amplification. "
                    "Use SARF framing and emotion cues.\n"
                    f"article_summary={json.dumps(article_summary, ensure_ascii=False)}\n"
                    f"article_text={json.dumps(article_text, ensure_ascii=False)}\n"
                    "Return JSON: "
                    "{\"sarf_label\":\"low|medium|high\",\"sarf_reason\":\"...\","
                    "\"dominant_emotion\":\"fear|anger|sadness|neutral|hope\","
                    "\"nimby_risk_score\":0.0,\"risk_frame\":\"...\"}"
                ),
            },
        ],
        "response_format": {"type": "json_object"},
    }
    payload = call_json_api(
        url=f"{llm_base_url}/chat/completions",
        timeout=timeout,
        headers={"Authorization": f"Bearer {llm_api_key}"},
        body=body,
    )
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

    sarf_label = normalize_space(parsed.get("sarf_label") or "").lower() or "low"
    if sarf_label not in {"low", "medium", "high"}:
        sarf_label = "low"
    return AnalysisResult(
        sarf_label=sarf_label,
        sarf_reason=normalize_space(parsed.get("sarf_reason") or "llm_no_reason"),
        dominant_emotion=normalize_space(parsed.get("dominant_emotion") or "neutral").lower(),
        nimby_risk_score=clamp_risk(parsed.get("nimby_risk_score")),
        risk_frame=normalize_space(parsed.get("risk_frame") or "general concern"),
        model_name=f"llm:{llm_model}",
    )


def analyze_one(
    article_text: str,
    article_summary: str,
    *,
    mode: str,
    timeout: float,
) -> AnalysisResult:
    if mode == "rule":
        return analyze_rule(article_text, article_summary)
    llm_base_url, llm_api_key, llm_model = require_llm_env()
    try:
        return analyze_llm(
            article_text,
            article_summary,
            llm_base_url=llm_base_url,
            llm_api_key=llm_api_key,
            llm_model=llm_model,
            timeout=timeout,
        )
    except Exception:
        fallback = analyze_rule(article_text, article_summary)
        return AnalysisResult(
            sarf_label=fallback.sarf_label,
            sarf_reason="llm_failed_fallback_rule",
            dominant_emotion=fallback.dominant_emotion,
            nimby_risk_score=fallback.nimby_risk_score,
            risk_frame=fallback.risk_frame,
            model_name=fallback.model_name,
        )


def run_analysis(args: argparse.Namespace) -> int:
    mode = normalize_space(args.mode).lower()
    if mode not in ALLOWED_MODES:
        raise ValueError(f"--mode must be one of {sorted(ALLOWED_MODES)}")
    if args.hours <= 0:
        raise ValueError("--hours must be positive")
    if args.limit <= 0:
        raise ValueError("--limit must be positive")

    threshold = utc_threshold(args.hours)
    analyzed_count = 0
    emotions: list[str] = []
    risk_scores: list[float] = []

    with connect_db(args.db) as conn:
        ensure_social_analysis_columns(conn)
        rows = conn.execute(
            """
            SELECT
                id, title, article_text, article_summary, seendate_utc
            FROM social_events
            WHERE is_analyzed = 0
              AND COALESCE(seendate_utc, '') >= ?
            ORDER BY seendate_utc DESC, id DESC
            LIMIT ?
            """,
            (threshold, args.limit),
        ).fetchall()

        for row in rows:
            article_summary = normalize_space(row["article_summary"] or "")
            article_text = normalize_space(row["article_text"] or "")
            if not article_text:
                article_text = normalize_space(row["title"] or "")
            result = analyze_one(
                article_text,
                article_summary,
                mode=mode,
                timeout=args.timeout,
            )
            analyzed_at = now_utc_iso()
            conn.execute(
                """
                UPDATE social_events
                SET
                    is_analyzed = 1,
                    analyzed_at = ?,
                    analysis_model = ?,
                    sarf_label = ?,
                    sarf_reason = ?,
                    dominant_emotion = ?,
                    nimby_risk_score = ?,
                    risk_frame = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    analyzed_at,
                    result.model_name,
                    result.sarf_label,
                    result.sarf_reason,
                    result.dominant_emotion,
                    result.nimby_risk_score,
                    result.risk_frame,
                    analyzed_at,
                    int(row["id"]),
                ),
            )
            analyzed_count += 1
            emotions.append(result.dominant_emotion)
            risk_scores.append(result.nimby_risk_score)
        conn.commit()

    if analyzed_count == 0:
        print('[SUCCESS] Analyzed 0 news items | {"dominant_emotion":"none","nimby_risk_score":0.0}')
        return 0

    dominant = Counter(emotions).most_common(1)[0][0] if emotions else "neutral"
    avg_risk = round(sum(risk_scores) / len(risk_scores), 4) if risk_scores else 0.0
    print(
        f'[SUCCESS] Analyzed {analyzed_count} news items | {{"dominant_emotion":"{dominant}","nimby_risk_score":{avg_risk}}}'
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze pending GDELT social_events rows for NIMBY and SARF amplification risk."
    )
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser.add_argument("--hours", type=int, default=24, help="Only analyze rows from the last N hours.")
    parser.add_argument("--limit", type=int, default=50, help="Max rows to analyze.")
    parser.add_argument(
        "--mode",
        default="llm",
        choices=sorted(ALLOWED_MODES),
        help="Analysis mode: llm or rule.",
    )
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="LLM API timeout in seconds.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return int(run_analysis(args))
    except sqlite3.Error as exc:
        print(f"CASWARN_ERR reason=sqlite_error detail={exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"CASWARN_ERR reason=value_error detail={exc}", file=sys.stderr)
        return 1
    except RuntimeError as exc:
        print(f"CASWARN_ERR reason=runtime_error detail={exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"CASWARN_ERR reason=unexpected detail={exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
