#!/usr/bin/env python3
"""Fetch GDELT DOC API records and persist environment signals into SQLite."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen


DEFAULT_DB_FILENAME = "gdelt_environment.db"
DEFAULT_DB_PATH = os.environ.get("GDELT_ENV_DB_PATH", DEFAULT_DB_FILENAME)
DEFAULT_GDELT_API_BASE = "https://api.gdeltproject.org/api/v2/doc/doc"
DEFAULT_TIMEOUT = 20.0
DEFAULT_USER_AGENT = "listener-gdelt-doc-ingestor/1.0 (+https://github.com/tiangong-ai/skills)"
DEFAULT_LLM_TIMEOUT = 25.0
DEFAULT_GDELT_THEME_FILTERS = (
    "ENV_CLIMATECHANGE",
    "ENV_GREENHOUSE",
    "ENV_POLLUTION",
    "ENV_AIRPOLLUTION",
    "ENV_WATERPOLLUTION",
)
LEGACY_ENTRYPOINT_NOTICE = (
    "GDELT_ENV_WARN legacy_entrypoint=scripts/gdelt_fetch.py "
    "recommended=scripts/gdelt_ingest.py,scripts/gdelt_enrich.py,scripts/gdelt_summarize.py"
)
TRACKING_QUERY_PARAMS = {
    "ref",
    "source",
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
}

ALLOWED_CLASSIFY_MODES = {"none", "rule", "llm"}
RULE_KEYWORDS = (
    "climate",
    "warming",
    "decarbon",
    "carbon",
    "emission",
    "net zero",
    "biodiversity",
    "pollution",
    "air quality",
    "water quality",
    "waste",
    "recycling",
    "renewable",
    "solar",
    "wind",
    "green energy",
    "environment",
    "environmental",
    "ecology",
    "生态",
    "环境",
    "污染",
    "碳排",
    "双碳",
    "绿电",
)

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS gdelt_environment_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key TEXT NOT NULL UNIQUE,
    url_key TEXT,
    query_text TEXT NOT NULL,
    start_datetime TEXT NOT NULL,
    end_datetime TEXT NOT NULL,
    title TEXT,
    url TEXT,
    source_domain TEXT,
    source_country TEXT,
    language TEXT,
    seendate_utc TEXT,
    social_image_url TEXT,
    avg_tone REAL,
    goldstein_scale REAL,
    env_relevance INTEGER CHECK (env_relevance IN (0, 1)),
    env_label TEXT,
    env_reason TEXT,
    classifier TEXT,
    classified_at TEXT,
    raw_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gdelt_env_seen ON gdelt_environment_events(seendate_utc DESC);
CREATE INDEX IF NOT EXISTS idx_gdelt_env_relevance ON gdelt_environment_events(env_relevance, seendate_utc DESC);
CREATE INDEX IF NOT EXISTS idx_gdelt_env_query_seen ON gdelt_environment_events(query_text, seendate_utc DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_gdelt_env_url_key_unique
    ON gdelt_environment_events(url_key)
    WHERE url_key IS NOT NULL AND url_key != '';
CREATE INDEX IF NOT EXISTS idx_gdelt_env_goldstein ON gdelt_environment_events(goldstein_scale, seendate_utc DESC);
CREATE INDEX IF NOT EXISTS idx_gdelt_env_avg_tone ON gdelt_environment_events(avg_tone, seendate_utc DESC);

CREATE TABLE IF NOT EXISTS social_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url_key TEXT NOT NULL UNIQUE,
    event_key TEXT,
    title TEXT,
    url TEXT,
    source_domain TEXT,
    source_country TEXT,
    language TEXT,
    seendate_utc TEXT,
    avg_tone REAL,
    goldstein_scale REAL,
    tone_bucket TEXT,
    conflict_bucket TEXT,
    env_relevance INTEGER CHECK (env_relevance IN (0, 1)),
    env_label TEXT,
    env_reason TEXT,
    classifier TEXT,
    article_summary TEXT,
    article_text TEXT,
    is_analyzed INTEGER NOT NULL DEFAULT 0 CHECK (is_analyzed IN (0, 1)),
    analyzed_at TEXT,
    analysis_model TEXT,
    sarf_label TEXT,
    sarf_reason TEXT,
    dominant_emotion TEXT,
    nimby_risk_score REAL,
    risk_frame TEXT,
    raw_event_count INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_social_events_seen ON social_events(seendate_utc DESC);
CREATE INDEX IF NOT EXISTS idx_social_events_env ON social_events(env_relevance, seendate_utc DESC);
CREATE INDEX IF NOT EXISTS idx_social_events_conflict ON social_events(conflict_bucket, seendate_utc DESC);
"""


@dataclass(frozen=True)
class Classification:
    relevance: int | None
    label: str | None
    reason: str | None
    classifier: str | None


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_space(value: str) -> str:
    return " ".join(str(value or "").split())


def canonicalize_url(url: str) -> str:
    raw = normalize_space(url)
    if not raw:
        return ""
    try:
        parts = urlsplit(raw)
    except Exception:
        return raw
    if not parts.scheme or not parts.netloc:
        return raw

    filtered_query: list[tuple[str, str]] = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        key_lower = key.lower()
        if key_lower.startswith("utm_"):
            continue
        if key_lower in TRACKING_QUERY_PARAMS:
            continue
        filtered_query.append((key, value))

    return urlunsplit(
        (
            parts.scheme.lower(),
            parts.netloc.lower(),
            parts.path or "/",
            urlencode(filtered_query, doseq=True),
            "",
        )
    )


def require_gdelt_datetime(raw: str, label: str) -> str:
    value = normalize_space(raw)
    if not re.fullmatch(r"\d{14}", value):
        raise ValueError(f"{label} must be UTC YYYYMMDDHHMMSS, got {raw!r}")
    return value


def resolve_db_path(db_path: str) -> Path:
    raw = str(db_path or "").strip()
    if not raw:
        raw = DEFAULT_DB_PATH
    path = Path(raw).expanduser()
    looks_like_dir = raw.endswith(("/", "\\")) or path.is_dir() or path.suffix == ""
    if looks_like_dir:
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


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    ensure_schema_migrations(conn)
    conn.commit()


def table_has_column(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(str(row["name"]) == column_name for row in rows)


def ensure_schema_migrations(conn: sqlite3.Connection) -> None:
    had_avg_tone = table_has_column(conn, "gdelt_environment_events", "avg_tone")
    if not table_has_column(conn, "gdelt_environment_events", "url_key"):
        conn.execute("ALTER TABLE gdelt_environment_events ADD COLUMN url_key TEXT")
    if not had_avg_tone:
        conn.execute("ALTER TABLE gdelt_environment_events ADD COLUMN avg_tone REAL")
    if not table_has_column(conn, "gdelt_environment_events", "goldstein_scale"):
        conn.execute("ALTER TABLE gdelt_environment_events ADD COLUMN goldstein_scale REAL")
    if table_has_column(conn, "gdelt_environment_events", "tone") and not had_avg_tone:
        conn.execute("UPDATE gdelt_environment_events SET avg_tone = tone WHERE avg_tone IS NULL")

    # Backfill url_key for old rows after schema upgrade.
    rows = conn.execute(
        """
        SELECT id, url
        FROM gdelt_environment_events
        WHERE (url_key IS NULL OR url_key = '') AND url IS NOT NULL AND url != ''
        """
    ).fetchall()
    for row in rows:
        url_key = canonicalize_url(row["url"])
        if not url_key:
            continue
        conn.execute(
            "UPDATE gdelt_environment_events SET url_key = ? WHERE id = ?",
            (url_key, int(row["id"])),
        )

    social_columns: tuple[tuple[str, str], ...] = (
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
    for column_name, column_type in social_columns:
        if not table_has_column(conn, "social_events", column_name):
            conn.execute(f"ALTER TABLE social_events ADD COLUMN {column_name} {column_type}")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_social_events_analyzed ON social_events(is_analyzed, seendate_utc DESC)"
    )


def build_event_key(query_text: str, article: dict[str, Any]) -> str:
    title = normalize_space(article.get("title") or "")
    url = canonicalize_url(str(article.get("url") or "")) or normalize_space(article.get("url") or "")
    seendate = normalize_space(article.get("seendate") or "")
    basis = "|".join([normalize_space(query_text), title, url, seendate])
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def parse_float_value(raw: Any) -> float | None:
    if raw is None:
        return None
    try:
        return float(str(raw))
    except Exception:
        return None


def parse_avg_tone(article: dict[str, Any]) -> float | None:
    for key in ("avgtone", "avgTone", "tone", "AvgTone"):
        value = parse_float_value(article.get(key))
        if value is not None:
            return value
    return None


def parse_goldstein_scale(article: dict[str, Any]) -> float | None:
    for key in ("goldsteinscale", "goldsteinScale", "GoldsteinScale", "goldstein"):
        value = parse_float_value(article.get(key))
        if value is not None:
            return value
    return None


def tone_bucket(avg_tone: float | None) -> str | None:
    if avg_tone is None:
        return None
    if avg_tone >= 2.0:
        return "positive"
    if avg_tone <= -2.0:
        return "negative"
    return "neutral"


def conflict_bucket(goldstein_scale: float | None) -> str | None:
    if goldstein_scale is None:
        return None
    if goldstein_scale <= -5.0:
        return "high-conflict"
    if goldstein_scale < 0:
        return "mid-conflict"
    return "low-conflict"


def parse_mode(raw: str) -> str:
    mode = normalize_space(raw).lower()
    if mode not in ALLOWED_CLASSIFY_MODES:
        raise ValueError(f"classify mode must be one of {sorted(ALLOWED_CLASSIFY_MODES)}")
    return mode


def parse_themes(raw: str) -> list[str]:
    parts = [normalize_space(item) for item in str(raw or "").replace(";", ",").split(",")]
    deduped: list[str] = []
    seen: set[str] = set()
    for part in parts:
        value = part.lower()
        if value.startswith("theme:"):
            value = value.split(":", 1)[1]
        normalized = normalize_space(value).upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def merge_theme_filters(raw_themes: str, disable_defaults: bool) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    if not disable_defaults:
        for theme in DEFAULT_GDELT_THEME_FILTERS:
            if theme in seen:
                continue
            merged.append(theme)
            seen.add(theme)
    for theme in parse_themes(raw_themes):
        if theme in seen:
            continue
        merged.append(theme)
        seen.add(theme)
    return merged


def build_query_with_themes(base_query: str, themes: list[str]) -> str:
    query = normalize_space(base_query)
    if not themes:
        return query
    theme_clause = " OR ".join([f"theme:{theme}" for theme in themes])
    return f"({query}) AND ({theme_clause})"


def pick_article_value(article: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = normalize_space(article.get(key) or "")
        if value:
            return value
    return ""


def extract_article_summary(article: dict[str, Any], fallback_title: str) -> str:
    summary = pick_article_value(
        article,
        "summary",
        "snippet",
        "snippetText",
        "description",
        "socialtitle",
        "social_title",
    )
    return summary or normalize_space(fallback_title)


def extract_article_text(article: dict[str, Any], fallback_title: str) -> str:
    title = normalize_space(fallback_title) or pick_article_value(article, "title")
    summary = extract_article_summary(article, fallback_title)
    content = pick_article_value(article, "content", "body", "text")
    merged = " ".join([part for part in (title, summary, content) if part])
    return normalize_space(merged)


def call_json_api(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: dict[str, Any] | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    req_headers = {"User-Agent": DEFAULT_USER_AGENT}
    if headers:
        req_headers.update(headers)
    payload: bytes | None = None
    if body is not None:
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        req_headers["Content-Type"] = "application/json"
    request = Request(url=url, method=method, headers=req_headers, data=payload)

    try:
        with urlopen(request, timeout=timeout) as resp:
            text = resp.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"http_error status={exc.code} detail={detail[:500]}") from exc
    except URLError as exc:
        raise RuntimeError(f"network_error detail={exc.reason}") from exc

    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("invalid_json_response") from exc
    if not isinstance(data, dict):
        raise RuntimeError("unexpected_response_shape")
    return data


def fetch_gdelt_articles(
    *,
    query_text: str,
    start_datetime: str,
    end_datetime: str,
    max_records: int,
    gdelt_api_base: str,
    timeout: float,
) -> list[dict[str, Any]]:
    params = {
        "query": query_text,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": str(max_records),
        "startdatetime": start_datetime,
        "enddatetime": end_datetime,
    }
    url = gdelt_api_base + "?" + urlencode(params)
    payload = call_json_api(url=url, timeout=timeout)

    # GDELT DOC API commonly returns {"articles":[...]}.
    articles = payload.get("articles")
    if articles is None:
        return []
    if not isinstance(articles, list):
        raise RuntimeError("gdelt_articles_is_not_list")
    normalized: list[dict[str, Any]] = []
    for item in articles:
        if isinstance(item, dict):
            normalized.append(item)
    return normalized


def load_articles_from_fixture(path: str) -> list[dict[str, Any]]:
    file_path = Path(str(path or "").strip()).expanduser()
    if not file_path.exists():
        raise ValueError(f"--articles-json not found: {file_path}")
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"--articles-json invalid json: {file_path}") from exc

    if isinstance(payload, dict):
        raw_articles = payload.get("articles")
    elif isinstance(payload, list):
        raw_articles = payload
    else:
        raw_articles = None

    if not isinstance(raw_articles, list):
        raise ValueError("--articles-json must be a JSON list or an object with key 'articles'")
    return [item for item in raw_articles if isinstance(item, dict)]


def classify_rule(article: dict[str, Any]) -> Classification:
    text = " ".join(
        [
            normalize_space(article.get("title") or ""),
            normalize_space(article.get("url") or ""),
            normalize_space(article.get("sourcecountry") or ""),
        ]
    ).lower()
    hit = next((kw for kw in RULE_KEYWORDS if kw.lower() in text), None)
    if hit:
        return Classification(1, "environment", f"keyword_match:{hit}", "rule")
    return Classification(0, "non-environment", "no_environment_keyword_match", "rule")


def require_llm_env() -> tuple[str, str, str]:
    base_url = normalize_space(os.environ.get("LLM_API_BASE_URL") or "")
    api_key = normalize_space(os.environ.get("LLM_API_KEY") or "")
    model = normalize_space(os.environ.get("LLM_MODEL") or "")
    if not base_url:
        raise ValueError("LLM_API_BASE_URL is required when --classify-mode llm")
    if not api_key:
        raise ValueError("LLM_API_KEY is required when --classify-mode llm")
    if not model:
        raise ValueError("LLM_MODEL is required when --classify-mode llm")
    return base_url.rstrip("/"), api_key, model


def classify_llm(
    article: dict[str, Any],
    *,
    llm_base_url: str,
    llm_api_key: str,
    llm_model: str,
    llm_timeout: float,
) -> Classification:
    prompt = {
        "title": article.get("title"),
        "url": article.get("url"),
        "sourcecountry": article.get("sourcecountry"),
        "language": article.get("language"),
        "seendate": article.get("seendate"),
    }
    body = {
        "model": llm_model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a strict topic classifier. "
                    "Return JSON only: {\"is_environment\":0|1,\"label\":\"...\",\"reason\":\"...\"}."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Classify whether this news record is about environment/climate/ecology/sustainability.\n"
                    f"record={json.dumps(prompt, ensure_ascii=False)}"
                ),
            },
        ],
        "response_format": {"type": "json_object"},
    }
    payload = call_json_api(
        url=f"{llm_base_url}/chat/completions",
        method="POST",
        timeout=llm_timeout,
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

    raw_flag = parsed.get("is_environment")
    try:
        relevance = 1 if int(raw_flag) == 1 else 0
    except Exception as exc:
        raise RuntimeError("llm_invalid_is_environment") from exc

    label = normalize_space(parsed.get("label") or ("environment" if relevance else "non-environment"))
    reason = normalize_space(parsed.get("reason") or "")
    return Classification(relevance, label or None, reason or None, f"llm:{llm_model}")


def choose_classification(
    article: dict[str, Any],
    *,
    classify_mode: str,
    llm_timeout: float,
) -> Classification:
    if classify_mode == "none":
        return Classification(None, None, None, None)
    if classify_mode == "rule":
        return classify_rule(article)
    llm_base_url, llm_api_key, llm_model = require_llm_env()
    try:
        return classify_llm(
            article,
            llm_base_url=llm_base_url,
            llm_api_key=llm_api_key,
            llm_model=llm_model,
            llm_timeout=llm_timeout,
        )
    except Exception:
        # Keep ingestion running; downgrade to deterministic fallback.
        fallback = classify_rule(article)
        return Classification(fallback.relevance, fallback.label, "llm_failed_fallback_rule", fallback.classifier)


def upsert_event(
    conn: sqlite3.Connection,
    *,
    query_text: str,
    start_datetime: str,
    end_datetime: str,
    article: dict[str, Any],
    classify_mode: str,
    llm_timeout: float,
) -> bool:
    event_key = build_event_key(query_text=query_text, article=article)
    now = now_utc_iso()
    cls = choose_classification(article, classify_mode=classify_mode, llm_timeout=llm_timeout)
    seendate = normalize_space(article.get("seendate") or "") or None
    url = normalize_space(article.get("url") or "") or None
    url_key = canonicalize_url(url or "") or None
    avg_tone = parse_avg_tone(article)
    goldstein_scale = parse_goldstein_scale(article)
    before_changes = conn.total_changes

    base_values = (
        event_key,
        url_key,
        query_text,
        start_datetime,
        end_datetime,
        normalize_space(article.get("title") or "") or None,
        url,
        normalize_space(article.get("domain") or "") or None,
        normalize_space(article.get("sourcecountry") or "") or None,
        normalize_space(article.get("language") or "") or None,
        seendate,
        normalize_space(article.get("socialimage") or "") or None,
        avg_tone,
        goldstein_scale,
        cls.relevance,
        cls.label,
        cls.reason,
        cls.classifier,
        now if cls.relevance is not None else None,
        json.dumps(article, ensure_ascii=False, separators=(",", ":")),
        now,
        now,
    )

    if url_key:
        existing = conn.execute(
            "SELECT id FROM gdelt_environment_events WHERE url_key = ? ORDER BY id DESC LIMIT 1",
            (url_key,),
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE gdelt_environment_events
                SET
                    event_key=?,
                    url_key=?,
                    query_text=?,
                    start_datetime=?,
                    end_datetime=?,
                    title=?,
                    url=?,
                    source_domain=?,
                    source_country=?,
                    language=?,
                    seendate_utc=?,
                    social_image_url=?,
                    avg_tone=COALESCE(?, avg_tone),
                    goldstein_scale=COALESCE(?, goldstein_scale),
                    env_relevance=COALESCE(?, env_relevance),
                    env_label=COALESCE(?, env_label),
                    env_reason=COALESCE(?, env_reason),
                    classifier=COALESCE(?, classifier),
                    classified_at=COALESCE(?, classified_at),
                    raw_json=?,
                    updated_at=?
                WHERE id=?
                """,
                (
                    event_key,
                    url_key,
                    query_text,
                    start_datetime,
                    end_datetime,
                    normalize_space(article.get("title") or "") or None,
                    url,
                    normalize_space(article.get("domain") or "") or None,
                    normalize_space(article.get("sourcecountry") or "") or None,
                    normalize_space(article.get("language") or "") or None,
                    seendate,
                    normalize_space(article.get("socialimage") or "") or None,
                    avg_tone,
                    goldstein_scale,
                    cls.relevance,
                    cls.label,
                    cls.reason,
                    cls.classifier,
                    now if cls.relevance is not None else None,
                    json.dumps(article, ensure_ascii=False, separators=(",", ":")),
                    now,
                    int(existing["id"]),
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO gdelt_environment_events (
                    event_key, url_key, query_text, start_datetime, end_datetime, title, url, source_domain,
                    source_country, language, seendate_utc, social_image_url, avg_tone, goldstein_scale,
                    env_relevance, env_label, env_reason, classifier, classified_at, raw_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_key) DO UPDATE SET
                    title=excluded.title,
                    source_domain=excluded.source_domain,
                    source_country=excluded.source_country,
                    language=excluded.language,
                    seendate_utc=excluded.seendate_utc,
                    social_image_url=excluded.social_image_url,
                    avg_tone=COALESCE(excluded.avg_tone, gdelt_environment_events.avg_tone),
                    goldstein_scale=COALESCE(excluded.goldstein_scale, gdelt_environment_events.goldstein_scale),
                    env_relevance=COALESCE(excluded.env_relevance, gdelt_environment_events.env_relevance),
                    env_label=COALESCE(excluded.env_label, gdelt_environment_events.env_label),
                    env_reason=COALESCE(excluded.env_reason, gdelt_environment_events.env_reason),
                    classifier=COALESCE(excluded.classifier, gdelt_environment_events.classifier),
                    classified_at=COALESCE(excluded.classified_at, gdelt_environment_events.classified_at),
                    raw_json=excluded.raw_json,
                    updated_at=excluded.updated_at
                """,
                base_values,
            )
    else:
        conn.execute(
            """
            INSERT INTO gdelt_environment_events (
                event_key, url_key, query_text, start_datetime, end_datetime, title, url, source_domain,
                source_country, language, seendate_utc, social_image_url, avg_tone, goldstein_scale,
                env_relevance, env_label, env_reason, classifier, classified_at, raw_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_key) DO UPDATE SET
                title=excluded.title,
                source_domain=excluded.source_domain,
                source_country=excluded.source_country,
                language=excluded.language,
                seendate_utc=excluded.seendate_utc,
                social_image_url=excluded.social_image_url,
                avg_tone=COALESCE(excluded.avg_tone, gdelt_environment_events.avg_tone),
                goldstein_scale=COALESCE(excluded.goldstein_scale, gdelt_environment_events.goldstein_scale),
                env_relevance=COALESCE(excluded.env_relevance, gdelt_environment_events.env_relevance),
                env_label=COALESCE(excluded.env_label, gdelt_environment_events.env_label),
                env_reason=COALESCE(excluded.env_reason, gdelt_environment_events.env_reason),
                classifier=COALESCE(excluded.classifier, gdelt_environment_events.classifier),
                classified_at=COALESCE(excluded.classified_at, gdelt_environment_events.classified_at),
                raw_json=excluded.raw_json,
                updated_at=excluded.updated_at
            """,
            base_values,
        )
    return conn.total_changes > before_changes


def cmd_init_db(args: argparse.Namespace) -> int:
    with connect_db(args.db) as conn:
        init_db(conn)
    print(f"GDELT_DB_OK path={resolve_db_path(args.db)} table=gdelt_environment_events")
    return 0


def cmd_sync(args: argparse.Namespace) -> int:
    base_query = normalize_space(args.query)
    if not base_query:
        raise ValueError("--query is required")
    themes = merge_theme_filters(args.themes, args.disable_default_themes)
    query_text = build_query_with_themes(base_query, themes)
    start_datetime = require_gdelt_datetime(args.start_datetime, "--start-datetime")
    end_datetime = require_gdelt_datetime(args.end_datetime, "--end-datetime")
    if end_datetime <= start_datetime:
        raise ValueError("--end-datetime must be greater than --start-datetime")
    if args.max_records < 1 or args.max_records > 250:
        raise ValueError("--max-records must be in [1, 250]")
    classify_mode = parse_mode(args.classify_mode)

    with connect_db(args.db) as conn:
        init_db(conn)
        fixture_path = normalize_space(getattr(args, "articles_json", ""))
        if fixture_path:
            articles = load_articles_from_fixture(fixture_path)
        else:
            articles = fetch_gdelt_articles(
                query_text=query_text,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                max_records=args.max_records,
                gdelt_api_base=normalize_space(args.gdelt_api_base) or DEFAULT_GDELT_API_BASE,
                timeout=args.timeout,
            )
        inserted = 0
        for article in articles:
            if upsert_event(
                conn,
                query_text=query_text,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                article=article,
                classify_mode=classify_mode,
                llm_timeout=args.llm_timeout,
            ):
                inserted += 1
        conn.commit()
        total_rows = int(
            conn.execute("SELECT COUNT(1) AS c FROM gdelt_environment_events").fetchone()["c"]
        )
        unique_urls = int(
            conn.execute(
                "SELECT COUNT(1) AS c FROM gdelt_environment_events WHERE url_key IS NOT NULL AND url_key != ''"
            ).fetchone()["c"]
        )

    print(
        "GDELT_SYNC_OK "
        f"query={query_text!r} "
        f"start={start_datetime} end={end_datetime} "
        f"themes={themes} "
        f"fixture={fixture_path or 'none'} "
        f"fetched={len(articles)} upserted={inserted} classify_mode={classify_mode} "
        f"rows={total_rows} unique_urls={unique_urls}"
    )
    return 0


def cmd_enrich(args: argparse.Namespace) -> int:
    classify_mode = parse_mode(args.classify_mode)
    processed = 0
    updated = 0
    with connect_db(args.db) as conn:
        init_db(conn)
        rows = conn.execute(
            """
            SELECT id, raw_json, title, url, source_country, language, seendate_utc
            FROM gdelt_environment_events
            ORDER BY COALESCE(seendate_utc, updated_at) DESC, id DESC
            LIMIT ?
            """,
            (args.limit,),
        ).fetchall()
        for row in rows:
            processed += 1
            article: dict[str, Any]
            try:
                parsed = json.loads(row["raw_json"])
                article = parsed if isinstance(parsed, dict) else {}
            except Exception:
                article = {}
            if not article:
                article = {
                    "title": row["title"],
                    "url": row["url"],
                    "sourcecountry": row["source_country"],
                    "language": row["language"],
                    "seendate": row["seendate_utc"],
                }

            cls = choose_classification(article, classify_mode=classify_mode, llm_timeout=args.llm_timeout)
            before_changes = conn.total_changes
            now = now_utc_iso()
            conn.execute(
                """
                UPDATE gdelt_environment_events
                SET
                    url_key = COALESCE(NULLIF(url_key, ''), ?),
                    avg_tone = COALESCE(avg_tone, ?),
                    goldstein_scale = COALESCE(goldstein_scale, ?),
                    env_relevance = COALESCE(?, env_relevance),
                    env_label = COALESCE(?, env_label),
                    env_reason = COALESCE(?, env_reason),
                    classifier = COALESCE(?, classifier),
                    classified_at = COALESCE(?, classified_at),
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    canonicalize_url(row["url"] or "") or None,
                    parse_avg_tone(article),
                    parse_goldstein_scale(article),
                    cls.relevance,
                    cls.label,
                    cls.reason,
                    cls.classifier,
                    now if cls.relevance is not None else None,
                    now,
                    int(row["id"]),
                ),
            )
            if conn.total_changes > before_changes:
                updated += 1
        conn.commit()

    print(
        "GDELT_ENRICH_OK "
        f"processed={processed} updated={updated} classify_mode={classify_mode}"
    )
    return 0


def cmd_summarize(args: argparse.Namespace) -> int:
    processed = 0
    upserted = 0
    with connect_db(args.db) as conn:
        init_db(conn)
        where_clauses = ["url_key IS NOT NULL", "url_key != ''"]
        params: list[Any] = []
        if args.only_relevant:
            where_clauses.append("env_relevance = 1")
        if args.since_datetime:
            since = require_gdelt_datetime(args.since_datetime, "--since-datetime")
            where_clauses.append("seendate_utc >= ?")
            params.append(since)
        where_sql = " AND ".join(where_clauses)
        query = f"""
            SELECT *
            FROM gdelt_environment_events
            WHERE {where_sql}
            ORDER BY COALESCE(seendate_utc, updated_at) DESC, id DESC
            LIMIT ?
        """
        params.append(args.limit)
        rows = conn.execute(query, tuple(params)).fetchall()
        for row in rows:
            processed += 1
            before_changes = conn.total_changes
            now = now_utc_iso()
            try:
                parsed = json.loads(row["raw_json"] or "{}")
                article = parsed if isinstance(parsed, dict) else {}
            except Exception:
                article = {}
            article_summary = extract_article_summary(article, row["title"] or "")
            article_text = extract_article_text(article, row["title"] or "")
            conn.execute(
                """
                INSERT INTO social_events (
                    url_key, event_key, title, url, source_domain, source_country, language, seendate_utc,
                    avg_tone, goldstein_scale, tone_bucket, conflict_bucket, env_relevance, env_label,
                    env_reason, classifier, article_summary, article_text, raw_event_count, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                ON CONFLICT(url_key) DO UPDATE SET
                    event_key=excluded.event_key,
                    title=excluded.title,
                    url=excluded.url,
                    source_domain=excluded.source_domain,
                    source_country=excluded.source_country,
                    language=excluded.language,
                    seendate_utc=excluded.seendate_utc,
                    avg_tone=COALESCE(excluded.avg_tone, social_events.avg_tone),
                    goldstein_scale=COALESCE(excluded.goldstein_scale, social_events.goldstein_scale),
                    tone_bucket=COALESCE(excluded.tone_bucket, social_events.tone_bucket),
                    conflict_bucket=COALESCE(excluded.conflict_bucket, social_events.conflict_bucket),
                    env_relevance=COALESCE(excluded.env_relevance, social_events.env_relevance),
                    env_label=COALESCE(excluded.env_label, social_events.env_label),
                    env_reason=COALESCE(excluded.env_reason, social_events.env_reason),
                    classifier=COALESCE(excluded.classifier, social_events.classifier),
                    article_summary=COALESCE(excluded.article_summary, social_events.article_summary),
                    article_text=COALESCE(excluded.article_text, social_events.article_text),
                    raw_event_count=COALESCE(social_events.raw_event_count, 0) + 1,
                    updated_at=excluded.updated_at
                """,
                (
                    row["url_key"],
                    row["event_key"],
                    row["title"],
                    row["url"],
                    row["source_domain"],
                    row["source_country"],
                    row["language"],
                    row["seendate_utc"],
                    row["avg_tone"],
                    row["goldstein_scale"],
                    tone_bucket(row["avg_tone"]),
                    conflict_bucket(row["goldstein_scale"]),
                    row["env_relevance"],
                    row["env_label"],
                    row["env_reason"],
                    row["classifier"],
                    article_summary or None,
                    article_text or None,
                    now,
                    now,
                ),
            )
            if conn.total_changes > before_changes:
                upserted += 1
        conn.commit()
        social_rows = int(conn.execute("SELECT COUNT(1) AS c FROM social_events").fetchone()["c"])
        source_rows = int(conn.execute("SELECT COUNT(1) AS c FROM gdelt_environment_events").fetchone()["c"])

    print(
        "SOCIAL_SUMMARIZE_OK "
        f"source_rows={source_rows} processed={processed} upserted={upserted} social_rows={social_rows}"
    )
    return 0


def cmd_list_events(args: argparse.Namespace) -> int:
    with connect_db(args.db) as conn:
        rows = conn.execute(
            """
            SELECT id, seendate_utc, env_relevance, avg_tone, goldstein_scale, title, url, source_country, classifier
            FROM gdelt_environment_events
            ORDER BY COALESCE(seendate_utc, updated_at) DESC, id DESC
            LIMIT ?
            """,
            (args.limit,),
        ).fetchall()
    print("id\tseendate_utc\tenv_relevance\tavg_tone\tgoldstein_scale\tsource_country\tclassifier\ttitle\turl")
    for row in rows:
        print(
            f"{row['id']}\t{row['seendate_utc'] or ''}\t{row['env_relevance'] if row['env_relevance'] is not None else ''}\t"
            f"{row['avg_tone'] if row['avg_tone'] is not None else ''}\t{row['goldstein_scale'] if row['goldstein_scale'] is not None else ''}\t"
            f"{row['source_country'] or ''}\t{row['classifier'] or ''}\t{row['title'] or ''}\t{row['url'] or ''}"
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch GDELT environment records and persist into SQLite table gdelt_environment_events."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_init = subparsers.add_parser("init-db", help="Initialize SQLite schema.")
    parser_init.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser_init.set_defaults(func=cmd_init_db)

    parser_sync = subparsers.add_parser("sync", help="Fetch GDELT records and upsert into table.")
    parser_sync.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser_sync.add_argument("--query", required=True, help="GDELT DOC query expression.")
    parser_sync.add_argument(
        "--themes",
        default="",
        help="Comma-separated GDELT theme filters appended as OR-clause, e.g. ENV_GREENHOUSE,ENV_POLLUTION.",
    )
    parser_sync.add_argument(
        "--disable-default-themes",
        action="store_true",
        help="Disable built-in environment themes when building the DOC query.",
    )
    parser_sync.add_argument("--start-datetime", required=True, help="UTC start datetime YYYYMMDDHHMMSS.")
    parser_sync.add_argument("--end-datetime", required=True, help="UTC end datetime YYYYMMDDHHMMSS.")
    parser_sync.add_argument(
        "--classify-mode",
        default="rule",
        choices=sorted(ALLOWED_CLASSIFY_MODES),
        help="Classification mode: none, rule, llm.",
    )
    parser_sync.add_argument("--max-records", type=int, default=100, help="Max records [1,250].")
    parser_sync.add_argument(
        "--articles-json",
        default="",
        help="Optional local fixture file. JSON list or {'articles':[...]} in GDELT article shape.",
    )
    parser_sync.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="HTTP timeout seconds.")
    parser_sync.add_argument(
        "--gdelt-api-base",
        default=DEFAULT_GDELT_API_BASE,
        help="GDELT DOC API base URL.",
    )
    parser_sync.add_argument(
        "--llm-timeout",
        type=float,
        default=DEFAULT_LLM_TIMEOUT,
        help="LLM API timeout seconds when classify-mode=llm.",
    )
    parser_sync.set_defaults(func=cmd_sync)

    parser_enrich = subparsers.add_parser(
        "enrich",
        help="Clean/enrich existing rows: URL key, AvgTone/GoldsteinScale, and classification.",
    )
    parser_enrich.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser_enrich.add_argument(
        "--classify-mode",
        default="rule",
        choices=sorted(ALLOWED_CLASSIFY_MODES),
        help="Classification mode: none, rule, llm.",
    )
    parser_enrich.add_argument("--limit", type=int, default=500, help="Max rows to enrich.")
    parser_enrich.add_argument(
        "--llm-timeout",
        type=float,
        default=DEFAULT_LLM_TIMEOUT,
        help="LLM API timeout seconds when classify-mode=llm.",
    )
    parser_enrich.set_defaults(func=cmd_enrich)

    parser_summarize = subparsers.add_parser(
        "summarize",
        help="Idempotent upsert from gdelt_environment_events into social_events.",
    )
    parser_summarize.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser_summarize.add_argument("--limit", type=int, default=2000, help="Max source rows to process.")
    parser_summarize.add_argument(
        "--since-datetime",
        default="",
        help="Only process source rows with seendate_utc >= this UTC timestamp (YYYYMMDDHHMMSS).",
    )
    parser_summarize.add_argument(
        "--only-relevant",
        action="store_true",
        help="Only summarize rows where env_relevance=1.",
    )
    parser_summarize.set_defaults(func=cmd_summarize)

    parser_list = subparsers.add_parser("list-events", help="List recent stored events.")
    parser_list.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser_list.add_argument("--limit", type=int, default=50, help="Max rows to show.")
    parser_list.set_defaults(func=cmd_list_events)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    print(LEGACY_ENTRYPOINT_NOTICE, file=sys.stderr)
    try:
        return int(args.func(args))
    except sqlite3.Error as exc:
        print(f"GDELT_ENV_ERR reason=sqlite_error detail={exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"GDELT_ENV_ERR reason=value_error detail={exc}", file=sys.stderr)
        return 1
    except RuntimeError as exc:
        print(f"GDELT_ENV_ERR reason=runtime_error detail={exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"GDELT_ENV_ERR reason=unexpected detail={exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
