#!/usr/bin/env python3
"""Global AQI ingestion pipeline using OpenAQ with SQLite persistence."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_DB_FILENAME = "observer_physical.db"
DEFAULT_DB_PATH = os.environ.get("OBSERVER_DB_PATH", DEFAULT_DB_FILENAME)
DEFAULT_OPENAQ_BASE_URL = os.environ.get("OPENAQ_API_BASE_URL", "https://api.openaq.org/v3")
DEFAULT_OPENAQ_TIMEOUT = float(os.environ.get("OPENAQ_TIMEOUT_SECONDS", "20"))
DEFAULT_OPENAQ_LIMIT = int(os.environ.get("OPENAQ_PAGE_LIMIT", "100"))
DEFAULT_OPENAQ_MAX_PAGES = int(os.environ.get("OPENAQ_MAX_PAGES", "20"))
DEFAULT_OPENAQ_SLEEP_MS = int(os.environ.get("OPENAQ_SLEEP_MS", "1100"))
DEFAULT_OPENAQ_USER_AGENT = os.environ.get(
    "OPENAQ_USER_AGENT",
    "observer-openaq-physical-ingestor/1.0 (+https://github.com/tiangong-ai/skills)",
)
LEGACY_ENTRYPOINT_NOTICE = (
    "PHYSICAL_WARN legacy_entrypoint=scripts/aqi_ingest.py "
    "recommended=scripts/observer_ingest.py,scripts/observer_enrich.py,scripts/observer_summarize.py"
)

TARGET_PARAMETER_IDS = {
    "pm25": 2,
    "no2": 7,
    "o3": 10,
}

MOLAR_MASS = {
    "no2": 46.0055,
    "o3": 47.9982,
}

WHO_2021_UGM3 = {
    "pm25": 15.0,  # 24h
    "no2": 25.0,  # 24h
    "o3": 100.0,  # 8h peak-season guideline used as screening threshold
}

US_EPA_CORE = {
    "pm25": {"value": 35.0, "unit": "ug/m3", "label": "EPA PM2.5 24h"},
    "no2": {"value": 100.0, "unit": "ppb", "label": "EPA NO2 1h"},
    "o3": {"value": 0.070, "unit": "ppm", "label": "EPA O3 8h"},
}

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS aq_raw_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name TEXT NOT NULL,
    location_id INTEGER,
    location_name TEXT,
    sensor_id INTEGER NOT NULL,
    parameter_code TEXT NOT NULL,
    country_code TEXT,
    latitude REAL,
    longitude REAL,
    observed_utc TEXT NOT NULL,
    value_raw REAL,
    unit_raw TEXT,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(source_name, sensor_id, parameter_code, observed_utc)
);

CREATE INDEX IF NOT EXISTS idx_aq_raw_obs_utc ON aq_raw_observations(observed_utc DESC);
CREATE INDEX IF NOT EXISTS idx_aq_raw_param_utc ON aq_raw_observations(parameter_code, observed_utc DESC);
CREATE INDEX IF NOT EXISTS idx_aq_raw_country_utc ON aq_raw_observations(country_code, observed_utc DESC);

CREATE TABLE IF NOT EXISTS aq_enriched_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_id INTEGER NOT NULL UNIQUE,
    standard_profile TEXT NOT NULL,
    standard_name TEXT,
    standard_unit TEXT,
    threshold_value REAL,
    threshold_ugm3 REAL,
    value_ugm3 REAL,
    variance_ratio REAL,
    is_exceed INTEGER CHECK (is_exceed IN (0, 1)),
    note TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(raw_id) REFERENCES aq_raw_observations(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_aq_enriched_exceed ON aq_enriched_observations(is_exceed);
CREATE INDEX IF NOT EXISTS idx_aq_enriched_profile ON aq_enriched_observations(standard_profile);

CREATE TABLE IF NOT EXISTS physical_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_key TEXT NOT NULL UNIQUE,
    metric_date TEXT NOT NULL,
    country_code TEXT,
    parameter_code TEXT NOT NULL,
    standard_profile TEXT NOT NULL,
    sample_count INTEGER NOT NULL,
    avg_value_ugm3 REAL,
    max_value_ugm3 REAL,
    exceed_count INTEGER NOT NULL,
    exceed_rate REAL,
    max_variance_ratio REAL,
    source_row_count INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_physical_metrics_date ON physical_metrics(metric_date DESC);
CREATE INDEX IF NOT EXISTS idx_physical_metrics_param ON physical_metrics(parameter_code, metric_date DESC);
CREATE INDEX IF NOT EXISTS idx_physical_metrics_country ON physical_metrics(country_code, metric_date DESC);
"""


@dataclass(frozen=True)
class RuntimeConfig:
    openaq_base_url: str
    openaq_api_key: str
    timeout: float
    limit: int
    max_pages: int
    sleep_ms: int
    user_agent: str


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_space(value: str) -> str:
    return " ".join(str(value or "").split())


def parse_iso_datetime(raw: str, label: str) -> str:
    text = normalize_space(raw)
    if not text:
        raise ValueError(f"{label} is required")
    candidate = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError(f"{label} must be ISO-8601 datetime, got {raw!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_db_path(db_path: str) -> Path:
    raw = normalize_space(db_path)
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
    conn.commit()


def parse_bbox(raw: str) -> tuple[float, float, float, float]:
    parts = [normalize_space(x) for x in str(raw).split(",")]
    if len(parts) != 4:
        raise ValueError("--bbox must be min_lon,min_lat,max_lon,max_lat")
    values = tuple(float(x) for x in parts)
    min_lon, min_lat, max_lon, max_lat = values
    if not (-180 <= min_lon <= 180 and -180 <= max_lon <= 180 and -90 <= min_lat <= 90 and -90 <= max_lat <= 90):
        raise ValueError("--bbox coordinates out of WGS84 bounds")
    if min_lon >= max_lon or min_lat >= max_lat:
        raise ValueError("--bbox must satisfy min_lon<max_lon and min_lat<max_lat")
    return values


def load_runtime_config(args: argparse.Namespace) -> RuntimeConfig:
    api_key = normalize_space(args.openaq_api_key or os.environ.get("OPENAQ_API_KEY") or "")
    if not api_key:
        raise ValueError("OPENAQ_API_KEY is required (env or --openaq-api-key)")
    return RuntimeConfig(
        openaq_base_url=normalize_space(args.openaq_base_url) or DEFAULT_OPENAQ_BASE_URL,
        openaq_api_key=api_key,
        timeout=float(args.timeout),
        limit=int(args.limit),
        max_pages=int(args.max_pages),
        sleep_ms=max(int(args.sleep_ms), 0),
        user_agent=normalize_space(args.user_agent) or DEFAULT_OPENAQ_USER_AGENT,
    )


def load_fixture_records(path: str) -> list[dict[str, Any]]:
    file_path = Path(str(path or "").strip()).expanduser()
    if not file_path.exists():
        raise ValueError(f"--fixture-json not found: {file_path}")
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"--fixture-json invalid json: {file_path}") from exc

    records: Any
    if isinstance(payload, dict):
        records = payload.get("records")
    elif isinstance(payload, list):
        records = payload
    else:
        records = None
    if not isinstance(records, list):
        raise ValueError("--fixture-json must be a JSON list or an object with key 'records'")
    normalized: list[dict[str, Any]] = []
    for item in records:
        if isinstance(item, dict):
            normalized.append(item)
    return normalized


def openaq_get(config: RuntimeConfig, path: str, query: dict[str, Any]) -> dict[str, Any]:
    cleaned = {k: v for k, v in query.items() if v is not None and str(v) != ""}
    url = config.openaq_base_url.rstrip("/") + path + "?" + urlencode(cleaned, doseq=True)
    req = Request(
        url=url,
        method="GET",
        headers={
            "X-API-Key": config.openaq_api_key,
            "User-Agent": config.user_agent,
            "Accept": "application/json",
        },
    )
    try:
        with urlopen(req, timeout=config.timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"openaq_http_error status={exc.code} detail={detail[:300]}") from exc
    except URLError as exc:
        raise RuntimeError(f"openaq_network_error detail={exc.reason}") from exc

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("openaq_invalid_json") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("openaq_invalid_shape")
    return payload


def maybe_sleep_ms(ms: int) -> None:
    if ms > 0:
        time.sleep(ms / 1000.0)


def extract_country_code(location: dict[str, Any]) -> str | None:
    country = location.get("country")
    if isinstance(country, dict):
        code = normalize_space(country.get("code") or "")
        return code.upper() or None
    code = normalize_space(location.get("countryCode") or "")
    return code.upper() or None


def extract_lat_lon(location: dict[str, Any]) -> tuple[float | None, float | None]:
    coordinates = location.get("coordinates")
    if isinstance(coordinates, dict):
        lat = coordinates.get("latitude")
        lon = coordinates.get("longitude")
        try:
            return (float(lat), float(lon))
        except Exception:
            return (None, None)
    return (None, None)


def extract_parameter_code(sensor: dict[str, Any]) -> str | None:
    parameter = sensor.get("parameter")
    if isinstance(parameter, dict):
        name = normalize_space(parameter.get("name") or "")
        return name.lower() or None
    return None


def extract_measurement_value(row: dict[str, Any]) -> tuple[float | None, str | None]:
    value = row.get("value")
    unit = normalize_space(row.get("unit") or "")
    if not unit and isinstance(row.get("parameter"), dict):
        unit = normalize_space(row["parameter"].get("units") or "")
    if value is None and isinstance(row.get("summary"), dict):
        value = row["summary"].get("avg")
    try:
        val = float(value) if value is not None else None
    except Exception:
        val = None
    return val, unit or None


def extract_measurement_time(row: dict[str, Any]) -> str | None:
    # Supports OpenAQ measurements/hours response variants.
    dt = row.get("datetime")
    if isinstance(dt, dict):
        utc_value = normalize_space(dt.get("utc") or "")
        if utc_value:
            return parse_iso_datetime(utc_value, "measurement.datetime.utc")

    # OpenAQ v3 hours payloads commonly provide interval boundaries in `period` or `coverage`.
    for key in ("period", "coverage"):
        block = row.get(key)
        if isinstance(block, dict):
            for nested_key in ("datetimeFrom", "datetimeTo"):
                nested = block.get(nested_key)
                if isinstance(nested, dict):
                    utc_value = normalize_space(nested.get("utc") or "")
                    if utc_value:
                        return parse_iso_datetime(utc_value, f"measurement.{key}.{nested_key}.utc")

    utc_value = normalize_space(row.get("datetimeUtc") or row.get("date") or "")
    if utc_value:
        return parse_iso_datetime(utc_value, "measurement.datetime")
    return None


def fetch_location_sensors(
    config: RuntimeConfig,
    location_id: int,
    *,
    max_pages: int,
) -> list[dict[str, Any]]:
    sensors: list[dict[str, Any]] = []
    for page in range(1, max_pages + 1):
        payload = openaq_get(
            config,
            f"/locations/{location_id}/sensors",
            {"limit": config.limit, "page": page},
        )
        items = payload.get("results")
        if not isinstance(items, list) or not items:
            break
        sensors.extend([x for x in items if isinstance(x, dict)])
        if len(items) < config.limit:
            break
        maybe_sleep_ms(config.sleep_ms)
    return sensors


def fetch_sensor_rows(
    config: RuntimeConfig,
    sensor_id: int,
    *,
    datetime_from: str,
    datetime_to: str,
    endpoint: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for page in range(1, config.max_pages + 1):
        payload = openaq_get(
            config,
            f"/sensors/{sensor_id}/{endpoint}",
            {
                "datetime_from": datetime_from,
                "datetime_to": datetime_to,
                "date_from": datetime_from,
                "date_to": datetime_to,
                "limit": config.limit,
                "page": page,
            },
        )
        results = payload.get("results")
        if not isinstance(results, list) or not results:
            break
        rows.extend([x for x in results if isinstance(x, dict)])
        if len(results) < config.limit:
            break
        maybe_sleep_ms(config.sleep_ms)
    return rows


def choose_standard_profile(country_code: str | None, forced_profile: str) -> str:
    profile = normalize_space(forced_profile).lower()
    if profile in ("who_2021", "us_epa_core"):
        return profile
    if (country_code or "").upper() == "US":
        return "us_epa_core"
    return "who_2021"


def normalize_unit(unit: str | None) -> str:
    text = normalize_space(unit or "").lower()
    text = text.replace("μ", "u").replace("µ", "u")
    text = text.replace("m³", "m3").replace("m^3", "m3")
    return text


def convert_to_ugm3(parameter_code: str, value: float | None, unit_raw: str | None) -> tuple[float | None, str]:
    if value is None:
        return None, "missing_value"
    param = normalize_space(parameter_code).lower()
    unit = normalize_unit(unit_raw)
    if unit in ("ug/m3", "ugm3", "µg/m3", "μg/m3"):
        return value, "as_is_ugm3"
    if unit == "ppm":
        if param not in MOLAR_MASS:
            return None, "ppm_unsupported_parameter"
        ppb = value * 1000.0
        ugm3 = ppb * MOLAR_MASS[param] / 24.45
        return ugm3, "ppm_to_ugm3_25c_1atm"
    if unit == "ppb":
        if param not in MOLAR_MASS:
            return None, "ppb_unsupported_parameter"
        ugm3 = value * MOLAR_MASS[param] / 24.45
        return ugm3, "ppb_to_ugm3_25c_1atm"
    return None, f"unsupported_unit:{unit_raw or ''}"


def threshold_for_profile(parameter_code: str, profile: str) -> tuple[float | None, str | None, float | None, str | None]:
    param = normalize_space(parameter_code).lower()
    if profile == "who_2021":
        threshold = WHO_2021_UGM3.get(param)
        if threshold is None:
            return None, None, None, "parameter_not_in_who_profile"
        return threshold, "ug/m3", threshold, f"WHO 2021 {param}"

    if profile == "us_epa_core":
        config = US_EPA_CORE.get(param)
        if config is None:
            return None, None, None, "parameter_not_in_us_epa_profile"
        unit = str(config["unit"])
        value = float(config["value"])
        if unit == "ug/m3":
            return value, unit, value, str(config["label"])
        converted, note = convert_to_ugm3(param, value, unit)
        return value, unit, converted, str(config["label"]) + f" ({note})"

    return None, None, None, "unsupported_profile"


def upsert_raw_row(
    conn: sqlite3.Connection,
    *,
    location: dict[str, Any],
    sensor: dict[str, Any],
    parameter_code: str,
    row: dict[str, Any],
) -> bool:
    location_id = location.get("id")
    sensor_id = sensor.get("id")
    if location_id is None or sensor_id is None:
        return False
    observed_utc = extract_measurement_time(row)
    if not observed_utc:
        return False
    val, unit = extract_measurement_value(row)
    country_code = extract_country_code(location)
    lat, lon = extract_lat_lon(location)
    now = now_utc_iso()
    before = conn.total_changes
    conn.execute(
        """
        INSERT INTO aq_raw_observations (
            source_name, location_id, location_name, sensor_id, parameter_code, country_code,
            latitude, longitude, observed_utc, value_raw, unit_raw, payload_json, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_name, sensor_id, parameter_code, observed_utc) DO UPDATE SET
            location_name=excluded.location_name,
            country_code=excluded.country_code,
            latitude=excluded.latitude,
            longitude=excluded.longitude,
            value_raw=excluded.value_raw,
            unit_raw=excluded.unit_raw,
            payload_json=excluded.payload_json,
            updated_at=excluded.updated_at
        """,
        (
            "openaq",
            int(location_id),
            normalize_space(location.get("name") or "") or None,
            int(sensor_id),
            parameter_code,
            country_code,
            lat,
            lon,
            observed_utc,
            val,
            unit,
            json.dumps(row, ensure_ascii=False, separators=(",", ":")),
            now,
            now,
        ),
    )
    return conn.total_changes > before


def cmd_init_db(args: argparse.Namespace) -> int:
    with connect_db(args.db) as conn:
        init_db(conn)
    print(f"PHYSICAL_DB_OK path={resolve_db_path(args.db)} tables=aq_raw_observations,aq_enriched_observations,physical_metrics")
    return 0


def cmd_ingest(args: argparse.Namespace) -> int:
    min_lon, min_lat, max_lon, max_lat = parse_bbox(args.bbox)
    start_utc = parse_iso_datetime(args.start_datetime, "--start-datetime")
    end_utc = parse_iso_datetime(args.end_datetime, "--end-datetime")
    if end_utc <= start_utc:
        raise ValueError("--end-datetime must be later than --start-datetime")
    fixture_path = normalize_space(getattr(args, "fixture_json", ""))
    config = load_runtime_config(args) if not fixture_path else None

    with connect_db(args.db) as conn:
        init_db(conn)
        raw_upserted = 0
        sensors_scanned = 0
        rows_fetched = 0
        if fixture_path:
            records = load_fixture_records(fixture_path)
            locations = []
            for rec in records:
                location = rec.get("location")
                sensor = rec.get("sensor")
                row = rec.get("row")
                parameter_code = normalize_space(rec.get("parameter_code") or "").lower()
                if not isinstance(location, dict) or not isinstance(sensor, dict) or not isinstance(row, dict):
                    continue
                if parameter_code not in TARGET_PARAMETER_IDS:
                    parameter_code = normalize_space(extract_parameter_code(sensor) or "").lower()
                if parameter_code not in TARGET_PARAMETER_IDS:
                    continue
                locations.append(location)
                sensors_scanned += 1
                rows_fetched += 1
                if upsert_raw_row(conn, location=location, sensor=sensor, parameter_code=parameter_code, row=row):
                    raw_upserted += 1
        else:
            assert config is not None
            locations = []
            for page in range(1, config.max_pages + 1):
                payload = openaq_get(
                    config,
                    "/locations",
                    {
                        "bbox": f"{min_lon},{min_lat},{max_lon},{max_lat}",
                        "parameters_id": [TARGET_PARAMETER_IDS["pm25"], TARGET_PARAMETER_IDS["no2"], TARGET_PARAMETER_IDS["o3"]],
                        "limit": config.limit,
                        "page": page,
                    },
                )
                items = payload.get("results")
                if not isinstance(items, list) or not items:
                    break
                locations.extend([x for x in items if isinstance(x, dict)])
                if len(items) < config.limit or len(locations) >= args.max_locations:
                    break
                maybe_sleep_ms(config.sleep_ms)

            locations = locations[: args.max_locations]
            for location in locations:
                location_id = location.get("id")
                if location_id is None:
                    continue
                sensors = fetch_location_sensors(config, int(location_id), max_pages=min(config.max_pages, 5))
                kept: list[dict[str, Any]] = []
                for sensor in sensors:
                    code = extract_parameter_code(sensor)
                    if code in TARGET_PARAMETER_IDS:
                        kept.append(sensor)
                kept = kept[: args.max_sensors_per_location]
                for sensor in kept:
                    sensors_scanned += 1
                    code = extract_parameter_code(sensor)
                    if not code:
                        continue
                    try:
                        values = fetch_sensor_rows(
                            config,
                            int(sensor["id"]),
                            datetime_from=start_utc,
                            datetime_to=end_utc,
                            endpoint="hours",
                        )
                        if not values:
                            values = fetch_sensor_rows(
                                config,
                                int(sensor["id"]),
                                datetime_from=start_utc,
                                datetime_to=end_utc,
                                endpoint="measurements",
                            )
                    except RuntimeError:
                        continue
                    rows_fetched += len(values)
                    for row in values:
                        if upsert_raw_row(conn, location=location, sensor=sensor, parameter_code=code, row=row):
                            raw_upserted += 1
                    maybe_sleep_ms(config.sleep_ms)

        conn.commit()
        raw_total = int(conn.execute("SELECT COUNT(1) AS c FROM aq_raw_observations").fetchone()["c"])
        country_count = int(
            conn.execute("SELECT COUNT(DISTINCT country_code) AS c FROM aq_raw_observations WHERE country_code IS NOT NULL")
            .fetchone()["c"]
        )

    print(
        "PHYSICAL_INGEST_OK "
        f"bbox={min_lon},{min_lat},{max_lon},{max_lat} "
        f"start={start_utc} end={end_utc} "
        f"fixture={fixture_path or 'none'} "
        f"locations={len(locations)} sensors={sensors_scanned} fetched={rows_fetched} "
        f"upserted={raw_upserted} raw_total={raw_total} countries={country_count}"
    )
    return 0


def cmd_enrich(args: argparse.Namespace) -> int:
    start_utc = parse_iso_datetime(args.start_datetime, "--start-datetime") if args.start_datetime else ""
    end_utc = parse_iso_datetime(args.end_datetime, "--end-datetime") if args.end_datetime else ""
    if start_utc and end_utc and end_utc <= start_utc:
        raise ValueError("--end-datetime must be later than --start-datetime")

    with connect_db(args.db) as conn:
        init_db(conn)
        where = []
        params: list[Any] = []
        if start_utc:
            where.append("r.observed_utc >= ?")
            params.append(start_utc)
        if end_utc:
            where.append("r.observed_utc <= ?")
            params.append(end_utc)
        where_sql = " AND ".join(where)
        if where_sql:
            where_sql = "WHERE " + where_sql
        rows = conn.execute(
            f"""
            SELECT r.*
            FROM aq_raw_observations r
            {where_sql}
            ORDER BY r.observed_utc DESC, r.id DESC
            LIMIT ?
            """,
            tuple(params + [args.limit]),
        ).fetchall()

        processed = 0
        upserted = 0
        exceeded = 0
        for row in rows:
            processed += 1
            profile = choose_standard_profile(row["country_code"], args.standard_profile)
            value_ugm3, convert_note = convert_to_ugm3(row["parameter_code"], row["value_raw"], row["unit_raw"])
            threshold_value, threshold_unit, threshold_ugm3, standard_name = threshold_for_profile(row["parameter_code"], profile)
            variance_ratio = None
            is_exceed = None
            note_parts = [convert_note]
            if value_ugm3 is not None and threshold_ugm3 is not None and threshold_ugm3 > 0:
                variance_ratio = value_ugm3 / threshold_ugm3
                is_exceed = 1 if variance_ratio > 1.0 else 0
                if is_exceed == 1:
                    exceeded += 1
            else:
                note_parts.append("threshold_or_value_missing")
            note = ",".join([x for x in note_parts if x])
            now = now_utc_iso()
            before = conn.total_changes
            conn.execute(
                """
                INSERT INTO aq_enriched_observations (
                    raw_id, standard_profile, standard_name, standard_unit, threshold_value, threshold_ugm3,
                    value_ugm3, variance_ratio, is_exceed, note, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(raw_id) DO UPDATE SET
                    standard_profile=excluded.standard_profile,
                    standard_name=excluded.standard_name,
                    standard_unit=excluded.standard_unit,
                    threshold_value=excluded.threshold_value,
                    threshold_ugm3=excluded.threshold_ugm3,
                    value_ugm3=excluded.value_ugm3,
                    variance_ratio=excluded.variance_ratio,
                    is_exceed=excluded.is_exceed,
                    note=excluded.note,
                    updated_at=excluded.updated_at
                """,
                (
                    int(row["id"]),
                    profile,
                    standard_name,
                    threshold_unit,
                    threshold_value,
                    threshold_ugm3,
                    value_ugm3,
                    variance_ratio,
                    is_exceed,
                    note,
                    now,
                    now,
                ),
            )
            if conn.total_changes > before:
                upserted += 1
        conn.commit()
        enriched_total = int(conn.execute("SELECT COUNT(1) AS c FROM aq_enriched_observations").fetchone()["c"])

    print(
        "PHYSICAL_ENRICH_OK "
        f"processed={processed} upserted={upserted} exceeded={exceeded} enriched_total={enriched_total}"
    )
    return 0


def cmd_summarize(args: argparse.Namespace) -> int:
    start_utc = parse_iso_datetime(args.start_datetime, "--start-datetime") if args.start_datetime else ""
    end_utc = parse_iso_datetime(args.end_datetime, "--end-datetime") if args.end_datetime else ""
    if start_utc and end_utc and end_utc <= start_utc:
        raise ValueError("--end-datetime must be later than --start-datetime")

    with connect_db(args.db) as conn:
        init_db(conn)
        where = []
        params: list[Any] = []
        if start_utc:
            where.append("r.observed_utc >= ?")
            params.append(start_utc)
        if end_utc:
            where.append("r.observed_utc <= ?")
            params.append(end_utc)
        if args.only_exceed:
            where.append("COALESCE(e.is_exceed, 0) = 1")
        where_sql = " AND ".join(where)
        if where_sql:
            where_sql = "WHERE " + where_sql

        rows = conn.execute(
            f"""
            SELECT
                substr(r.observed_utc, 1, 10) AS metric_date,
                COALESCE(r.country_code, '') AS country_code,
                r.parameter_code,
                e.standard_profile,
                COUNT(1) AS sample_count,
                AVG(e.value_ugm3) AS avg_value_ugm3,
                MAX(e.value_ugm3) AS max_value_ugm3,
                SUM(CASE WHEN COALESCE(e.is_exceed, 0) = 1 THEN 1 ELSE 0 END) AS exceed_count,
                AVG(COALESCE(e.is_exceed, 0)) AS exceed_rate,
                MAX(e.variance_ratio) AS max_variance_ratio,
                COUNT(1) AS source_row_count
            FROM aq_raw_observations r
            JOIN aq_enriched_observations e ON e.raw_id = r.id
            {where_sql}
            GROUP BY substr(r.observed_utc, 1, 10), COALESCE(r.country_code, ''), r.parameter_code, e.standard_profile
            ORDER BY metric_date DESC
            LIMIT ?
            """,
            tuple(params + [args.group_limit]),
        ).fetchall()

        upserted = 0
        for row in rows:
            metric_key = "|".join(
                [
                    row["metric_date"] or "",
                    row["country_code"] or "",
                    row["parameter_code"] or "",
                    row["standard_profile"] or "",
                ]
            )
            now = now_utc_iso()
            before = conn.total_changes
            conn.execute(
                """
                INSERT INTO physical_metrics (
                    metric_key, metric_date, country_code, parameter_code, standard_profile, sample_count,
                    avg_value_ugm3, max_value_ugm3, exceed_count, exceed_rate, max_variance_ratio,
                    source_row_count, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(metric_key) DO UPDATE SET
                    sample_count=excluded.sample_count,
                    avg_value_ugm3=excluded.avg_value_ugm3,
                    max_value_ugm3=excluded.max_value_ugm3,
                    exceed_count=excluded.exceed_count,
                    exceed_rate=excluded.exceed_rate,
                    max_variance_ratio=excluded.max_variance_ratio,
                    source_row_count=excluded.source_row_count,
                    updated_at=excluded.updated_at
                """,
                (
                    metric_key,
                    row["metric_date"],
                    row["country_code"] or None,
                    row["parameter_code"],
                    row["standard_profile"] or "unknown",
                    int(row["sample_count"] or 0),
                    row["avg_value_ugm3"],
                    row["max_value_ugm3"],
                    int(row["exceed_count"] or 0),
                    float(row["exceed_rate"] or 0),
                    row["max_variance_ratio"],
                    int(row["source_row_count"] or 0),
                    now,
                    now,
                ),
            )
            if conn.total_changes > before:
                upserted += 1
        conn.commit()
        raw_rows = int(conn.execute("SELECT COUNT(1) AS c FROM aq_raw_observations").fetchone()["c"])
        enriched_rows = int(conn.execute("SELECT COUNT(1) AS c FROM aq_enriched_observations").fetchone()["c"])
        metric_rows = int(conn.execute("SELECT COUNT(1) AS c FROM physical_metrics").fetchone()["c"])

    print(
        "PHYSICAL_SUMMARY_OK "
        f"raw_rows={raw_rows} enriched_rows={enriched_rows} groups={len(rows)} upserted={upserted} metric_rows={metric_rows}"
    )
    return 0


def cmd_list_metrics(args: argparse.Namespace) -> int:
    with connect_db(args.db) as conn:
        rows = conn.execute(
            """
            SELECT metric_date, country_code, parameter_code, standard_profile, sample_count,
                   avg_value_ugm3, max_value_ugm3, exceed_count, exceed_rate, max_variance_ratio
            FROM physical_metrics
            ORDER BY metric_date DESC, country_code, parameter_code
            LIMIT ?
            """,
            (args.limit,),
        ).fetchall()
    print("metric_date\tcountry\tparameter\tprofile\tsamples\tavg_ugm3\tmax_ugm3\texceed_count\texceed_rate\tmax_ratio")
    for row in rows:
        print(
            f"{row['metric_date']}\t{row['country_code'] or ''}\t{row['parameter_code']}\t{row['standard_profile']}\t"
            f"{row['sample_count']}\t{row['avg_value_ugm3'] if row['avg_value_ugm3'] is not None else ''}\t"
            f"{row['max_value_ugm3'] if row['max_value_ugm3'] is not None else ''}\t{row['exceed_count']}\t"
            f"{row['exceed_rate'] if row['exceed_rate'] is not None else ''}\t{row['max_variance_ratio'] if row['max_variance_ratio'] is not None else ''}"
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest and summarize global physical air-quality metrics from OpenAQ into SQLite."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_init = subparsers.add_parser("init-db", help="Initialize SQLite schema.")
    parser_init.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser_init.set_defaults(func=cmd_init_db)

    parser_ingest = subparsers.add_parser("ingest", help="Ingest OpenAQ rows by bbox and datetime range.")
    parser_ingest.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser_ingest.add_argument("--bbox", required=True, help="min_lon,min_lat,max_lon,max_lat")
    parser_ingest.add_argument("--start-datetime", required=True, help="UTC ISO-8601 datetime.")
    parser_ingest.add_argument("--end-datetime", required=True, help="UTC ISO-8601 datetime.")
    parser_ingest.add_argument("--max-locations", type=int, default=200, help="Max locations to scan.")
    parser_ingest.add_argument(
        "--max-sensors-per-location",
        type=int,
        default=10,
        help="Max target sensors per location.",
    )
    parser_ingest.add_argument("--openaq-api-base", dest="openaq_base_url", default=DEFAULT_OPENAQ_BASE_URL)
    parser_ingest.add_argument("--openaq-api-key", default="")
    parser_ingest.add_argument("--timeout", type=float, default=DEFAULT_OPENAQ_TIMEOUT)
    parser_ingest.add_argument("--limit", type=int, default=DEFAULT_OPENAQ_LIMIT)
    parser_ingest.add_argument("--max-pages", type=int, default=DEFAULT_OPENAQ_MAX_PAGES)
    parser_ingest.add_argument("--sleep-ms", type=int, default=DEFAULT_OPENAQ_SLEEP_MS)
    parser_ingest.add_argument("--user-agent", default=DEFAULT_OPENAQ_USER_AGENT)
    parser_ingest.add_argument(
        "--fixture-json",
        default="",
        help="Optional local fixture file. JSON list or {'records':[...]} with location/sensor/row fields.",
    )
    parser_ingest.set_defaults(func=cmd_ingest)

    parser_enrich = subparsers.add_parser(
        "enrich",
        help="Flatten and compare observations against WHO/EPA thresholds.",
    )
    parser_enrich.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser_enrich.add_argument("--start-datetime", default="", help="Optional UTC ISO-8601 lower bound.")
    parser_enrich.add_argument("--end-datetime", default="", help="Optional UTC ISO-8601 upper bound.")
    parser_enrich.add_argument(
        "--standard-profile",
        default="auto",
        choices=["auto", "who_2021", "us_epa_core"],
        help="auto: US->EPA else WHO",
    )
    parser_enrich.add_argument("--limit", type=int, default=100000, help="Max rows to process.")
    parser_enrich.set_defaults(func=cmd_enrich)

    parser_summarize = subparsers.add_parser(
        "summarize",
        help="Idempotent upsert aggregated rows into physical_metrics.",
    )
    parser_summarize.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser_summarize.add_argument("--start-datetime", default="", help="Optional UTC ISO-8601 lower bound.")
    parser_summarize.add_argument("--end-datetime", default="", help="Optional UTC ISO-8601 upper bound.")
    parser_summarize.add_argument("--only-exceed", action="store_true", help="Summarize exceeded rows only.")
    parser_summarize.add_argument("--group-limit", type=int, default=200000, help="Max grouped rows.")
    parser_summarize.set_defaults(func=cmd_summarize)

    parser_list = subparsers.add_parser("list-metrics", help="List aggregated physical metrics.")
    parser_list.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser_list.add_argument("--limit", type=int, default=100, help="Rows to display.")
    parser_list.set_defaults(func=cmd_list_metrics)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    print(LEGACY_ENTRYPOINT_NOTICE, file=sys.stderr)
    try:
        return int(args.func(args))
    except sqlite3.Error as exc:
        print(f"PHYSICAL_ERR reason=sqlite_error detail={exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"PHYSICAL_ERR reason=value_error detail={exc}", file=sys.stderr)
        return 1
    except RuntimeError as exc:
        print(f"PHYSICAL_ERR reason=runtime_error detail={exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # pragma: no cover - defensive
        print(f"PHYSICAL_ERR reason=unexpected detail={exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
