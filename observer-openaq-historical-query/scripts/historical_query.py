#!/usr/bin/env python3
"""Load targeted historical physical rows from archive SQLite into active observer DB."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from common.human_log import HumanLogger, default_skill_log_root

SKILL_NAME = "observer-openaq-historical-query"
LOGGER: HumanLogger | None = None


def log_event(category: str, summary: str, details: dict[str, object] | None = None) -> None:
    if LOGGER is not None:
        LOGGER.log(category=category, summary=summary, details=details or {})


def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def parse_bbox(raw: str) -> tuple[float, float, float, float]:
    parts = [x.strip() for x in raw.split(",")]
    if len(parts) != 4:
        raise ValueError("--bbox must be min_lon,min_lat,max_lon,max_lat")
    min_lon, min_lat, max_lon, max_lat = [float(x) for x in parts]
    return min_lon, min_lat, max_lon, max_lat


def ensure_target_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
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
        )
        """
    )


def cmd_ingest(args: argparse.Namespace) -> int:
    log_event("workflow_start", "Run historical ingest", {"args": vars(args)})
    min_lon, min_lat, max_lon, max_lat = parse_bbox(args.bbox)
    source_db = Path(args.archive_db).expanduser()
    target_db = Path(args.db).expanduser()
    if not source_db.exists():
        raise ValueError(f"archive db not found: {source_db}")

    with connect(str(source_db)) as src, connect(str(target_db)) as dst:
        ensure_target_table(dst)
        rows = src.execute(
            """
            SELECT source_name, location_id, location_name, sensor_id, parameter_code, country_code,
                   latitude, longitude, observed_utc, value_raw, unit_raw, payload_json, created_at, updated_at
            FROM aq_raw_observations
            WHERE observed_utc >= ? AND observed_utc <= ?
              AND longitude >= ? AND longitude <= ?
              AND latitude >= ? AND latitude <= ?
              AND parameter_code IN ('pm25','no2','o3')
            ORDER BY observed_utc DESC
            LIMIT ?
            """,
            (
                args.start_datetime,
                args.end_datetime,
                min_lon,
                max_lon,
                min_lat,
                max_lat,
                max(1, args.limit),
            ),
        ).fetchall()

        before = dst.total_changes
        for row in rows:
            dst.execute(
                """
                INSERT INTO aq_raw_observations (
                    source_name, location_id, location_name, sensor_id, parameter_code, country_code,
                    latitude, longitude, observed_utc, value_raw, unit_raw, payload_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_name, sensor_id, parameter_code, observed_utc) DO UPDATE SET
                    value_raw=excluded.value_raw,
                    unit_raw=excluded.unit_raw,
                    payload_json=excluded.payload_json,
                    updated_at=excluded.updated_at
                """,
                (
                    str(row["source_name"] or "openaq_archive"),
                    row["location_id"],
                    row["location_name"],
                    int(row["sensor_id"]),
                    row["parameter_code"],
                    row["country_code"],
                    row["latitude"],
                    row["longitude"],
                    row["observed_utc"],
                    row["value_raw"],
                    row["unit_raw"],
                    row["payload_json"],
                    row["created_at"],
                    row["updated_at"],
                ),
            )
        dst.commit()
        upserted = dst.total_changes - before

    print(
        "PHYSICAL_INGEST_OK "
        f"source=openaq_archive_db archive_db={source_db} target_db={target_db} "
        f"start={args.start_datetime} end={args.end_datetime} "
        f"bbox={min_lon},{min_lat},{max_lon},{max_lat} selected={len(rows)} upserted={upserted}"
    )
    log_event(
        "workflow_end",
        "Run historical ingest completed",
        {"selected": len(rows), "upserted": upserted, "source_db": str(source_db), "target_db": str(target_db)},
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Load historical rows from archive SQLite into active observer DB.")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="Query archive DB and upsert into active observer DB.")
    ingest.add_argument("--db", required=True, help="Target observer SQLite path.")
    ingest.add_argument("--archive-db", required=True, help="Archive SQLite path.")
    ingest.add_argument("--bbox", required=True)
    ingest.add_argument("--start-datetime", required=True)
    ingest.add_argument("--end-datetime", required=True)
    ingest.add_argument("--limit", type=int, default=5000)
    ingest.set_defaults(func=cmd_ingest)

    return parser


def main() -> int:
    global LOGGER  # pylint: disable=global-statement
    parser = build_parser()
    args = parser.parse_args()
    LOGGER = HumanLogger(skill_name=SKILL_NAME, root_dir=default_skill_log_root(__file__))
    log_event("cli_invocation", "CLI invoked", {"argv": sys.argv, "args": vars(args)})
    try:
        code = int(args.func(args))
        log_event("cli_exit", "CLI completed", {"exit_code": code})
        return code
    except sqlite3.Error as exc:
        print(f"PHYSICAL_ERR reason=sqlite_error detail={exc}", file=sys.stderr)
        log_event("cli_error", "SQLite error", {"detail": str(exc)})
        return 1
    except ValueError as exc:
        print(f"PHYSICAL_ERR reason=value_error detail={exc}", file=sys.stderr)
        log_event("cli_error", "Value error", {"detail": str(exc)})
        return 1
    except Exception as exc:
        print(f"PHYSICAL_ERR reason=unexpected detail={exc}", file=sys.stderr)
        log_event("cli_error", "Unexpected error", {"detail": str(exc)})
        return 1
    finally:
        if LOGGER is not None:
            LOGGER.close()


if __name__ == "__main__":
    sys.exit(main())
