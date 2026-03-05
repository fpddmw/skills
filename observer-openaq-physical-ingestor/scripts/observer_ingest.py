#!/usr/bin/env python3
"""Stage-1 ingestion commands for observer global AQI pipeline."""

from __future__ import annotations

import argparse
import sqlite3
import sys

from aqi_ingest import (
    DEFAULT_DB_PATH,
    DEFAULT_OPENAQ_BASE_URL,
    DEFAULT_OPENAQ_LIMIT,
    DEFAULT_OPENAQ_MAX_PAGES,
    DEFAULT_OPENAQ_SLEEP_MS,
    DEFAULT_OPENAQ_TIMEOUT,
    DEFAULT_OPENAQ_USER_AGENT,
    cmd_ingest,
    cmd_init_db,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stage-1 ingest for observer global AQI pipeline."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_init = subparsers.add_parser("init-db", help="Initialize SQLite schema.")
    parser_init.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser_init.set_defaults(func=cmd_init_db)

    parser_ingest = subparsers.add_parser(
        "ingest",
        help="Ingest OpenAQ rows by bbox and datetime range.",
    )
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

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
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
    except Exception as exc:
        print(f"PHYSICAL_ERR reason=unexpected detail={exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
