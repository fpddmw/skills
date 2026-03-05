#!/usr/bin/env python3
"""Stage-1 ingestion commands for GDELT environment pipeline."""

from __future__ import annotations

import argparse
import sqlite3
import sys

from gdelt_fetch import (
    DEFAULT_DB_PATH,
    DEFAULT_GDELT_API_BASE,
    DEFAULT_LLM_TIMEOUT,
    DEFAULT_TIMEOUT,
    ALLOWED_CLASSIFY_MODES,
    cmd_init_db,
    cmd_list_events,
    cmd_sync,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stage-1 ingest for GDELT environment pipeline."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_init = subparsers.add_parser("init-db", help="Initialize SQLite schema.")
    parser_init.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser_init.set_defaults(func=cmd_init_db)

    parser_ingest = subparsers.add_parser(
        "ingest",
        help="Fetch GDELT records and upsert into gdelt_environment_events.",
    )
    parser_ingest.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser_ingest.add_argument("--query", required=True, help="GDELT DOC query expression.")
    parser_ingest.add_argument("--start-datetime", required=True, help="UTC start datetime YYYYMMDDHHMMSS.")
    parser_ingest.add_argument("--end-datetime", required=True, help="UTC end datetime YYYYMMDDHHMMSS.")
    parser_ingest.add_argument(
        "--classify-mode",
        default="rule",
        choices=sorted(ALLOWED_CLASSIFY_MODES),
        help="Classification mode: none, rule, llm.",
    )
    parser_ingest.add_argument("--max-records", type=int, default=100, help="Max records [1,250].")
    parser_ingest.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="HTTP timeout seconds.")
    parser_ingest.add_argument(
        "--gdelt-api-base",
        default=DEFAULT_GDELT_API_BASE,
        help="GDELT DOC API base URL.",
    )
    parser_ingest.add_argument(
        "--llm-timeout",
        type=float,
        default=DEFAULT_LLM_TIMEOUT,
        help="LLM API timeout seconds when classify-mode=llm.",
    )
    parser_ingest.set_defaults(func=cmd_sync)

    parser_list = subparsers.add_parser("list-events", help="List recent stored events.")
    parser_list.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser_list.add_argument("--limit", type=int, default=50, help="Max rows to show.")
    parser_list.set_defaults(func=cmd_list_events)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
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
