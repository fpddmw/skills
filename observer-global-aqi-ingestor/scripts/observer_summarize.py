#!/usr/bin/env python3
"""Stage-3 summarize commands for observer global AQI pipeline."""

from __future__ import annotations

import argparse
import sqlite3
import sys

from aqi_ingest import (
    DEFAULT_DB_PATH,
    cmd_list_metrics,
    cmd_summarize,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stage-3 summarize for observer global AQI pipeline."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_sum = subparsers.add_parser(
        "summarize",
        help="Idempotent upsert aggregated rows into physical_metrics.",
    )
    parser_sum.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser_sum.add_argument("--start-datetime", default="", help="Optional UTC ISO-8601 lower bound.")
    parser_sum.add_argument("--end-datetime", default="", help="Optional UTC ISO-8601 upper bound.")
    parser_sum.add_argument("--only-exceed", action="store_true", help="Summarize exceeded rows only.")
    parser_sum.add_argument("--group-limit", type=int, default=200000, help="Max grouped rows.")
    parser_sum.set_defaults(func=cmd_summarize)

    parser_list = subparsers.add_parser("list-metrics", help="List aggregated physical metrics.")
    parser_list.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser_list.add_argument("--limit", type=int, default=100, help="Rows to display.")
    parser_list.set_defaults(func=cmd_list_metrics)

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
