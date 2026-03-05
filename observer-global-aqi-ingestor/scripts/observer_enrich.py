#!/usr/bin/env python3
"""Stage-2 enrichment commands for observer global AQI pipeline."""

from __future__ import annotations

import argparse
import sqlite3
import sys

from aqi_ingest import (
    DEFAULT_DB_PATH,
    cmd_enrich,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stage-2 enrich for observer global AQI pipeline."
    )
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser.add_argument("--start-datetime", default="", help="Optional UTC ISO-8601 lower bound.")
    parser.add_argument("--end-datetime", default="", help="Optional UTC ISO-8601 upper bound.")
    parser.add_argument(
        "--standard-profile",
        default="auto",
        choices=["auto", "who_2021", "us_epa_core"],
        help="auto: US->EPA else WHO",
    )
    parser.add_argument("--limit", type=int, default=100000, help="Max rows to process.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return int(cmd_enrich(args))
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
