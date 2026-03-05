#!/usr/bin/env python3
"""Stage-3 summarize commands for GDELT environment pipeline."""

from __future__ import annotations

import argparse
import sqlite3
import sys

from gdelt_fetch import (
    DEFAULT_DB_PATH,
    cmd_summarize,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stage-3 summarize for GDELT environment pipeline."
    )
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser.add_argument("--limit", type=int, default=2000, help="Max source rows to process.")
    parser.add_argument(
        "--since-datetime",
        default="",
        help="Only process source rows with seendate_utc >= this UTC timestamp (YYYYMMDDHHMMSS).",
    )
    parser.add_argument(
        "--only-relevant",
        action="store_true",
        help="Only summarize rows where env_relevance=1.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return int(cmd_summarize(args))
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
