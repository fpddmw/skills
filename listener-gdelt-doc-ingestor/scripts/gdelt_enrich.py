#!/usr/bin/env python3
"""Stage-2 enrichment commands for GDELT environment pipeline."""

from __future__ import annotations

import argparse
import sqlite3
import sys

from gdelt_fetch import (
    ALLOWED_CLASSIFY_MODES,
    DEFAULT_DB_PATH,
    DEFAULT_LLM_TIMEOUT,
    cmd_enrich,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stage-2 enrich for GDELT environment pipeline."
    )
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    parser.add_argument(
        "--classify-mode",
        default="rule",
        choices=sorted(ALLOWED_CLASSIFY_MODES),
        help="Classification mode: none, rule, llm.",
    )
    parser.add_argument("--limit", type=int, default=500, help="Max rows to enrich.")
    parser.add_argument(
        "--llm-timeout",
        type=float,
        default=DEFAULT_LLM_TIMEOUT,
        help="LLM API timeout seconds when classify-mode=llm.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return int(cmd_enrich(args))
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
