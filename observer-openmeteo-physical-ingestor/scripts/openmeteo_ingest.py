#!/usr/bin/env python3
"""Open-Meteo observer pipeline wrapper with OpenAQ-compatible status contracts."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from common.human_log import HumanLogger, default_skill_log_root

DEFAULT_BASE = "observer-openaq-physical-ingestor/scripts/aqi_ingest.py"
SKILL_NAME = "observer-openmeteo-physical-ingestor"
LOGGER: HumanLogger | None = None


def run_passthrough(argv: list[str]) -> int:
    if LOGGER is not None:
        LOGGER.log(category="command_dispatch", summary="Passthrough command", details={"argv": argv})
    proc = subprocess.run(argv, check=False)
    if LOGGER is not None:
        LOGGER.log(
            category="command_result",
            summary="Passthrough command finished",
            details={"argv": argv, "returncode": proc.returncode},
        )
    return int(proc.returncode)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest/enrich/summarize physical observations via Open-Meteo grid API."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_db = subparsers.add_parser("init-db", help="Initialize observer SQLite schema.")
    init_db.add_argument("--db", required=True, help="Observer SQLite path.")

    ingest = subparsers.add_parser("ingest", help="Ingest Open-Meteo hourly PM2.5/NO2/O3 by bbox.")
    ingest.add_argument("--db", required=True)
    ingest.add_argument("--bbox", required=True)
    ingest.add_argument("--start-datetime", required=True)
    ingest.add_argument("--end-datetime", required=True)
    ingest.add_argument("--max-locations", type=int, default=25, help="Grid points (max 25).")
    ingest.add_argument("--timeout", type=float, default=20.0)
    ingest.add_argument("--sleep-ms", type=int, default=0)
    ingest.add_argument("--openmeteo-api-base", default="")

    enrich = subparsers.add_parser("enrich", help="Reuse observer enrichment stage.")
    enrich.add_argument("--db", required=True)
    enrich.add_argument("--start-datetime", default="")
    enrich.add_argument("--end-datetime", default="")
    enrich.add_argument("--standard-profile", default="auto", choices=("auto", "who_2021", "us_epa_core"))
    enrich.add_argument("--limit", type=int, default=100000)

    summarize = subparsers.add_parser("summarize", help="Reuse observer summarize stage.")
    summarize.add_argument("--db", required=True)
    summarize.add_argument("--start-datetime", default="")
    summarize.add_argument("--end-datetime", default="")
    summarize.add_argument("--only-exceed", action="store_true")
    summarize.add_argument("--group-limit", type=int, default=200000)

    return parser


def main() -> int:
    global LOGGER  # pylint: disable=global-statement
    parser = build_parser()
    args = parser.parse_args()
    LOGGER = HumanLogger(skill_name=SKILL_NAME, root_dir=default_skill_log_root(__file__))
    LOGGER.log(category="cli_invocation", summary="CLI invoked", details={"argv": sys.argv, "args": vars(args)})
    try:
        if args.command == "init-db":
            code = run_passthrough(["python3", DEFAULT_BASE, "init-db", "--db", args.db])
            LOGGER.log(category="cli_exit", summary="CLI completed", details={"exit_code": code})
            return code

        if args.command == "ingest":
            cmd = [
                "python3",
                DEFAULT_BASE,
                "ingest",
                "--db",
                args.db,
                f"--bbox={args.bbox}",
                "--start-datetime",
                args.start_datetime,
                "--end-datetime",
                args.end_datetime,
                "--provider",
                "openmeteo",
                "--max-locations",
                str(args.max_locations),
                "--timeout",
                str(args.timeout),
                "--sleep-ms",
                str(args.sleep_ms),
            ]
            if args.openmeteo_api_base:
                cmd.extend(["--openmeteo-api-base", args.openmeteo_api_base])
            code = run_passthrough(cmd)
            LOGGER.log(category="cli_exit", summary="CLI completed", details={"exit_code": code})
            return code

        if args.command == "enrich":
            cmd = [
                "python3",
                DEFAULT_BASE,
                "enrich",
                "--db",
                args.db,
                "--standard-profile",
                args.standard_profile,
                "--limit",
                str(args.limit),
            ]
            if args.start_datetime:
                cmd.extend(["--start-datetime", args.start_datetime])
            if args.end_datetime:
                cmd.extend(["--end-datetime", args.end_datetime])
            code = run_passthrough(cmd)
            LOGGER.log(category="cli_exit", summary="CLI completed", details={"exit_code": code})
            return code

        cmd = [
            "python3",
            DEFAULT_BASE,
            "summarize",
            "--db",
            args.db,
            "--group-limit",
            str(args.group_limit),
        ]
        if args.start_datetime:
            cmd.extend(["--start-datetime", args.start_datetime])
        if args.end_datetime:
            cmd.extend(["--end-datetime", args.end_datetime])
        if args.only_exceed:
            cmd.append("--only-exceed")
        code = run_passthrough(cmd)
        LOGGER.log(category="cli_exit", summary="CLI completed", details={"exit_code": code})
        return code
    finally:
        LOGGER.close()


if __name__ == "__main__":
    sys.exit(main())
