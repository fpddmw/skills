#!/usr/bin/env python3
"""Observer-side data planning for moderator dispatch."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from common.human_log import HumanLogger, default_skill_log_root

SKILL_NAME = "observer-openaq-physical-ingestor"
LOGGER: HumanLogger | None = None
DEFAULT_TIMEOUT = 30.0

SUPPORTED_ENV_TYPES = ("air", "water", "soil", "radiation", "waste")
DOMAIN_TYPE_DEFAULTS: dict[str, tuple[str, ...]] = {
    "air": ("pm25", "no2", "o3"),
    "water": ("ph", "do", "cod", "nh3n", "tp"),
    "soil": ("heavy_metals", "organic_pollutants", "ph"),
    "radiation": ("gamma_dose_rate", "cs137", "i131"),
    "waste": ("landfill_volume", "incineration_emission", "wastewater_discharge"),
}
ENV_TYPE_KEYWORDS = {
    "water": ("water", "wastewater", "river", "marine", "ocean", "sewage", "水", "水质"),
    "radiation": ("radiation", "radioactive", "nuclear", "辐射", "核"),
    "soil": ("soil", "landfill", "ground contamination", "土壤"),
    "waste": ("waste", "garbage", "incinerator", "dump", "垃圾", "废弃物"),
    "air": ("air", "smog", "pm2.5", "ozone", "no2", "大气", "空气"),
}
PHYSICAL_INTENT_KEYWORDS = (
    "aqi",
    "pm2.5",
    "pm25",
    "no2",
    "o3",
    "pollutant",
    "physical signal",
    "physical metric",
    "sensor",
    "地理",
    "物理",
)


def normalize_space(value: Any) -> str:
    return " ".join(str(value or "").split())


def log_event(category: str, summary: str, details: dict[str, Any] | None = None) -> None:
    if LOGGER is not None:
        LOGGER.log(category=category, summary=summary, details=details or {})


def _parse_csv(raw: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in str(raw or "").replace(";", ",").split(","):
        token = normalize_space(item).lower()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def infer_env_domain(text: str, expected_env_type: str) -> str:
    expected = normalize_space(expected_env_type).lower()
    if expected in SUPPORTED_ENV_TYPES:
        return expected
    lower = text.lower()
    for domain in SUPPORTED_ENV_TYPES:
        if any(k in lower for k in ENV_TYPE_KEYWORDS.get(domain, ())):
            return domain
    return "air"


def choose_source(mode: str, domain: str, context: str, bbox_source: str) -> str:
    text = context.lower()
    if any(x in text for x in ("history", "historical", "retrospective", "回溯", "历史")):
        return "openaq_archive"
    if domain != "air":
        return "openmeteo_grid"
    if mode == "passive_patrol" and bbox_source == "fallback_bbox":
        return "openmeteo_grid"
    return "openaq_realtime"


def rule_plan(args: argparse.Namespace) -> dict[str, Any]:
    text = " ".join([args.objective, args.context, args.query, args.themes]).lower()
    domain = infer_env_domain(text, args.expected_env_type)
    source = choose_source(args.mode, domain, args.context, args.bbox_source)
    explicit_physical = any(k in text for k in PHYSICAL_INTENT_KEYWORDS)

    if explicit_physical:
        required = True
        reason = "explicit_physical_signal"
    elif args.mode == "passive_patrol" and domain == "air" and args.bbox_source == "fallback_bbox":
        required = False
        reason = "listener_first_global_patrol"
    elif args.mode == "passive_patrol" and domain in {"water", "soil", "radiation", "waste"} and args.bbox_source == "fallback_bbox":
        required = False
        reason = "listener_first_non_air_without_geo_anchor"
    else:
        required = args.mode == "active_reconnaissance"
        reason = "recon_requires_physical" if required else "listener_first_default"

    data_domains = [domain]
    data_types = list(DOMAIN_TYPE_DEFAULTS.get(domain, ("pm25", "no2", "o3")))
    if _parse_csv(args.raw_data_types):
        data_types = _parse_csv(args.raw_data_types)
    if _parse_csv(args.raw_data_domains):
        candidates = [x for x in _parse_csv(args.raw_data_domains) if x in SUPPORTED_ENV_TYPES]
        if candidates:
            data_domains = candidates
            merged: list[str] = []
            for d in data_domains:
                merged.extend(list(DOMAIN_TYPE_DEFAULTS.get(d, ())))
            data_types = merged or data_types

    plan = {
        "required": required,
        "requirement_reason": reason,
        "data_domains": data_domains,
        "data_types": data_types,
        "geo_strategy": args.geo_strategy,
        "source_preference": source,
        "target_country_hint": normalize_space(args.target_country).upper(),
        "reasoning_text": (
            f"rule_plan: required={required} because {reason}; "
            f"domains={data_domains}; types={data_types}; source={source}; geo_strategy={args.geo_strategy}"
        ),
        "planner_used": "rule",
    }
    return plan


def call_json_api(url: str, *, body: dict[str, Any], api_key: str, timeout: float) -> dict[str, Any]:
    payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = Request(
        url=url,
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": "observer-openaq-physical-ingestor/observer-plan/1.0",
        },
        data=payload,
    )
    log_event("api_request", "Observer planner API request", {"url": url, "timeout": timeout, "body": body})
    try:
        with urlopen(req, timeout=timeout) as resp:
            text = resp.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"http_error status={exc.code} detail={detail[:500]}") from exc
    except URLError as exc:
        raise RuntimeError(f"network_error detail={exc.reason}") from exc
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise RuntimeError("unexpected_response_shape")
    log_event("api_response", "Observer planner API response", {"url": url, "payload": parsed})
    return parsed


def llm_plan(args: argparse.Namespace) -> dict[str, Any]:
    base_url = normalize_space(os.environ.get("LLM_API_BASE_URL") or "")
    api_key = normalize_space(os.environ.get("LLM_API_KEY") or "")
    model = normalize_space(os.environ.get("LLM_MODEL") or "")
    if not (base_url and api_key and model):
        raise RuntimeError("missing_llm_env")
    body = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are observer data planner. Return JSON only with keys: "
                    "required,requirement_reason,data_domains,data_types,geo_strategy,source_preference,target_country_hint,reasoning_text. "
                    "required is boolean. "
                    "data_domains is array from air,water,soil,radiation,waste. "
                    "source_preference is openaq_realtime/openmeteo_grid/openaq_archive."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "objective": args.objective,
                        "context": args.context,
                        "query": args.query,
                        "themes": args.themes,
                        "mode": args.mode,
                        "expected_env_type": args.expected_env_type,
                        "bbox_source": args.bbox_source,
                        "geo_strategy": args.geo_strategy,
                        "target_country": args.target_country,
                    },
                    ensure_ascii=False,
                ),
            },
        ],
        "response_format": {"type": "json_object"},
    }
    log_event("llm_prompt", "Observer planner prompt", {"model": model, "base_url": base_url, "body": body})
    payload = call_json_api(
        f"{base_url.rstrip('/')}/chat/completions",
        body=body,
        api_key=api_key,
        timeout=args.timeout,
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
    parsed["planner_used"] = "llm"
    log_event("llm_response", "Observer planner response", {"model": model, "parsed_plan": parsed})
    return parsed


def sanitize_plan(plan: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    required = bool(plan.get("required"))
    reason = normalize_space(plan.get("requirement_reason") or "unspecified")
    domains = [x for x in [normalize_space(d).lower() for d in plan.get("data_domains", []) if normalize_space(d)] if x in SUPPORTED_ENV_TYPES]
    if not domains:
        domains = [infer_env_domain(f"{args.objective} {args.context} {args.query} {args.themes}", args.expected_env_type)]
    types = [normalize_space(x).lower() for x in plan.get("data_types", []) if normalize_space(x)]
    if not types:
        merged: list[str] = []
        for d in domains:
            merged.extend(DOMAIN_TYPE_DEFAULTS.get(d, ()))
        types = merged
    source = normalize_space(plan.get("source_preference")).lower()
    if source not in {"openaq_realtime", "openmeteo_grid", "openaq_archive"}:
        source = choose_source(args.mode, domains[0], args.context, args.bbox_source)
    geo_strategy = normalize_space(plan.get("geo_strategy") or args.geo_strategy) or args.geo_strategy
    country_hint = normalize_space(plan.get("target_country_hint") or args.target_country).upper()
    reasoning_text = normalize_space(plan.get("reasoning_text") or "")
    return {
        "required": required,
        "requirement_reason": reason,
        "data_domains": domains,
        "data_types": types,
        "geo_strategy": geo_strategy,
        "source_preference": source,
        "target_country_hint": country_hint,
        "reasoning_text": reasoning_text or f"sanitized_plan required={required} reason={reason}",
        "planner_used": normalize_space(plan.get("planner_used") or "rule"),
    }


def run_plan(args: argparse.Namespace) -> int:
    log_event("workflow_start", "Run observer planner", {"args": vars(args)})
    if args.planner == "rule":
        planned = rule_plan(args)
    elif args.planner == "llm":
        planned = llm_plan(args)
    else:
        try:
            planned = llm_plan(args)
        except Exception as exc:
            log_event("planning_fallback", "Observer planner fallback to rule", {"detail": str(exc)})
            planned = rule_plan(args)
    output = sanitize_plan(planned, args)
    log_event("workflow_end", "Observer planner completed", {"output": output})
    print(json.dumps(output, ensure_ascii=False, separators=(",", ":")))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Plan observer data requirements for moderator orchestration.")
    sub = parser.add_subparsers(dest="command", required=True)
    plan = sub.add_parser("plan", help="Generate observer data plan.")
    plan.add_argument("--objective", required=True)
    plan.add_argument("--context", default="")
    plan.add_argument("--query", default="")
    plan.add_argument("--themes", default="")
    plan.add_argument("--mode", default="passive_patrol", choices=("passive_patrol", "active_reconnaissance"))
    plan.add_argument("--expected-env-type", default="general")
    plan.add_argument("--bbox-source", default="fallback_bbox")
    plan.add_argument("--geo-strategy", default="fallback_bbox_global_scan")
    plan.add_argument("--target-country", default="")
    plan.add_argument("--raw-data-domains", default="")
    plan.add_argument("--raw-data-types", default="")
    plan.add_argument("--planner", default="auto", choices=("auto", "rule", "llm"))
    plan.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    plan.set_defaults(func=run_plan)
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
    except ValueError as exc:
        print(f"OBSERVER_PLAN_ERR reason=value_error detail={exc}", file=sys.stderr)
        log_event("cli_error", "Value error", {"detail": str(exc)})
        return 1
    except RuntimeError as exc:
        print(f"OBSERVER_PLAN_ERR reason=runtime_error detail={exc}", file=sys.stderr)
        log_event("cli_error", "Runtime error", {"detail": str(exc)})
        return 1
    except Exception as exc:
        print(f"OBSERVER_PLAN_ERR reason=unexpected detail={exc}", file=sys.stderr)
        log_event("cli_error", "Unexpected error", {"detail": str(exc)})
        return 1
    finally:
        if LOGGER is not None:
            LOGGER.close()


if __name__ == "__main__":
    sys.exit(main())

