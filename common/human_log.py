#!/usr/bin/env python3
"""Human-readable run logger with automatic large-payload file offloading."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

MAX_INLINE_CHARS = 1800
MAX_INLINE_LINES = 24
REDACT_PATTERNS = (
    re.compile(r"(Bearer\s+)([A-Za-z0-9._\-]+)"),
    re.compile(r"(api[-_]?key\s*[:=]\s*)(\S+)", re.IGNORECASE),
    re.compile(r"(token\s*[:=]\s*)(\S+)", re.IGNORECASE),
)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def default_skill_log_root(script_file: str) -> Path:
    return Path(script_file).resolve().parents[1] / "logs"


def _mask_token(value: str) -> str:
    text = str(value)
    if len(text) <= 8:
        return "***"
    return f"{text[:4]}***{text[-3:]}"


def _redact_scalar(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    out = value
    for pattern in REDACT_PATTERNS:
        out = pattern.sub(lambda m: f"{m.group(1)}{_mask_token(m.group(2))}", out)
    return out


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key).lower()
            if any(token in key_text for token in ("api_key", "apikey", "token", "secret", "password", "authorization")):
                out[str(key)] = "***redacted***"
            else:
                out[str(key)] = redact(item)
        return out
    if isinstance(value, list):
        return [redact(item) for item in value]
    return _redact_scalar(value)


def _stringify(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, indent=2, default=str)
    return str(value)


class HumanLogger:
    def __init__(self, *, skill_name: str, root_dir: Path):
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.skill_name = skill_name
        self.root_dir = Path(root_dir).expanduser()
        self.run_dir = self.root_dir / "runs" / f"{timestamp}-{os.getpid()}"
        self.artifact_dir = self.run_dir / "artifacts"
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        self.timeline_path = self.run_dir / "timeline.log"
        self._counter = 0
        self.log(
            category="run_start",
            summary=f"{skill_name} run started",
            details={"skill": skill_name, "pid": os.getpid(), "run_dir": str(self.run_dir)},
        )

    def _write_artifact(self, label: str, content: str) -> str:
        safe = re.sub(r"[^a-zA-Z0-9._-]+", "-", label).strip("-") or "payload"
        self._counter += 1
        path = self.artifact_dir / f"{self._counter:04d}-{safe}.txt"
        path.write_text(content, encoding="utf-8")
        return str(path)

    def log(self, *, category: str, summary: str, details: dict[str, Any] | None = None) -> None:
        now = utc_now()
        block = [f"[{now}] {category}: {summary}"]
        safe_details = redact(details or {})
        for key, value in safe_details.items():
            text = _stringify(value)
            too_long = len(text) > MAX_INLINE_CHARS or text.count("\n") > MAX_INLINE_LINES
            if too_long:
                artifact = self._write_artifact(str(key), text)
                block.append(f"  - {key}: [saved] {artifact}")
            else:
                text_single = text if "\n" in text else text.strip()
                if "\n" in text_single:
                    block.append(f"  - {key}:")
                    for line in text_single.splitlines():
                        block.append(f"      {line}")
                else:
                    block.append(f"  - {key}: {text_single}")
        with self.timeline_path.open("a", encoding="utf-8") as f:
            f.write("\n".join(block) + "\n")

    def close(self) -> None:
        self.log(category="run_end", summary=f"{self.skill_name} run finished", details={})
