from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SKILL_ROOT = Path(__file__).resolve().parents[2]
SKILLS_REPOSITORY = SKILL_ROOT.parent
EXPECTED_CLI_VERSION = "0.0.48"
EXPECTED_ENTRYPOINT = (
    f'npx --yes --package "@tiangong-ai/cli@{EXPECTED_CLI_VERSION}" -- tiangong-ai'
)
PIN_PATTERN = re.compile(r"@tiangong-ai/cli@([0-9]+\.[0-9]+\.[0-9]+)")
RUN_INSTALL_SMOKE = os.environ.get("TIANGONG_KB_INGEST_RUN_INSTALL_SMOKE") == "1"


class CliPinContractTests(unittest.TestCase):
    def test_every_runtime_literal_uses_the_reviewed_exact_cli(self) -> None:
        for relative in ("SKILL.md", "references/env.md"):
            text = (SKILL_ROOT / relative).read_text(encoding="utf-8")
            versions = PIN_PATTERN.findall(text)
            self.assertGreater(len(versions), 0, relative)
            self.assertEqual(set(versions), {EXPECTED_CLI_VERSION}, relative)
            self.assertIn(EXPECTED_ENTRYPOINT, text, relative)

    def test_removed_cli_pin_is_absent_from_the_installed_surface(self) -> None:
        for relative in ("SKILL.md", "references/env.md"):
            text = (SKILL_ROOT / relative).read_text(encoding="utf-8")
            self.assertNotIn("@tiangong-ai/cli@0.0.19", text, relative)


@unittest.skipUnless(
    RUN_INSTALL_SMOKE, "set TIANGONG_KB_INGEST_RUN_INSTALL_SMOKE=1"
)
class InstalledSkillSmokeTests(unittest.TestCase):
    def test_npx_copy_and_symlink_installs_run_exact_cli_smoke(self) -> None:
        if not shutil.which("npx"):
            self.skipTest("npx is not installed")

        with tempfile.TemporaryDirectory() as cache:
            for install_mode in ("copy", "symlink"):
                with (
                    self.subTest(install_mode=install_mode),
                    tempfile.TemporaryDirectory() as consumer,
                ):
                    consumer_root = Path(consumer)
                    home = consumer_root / "home"
                    home.mkdir(mode=0o700)
                    subprocess.run(["git", "init", "-q"], cwd=consumer, check=True)
                    environment = {
                        **os.environ,
                        "CI": "1",
                        "HOME": str(home),
                        "NO_COLOR": "1",
                        "npm_config_cache": cache,
                    }
                    for name in (
                        "TIANGONG_AI_API_KEY",
                        "TIANGONG_KB_API_KEY",
                        "TIANGONG_KB_API_BASE_URL",
                        "TIANGONG_KB_DEFAULT_COLLECTION_NAME",
                    ):
                        environment.pop(name, None)

                    install = [
                        "npx",
                        "--yes",
                        "skills@1.5.22",
                        "add",
                        str(SKILLS_REPOSITORY),
                        "--skill",
                        "tiangong-kb-ingest",
                        "--agent",
                        "codex",
                        "--yes",
                    ]
                    if install_mode == "copy":
                        install.append("--copy")
                    self.run_command(install, consumer_root, environment)

                    installed = (
                        consumer_root / ".agents" / "skills" / "tiangong-kb-ingest"
                    )
                    for relative in (
                        "SKILL.md",
                        "agents/openai.yaml",
                        "references/env.md",
                    ):
                        self.assertTrue((installed / relative).is_file(), relative)
                    installed_skill = (installed / "SKILL.md").read_text(
                        encoding="utf-8"
                    )
                    installed_environment = (installed / "references/env.md").read_text(
                        encoding="utf-8"
                    )
                    for installed_text in (installed_skill, installed_environment):
                        self.assertIn(EXPECTED_ENTRYPOINT, installed_text)
                        self.assertNotIn("@tiangong-ai/cli@0.0.19", installed_text)

                    cli = [
                        "npx",
                        "--yes",
                        "--package",
                        f"@tiangong-ai/cli@{EXPECTED_CLI_VERSION}",
                        "--",
                        "tiangong-ai",
                    ]
                    version = self.run_command(
                        [*cli, "--version"], consumer_root, environment
                    )
                    self.assertEqual(version.stdout.strip(), EXPECTED_CLI_VERSION)

                    help_result = self.run_command(
                        [*cli, "kb", "--help"], consumer_root, environment
                    )
                    for command in (
                        "kb ingest bulk",
                        "kb ingest bulk scan",
                        "kb ingest metadata dry-run",
                        "kb ingest status",
                        "kb collections list",
                        "kb collections schema",
                    ):
                        self.assertIn(command, help_result.stdout)

                    documents = consumer_root / "documents"
                    documents.mkdir()
                    (documents / "sample.txt").write_text(
                        "offline install smoke\n", encoding="utf-8"
                    )
                    scan = self.run_command(
                        [
                            *cli,
                            "kb",
                            "ingest",
                            "bulk",
                            "scan",
                            str(documents),
                            "--json",
                        ],
                        consumer_root,
                        environment,
                    )
                    self.assertIsInstance(json.loads(scan.stdout), dict)

    def run_command(
        self, command: list[str], cwd: Path, environment: dict[str, str]
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            command,
            cwd=cwd,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
            timeout=240,
        )


if __name__ == "__main__":
    unittest.main()
