from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from scripts.run_validation_tier import build_command


def test_validation_tier_print_only_writes_command_summary(tmp_path: Path) -> None:
    report_path = tmp_path / "validation_tier.json"

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/run_validation_tier.py",
            "fast",
            "--print-only",
            "--json-output",
            str(report_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "Validation tier: fast" in completed.stdout
    assert "tests/test_documentation_contract.py" in completed.stdout.replace("\\", "/")

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["tier"] == "fast"
    assert payload["status"] == "PRINT_ONLY"
    assert "tests/test_report_index.py" in " ".join(payload["command"]).replace("\\", "/")


def test_dynamic_v3_tier_discovers_related_test_files() -> None:
    command = build_command("dynamic-v3", python_executable="python", repo_root=Path.cwd())
    normalized = " ".join(command).replace("\\", "/")

    assert "tests/test_etf_dynamic_v3_parameter_research.py" in normalized
    assert "tests/test_backtest_sim_outcome.py" in normalized
    assert "tests/test_etf_dynamic_rescue.py" in normalized
    assert "tests/test_sim_defensive_validation.py" in normalized
