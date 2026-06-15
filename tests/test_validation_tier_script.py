from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from scripts.run_validation_tier import TIER_SPECS, build_command, resolve_tier


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
    assert "Resolved tier: fast-unit" in completed.stdout
    assert "Promotion blocking: True" in completed.stdout
    assert "Workers: 16" in completed.stdout
    assert "-n 16 --dist loadfile" in completed.stdout
    assert "tests/test_documentation_contract.py" in completed.stdout.replace("\\", "/")

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["tier"] == "fast"
    assert payload["resolved_tier"] == "fast-unit"
    assert payload["status"] == "PRINT_ONLY"
    assert payload["promotion_blocking"] is True
    assert payload["slow_suite_allowed"] is False
    assert payload["production_effect"] == "none"
    assert payload["strategy_logic_changed"] is False
    assert payload["broker_action_allowed"] is False
    assert payload["can_support_promotion_evidence"] is False
    assert payload["workers"] == "16"
    assert payload["dist"] == "loadfile"
    assert "tests/test_report_index.py" in " ".join(payload["command"]).replace("\\", "/")
    assert "-n 16 --dist loadfile" in " ".join(payload["command"])


def test_validation_tier_can_render_serial_command(tmp_path: Path) -> None:
    report_path = tmp_path / "validation_tier_serial.json"

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/run_validation_tier.py",
            "fast",
            "--print-only",
            "--workers",
            "1",
            "--json-output",
            str(report_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    command = " ".join(payload["command"])

    assert payload["workers"] == "1"
    assert "-n" not in command
    assert "--dist" not in command


def test_runtime_artifacts_are_written_for_print_only(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "runtime_artifact"

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/run_validation_tier.py",
            "contract-validation",
            "--print-only",
            "--write-runtime-artifact",
            "--artifact-dir",
            str(artifact_dir),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    summary_path = artifact_dir / "test_runtime_summary.json"
    reader_brief_path = artifact_dir / "test_runtime_reader_brief.md"
    assert summary_path.exists()
    assert reader_brief_path.exists()

    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    reader_brief = reader_brief_path.read_text(encoding="utf-8")
    assert payload["report_type"] == "test_runtime_summary"
    assert payload["resolved_tier"] == "contract-validation"
    assert payload["suite_family"] == "contract_validation"
    assert payload["promotion_blocking"] is True
    assert payload["status"] == "PRINT_ONLY"
    assert "test_runtime_reader_brief.md" in payload["reader_brief_path"]
    assert "Can support promotion evidence: `False`" in reader_brief
    assert "Production effect: `none`" in reader_brief


def test_formal_suite_contracts_are_registered() -> None:
    expected = {
        "fast-unit": ("fast_unit", True, False),
        "contract-validation": ("contract_validation", True, False),
        "report-validation": ("report_validation", True, False),
        "integration": ("integration", False, True),
        "slow-research-regression": ("slow_research_regression", False, True),
        "full": ("full_pytest", True, True),
    }

    for tier, (suite_family, promotion_blocking, slow_allowed) in expected.items():
        spec = TIER_SPECS[tier]
        assert spec.suite_family == suite_family
        assert spec.promotion_blocking is promotion_blocking
        assert spec.slow_suite_allowed is slow_allowed

    assert resolve_tier("fast") == "fast-unit"
    assert resolve_tier("reader-brief") == "report-validation"
    assert resolve_tier("dynamic-v3") == "slow-research-regression"
    assert resolve_tier("trading-engine") == "integration"


def test_reader_brief_alias_preserves_report_validation_coverage() -> None:
    command = build_command(
        "reader-brief",
        python_executable="python",
        repo_root=Path.cwd(),
        workers="1",
    )
    normalized = " ".join(command).replace("\\", "/")

    assert "tests/test_report_index.py" in normalized
    assert "tests/test_reader_brief.py" in normalized
    assert "tests/trading_engine" in normalized
    assert "report_index or reader_brief" in normalized


def test_slow_research_tier_discovers_related_test_files() -> None:
    command = build_command(
        "dynamic-v3",
        python_executable="python",
        repo_root=Path.cwd(),
        workers="1",
    )
    normalized = " ".join(command).replace("\\", "/")

    assert "tests/test_etf_dynamic_v3_parameter_research.py" in normalized
    assert "tests/test_backtest_sim_outcome.py" in normalized
    assert "tests/test_etf_dynamic_rescue.py" in normalized
    assert "tests/test_sim_defensive_validation.py" in normalized
