from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio import dynamic_v3_stress_scenarios as scenarios


def test_stress_scenario_library_builds_and_validates(tmp_path: Path) -> None:
    result = scenarios.build_stress_scenario_library(
        output_dir=tmp_path / "stress_scenario_library",
        generated_at=datetime(2026, 6, 15, tzinfo=UTC),
    )
    validation = scenarios.validate_stress_scenario_library_artifact(
        library_run_id=result["library_run_id"],
        output_dir=tmp_path / "stress_scenario_library",
    )
    payload = scenarios.stress_scenario_library_report_payload(
        library_run_id=result["library_run_id"],
        output_dir=tmp_path / "stress_scenario_library",
    )
    library = result["stress_scenario_library"]
    scenario_ids = {row["scenario_id"] for row in library["scenarios"]}

    assert validation["status"] == "PASS"
    assert payload["stress_scenario_library"]["library_run_id"] == result["library_run_id"]
    assert library["scenario_count"] == len(scenarios.REQUIRED_STRESS_SCENARIO_IDS)
    assert set(scenarios.REQUIRED_STRESS_SCENARIO_IDS).issubset(scenario_ids)
    assert library["required_scenarios_present"] is True
    assert library["candidate_validation_use"] == (
        "standardized_dynamic_v3_candidate_stress_validation"
    )
    assert library["next_validation_action"] == (
        "use_library_ids_in_next_stress_backfill_or_case_review"
    )
    assert library["data_downloaded_by_library"] is False
    assert library["pipelines_executed_by_library"] is False
    assert library["not_probability_forecast"] is True
    assert "stress_scenario_library_id" in result["stress_scenario_reader_brief"]


def test_stress_scenario_library_validation_fails_missing_required(
    tmp_path: Path,
) -> None:
    config = yaml.safe_load(
        scenarios.DEFAULT_STRESS_SCENARIO_LIBRARY_CONFIG_PATH.read_text(encoding="utf-8")
    )
    config["scenarios"] = [
        row for row in config["scenarios"] if row["scenario_id"] != "liquidity_squeeze"
    ]
    config_path = tmp_path / "stress_scenario_library_v1.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    result = scenarios.build_stress_scenario_library(
        config_path=config_path,
        output_dir=tmp_path / "stress_scenario_library",
        generated_at=datetime(2026, 6, 15, tzinfo=UTC),
    )

    validation = result["stress_scenario_validation"]
    assert validation["status"] == "FAIL"
    assert result["stress_scenario_library"]["required_scenarios_present"] is False
    assert result["stress_scenario_library"]["missing_required_scenarios"] == [
        "liquidity_squeeze"
    ]


def test_stress_scenario_library_cli_report_and_validate(tmp_path: Path) -> None:
    output_dir = tmp_path / "stress_scenario_library"
    result = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "stress-scenario-library",
            "report",
            "--output-dir",
            str(output_dir),
        ],
    )
    assert result.exit_code == 0
    assert "scenario_count=9" in result.output
    assert "required_scenarios_present=True" in result.output
    library_run_id = next(
        line.split("=", 1)[1]
        for line in result.output.splitlines()
        if line.startswith("library_run_id=")
    )

    validation = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "validate-stress-scenario-library",
            "--library-run-id",
            library_run_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert validation.exit_code == 0
    assert "status=PASS" in validation.output
