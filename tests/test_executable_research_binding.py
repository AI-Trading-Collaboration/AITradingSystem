from __future__ import annotations

import json
from pathlib import Path

from test_next_research_cycle import RUN_DATE, _write_return_to_research_inputs
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports import executable_research_binding as binding
from ai_trading_system.reports import next_research_cycle as next_cycle
from ai_trading_system.reports import reader_brief


def test_executable_binding_contract_defines_contract_only(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_next_research_cycle_inputs(reports_dir, tmp_path)

    payload = binding.build_next_candidate_executable_binding_contract_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )

    contract = payload["binding_contract"]
    output_types = {
        row["output_type"] for row in contract["output_schema"]["outputs"]
    }
    assert payload["status"] == "EXECUTABLE_BINDING_CONTRACT_READY"
    assert contract["candidate_id"] == (
        "median_plus_regime_mismatch_filter_research_redesign_v2"
    )
    assert contract["binding_version"] == binding.BINDING_VERSION
    assert set(binding.REQUIRED_OUTPUT_TYPES).issubset(output_types)
    assert contract["research_only"] is True
    assert contract["manual_review_only"] is True
    assert contract["official_target_weights"] is False
    assert contract["strategy_behavior_implemented"] is False
    assert contract["signal_binding_implemented"] is False
    assert contract["weight_binding_implemented"] is False
    assert payload["safety_boundary"]["paper_shadow_candidate_created"] is False
    assert payload["safety_boundary"]["official_target_weights_generated"] is False
    assert payload["production_effect"] == "none"

    validation = binding.validate_executable_binding_payload(
        payload,
        expected_report_type=binding.CONTRACT_REPORT_TYPE,
    )
    assert validation["status"] == "PASS"
    assert validation["summary"]["failed_check_count"] == 0


def test_executable_binding_contract_cli_writes_contract_and_validation(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_next_research_cycle_inputs(reports_dir, tmp_path)
    runner = CliRunner()

    contract_result = runner.invoke(
        app,
        [
            "reports",
            "next-candidate-executable-binding-contract",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert contract_result.exit_code == 0, contract_result.output

    validation_result = runner.invoke(
        app,
        [
            "reports",
            "validate-next-candidate-executable-binding-contract",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validation_result.exit_code == 0, validation_result.output

    contract_path = binding.default_executable_binding_json_path(
        binding.CONTRACT_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    validation_path = binding.default_executable_binding_json_path(
        binding.CONTRACT_VALIDATION_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    assert contract_path.exists()
    assert validation_path.exists()
    assert json.loads(validation_path.read_text(encoding="utf-8"))["status"] == "PASS"


def test_reader_brief_summarizes_executable_binding_contract(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_next_research_cycle_inputs(reports_dir, tmp_path)
    payload = binding.build_next_candidate_executable_binding_contract_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )
    validation = binding.validate_executable_binding_payload(payload)
    contract_path = binding.write_executable_binding_json(
        payload,
        binding.default_executable_binding_json_path(
            binding.CONTRACT_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    validation_path = binding.write_executable_binding_json(
        validation,
        binding.default_executable_binding_json_path(
            binding.CONTRACT_VALIDATION_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    report_index = {
        "reports": [
            {
                "report_id": binding.CONTRACT_REPORT_TYPE,
                "latest_artifact_path": str(contract_path),
            },
            {
                "report_id": binding.CONTRACT_VALIDATION_REPORT_TYPE,
                "latest_artifact_path": str(validation_path),
            },
        ]
    }

    summary = reader_brief._executable_binding_contract_summary(report_index)

    assert summary["availability"] == "AVAILABLE"
    assert summary["status"] == "EXECUTABLE_BINDING_CONTRACT_READY"
    assert summary["contract_status"] == "EXECUTABLE_BINDING_CONTRACT_READY"
    assert summary["validation_status"] == "PASS"
    assert summary["research_only"] is True
    assert summary["manual_review_only"] is True
    assert summary["official_target_weights"] is False
    assert summary["signal_binding_implemented"] is False
    assert summary["weight_binding_implemented"] is False


def _write_next_research_cycle_inputs(reports_dir: Path, project_root: Path) -> None:
    _write_return_to_research_inputs(reports_dir, project_root)
    payloads = next_cycle.build_next_research_cycle_payloads(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        project_root=project_root,
        data_quality_gate={
            "status": "PASS",
            "passed": True,
            "error_count": 0,
            "warning_count": 0,
            "report_path": str(reports_dir / "data_quality_2026-06-17.md"),
        },
    )
    for report_type, payload in payloads.items():
        next_cycle.write_next_research_cycle_json(
            payload,
            next_cycle.default_next_research_cycle_json_path(
                report_type,
                reports_dir,
                RUN_DATE,
            ),
        )
        next_cycle.write_next_research_cycle_markdown(
            payload,
            next_cycle.default_next_research_cycle_markdown_path(
                report_type,
                reports_dir,
                RUN_DATE,
            ),
        )
