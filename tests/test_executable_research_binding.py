from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from test_next_research_cycle import RUN_DATE, _write_return_to_research_inputs
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands import reports as reports_cli
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


def test_signal_binding_transforms_validated_inputs_to_research_signal_state(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_next_research_cycle_inputs(reports_dir, tmp_path)
    policy_paths = _write_signal_binding_inputs(tmp_path, RUN_DATE)
    _write_contract_and_validation(reports_dir)

    payload = binding.build_next_candidate_signal_binding_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        project_root=tmp_path,
        signal_input_policy_path=policy_paths["signal_input_policy_path"],
        signal_binding_policy_path=policy_paths["signal_binding_policy_path"],
        data_quality_gate=_passing_data_quality_gate(reports_dir),
    )

    assert payload["status"] in {
        binding.SIGNAL_BINDING_COMPLETE,
        binding.SIGNAL_BINDING_COMPLETE_WITH_WARNINGS,
    }
    assert payload["signal_binding"]["research_only"] is True
    assert payload["signal_binding"]["official_target_weights"] is False
    assert payload["signal_binding"]["hypothetical_research_weight_produced"] is False
    assert payload["signal_binding"]["backfill_metrics_produced"] is False
    assert len(payload["candidate_signal_series"]) == 1
    state = payload["signal_state"]
    assert state["signal_date"] == RUN_DATE.isoformat()
    assert state["risk_state"] == "risk_on"
    assert state["rotation_state"] == "increase_ai_risk"
    assert state["research_only"] is True
    assert state["official_target_weights"] is False
    assert payload["data_quality_gate"]["passed"] is True

    validation = binding.validate_executable_binding_payload(
        payload,
        expected_report_type=binding.SIGNAL_BINDING_REPORT_TYPE,
    )
    assert validation["status"] == "PASS"
    assert validation["summary"]["failed_check_count"] == 0


def test_signal_binding_blocks_missing_signal_series(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_next_research_cycle_inputs(reports_dir, tmp_path)
    policy_paths = _write_signal_binding_inputs(tmp_path, RUN_DATE)
    _write_contract_and_validation(reports_dir)
    (tmp_path / "data" / "etf_portfolio" / "signals.csv").unlink()

    payload = binding.build_next_candidate_signal_binding_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        project_root=tmp_path,
        signal_input_policy_path=policy_paths["signal_input_policy_path"],
        signal_binding_policy_path=policy_paths["signal_binding_policy_path"],
        data_quality_gate=_passing_data_quality_gate(reports_dir),
    )

    assert payload["status"] == binding.SIGNAL_BINDING_BLOCKED
    assert "etf_signal_series:missing_signal_file" in payload["blocking_reasons"]
    assert payload["candidate_signal_series"] == []
    validation = binding.validate_executable_binding_payload(
        payload,
        expected_report_type=binding.SIGNAL_BINDING_REPORT_TYPE,
    )
    assert validation["status"] == "PASS"


def test_signal_binding_validation_rejects_weight_output(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_next_research_cycle_inputs(reports_dir, tmp_path)
    policy_paths = _write_signal_binding_inputs(tmp_path, RUN_DATE)
    _write_contract_and_validation(reports_dir)
    payload = binding.build_next_candidate_signal_binding_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        project_root=tmp_path,
        signal_input_policy_path=policy_paths["signal_input_policy_path"],
        signal_binding_policy_path=policy_paths["signal_binding_policy_path"],
        data_quality_gate=_passing_data_quality_gate(reports_dir),
    )
    payload["hypothetical_research_weight"] = {"QQQ": 1.0}

    validation = binding.validate_executable_binding_payload(
        payload,
        expected_report_type=binding.SIGNAL_BINDING_REPORT_TYPE,
    )

    assert validation["status"] == "FAIL"
    assert any(
        issue["issue_id"] == "forbidden_outputs_absent"
        for issue in validation["blocking_issues"]
    )


def test_signal_binding_cli_writes_report_and_validation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_next_research_cycle_inputs(reports_dir, tmp_path)
    policy_paths = _write_signal_binding_inputs(tmp_path, RUN_DATE)
    _write_contract_and_validation(reports_dir)
    monkeypatch.setattr(
        reports_cli,
        "_run_next_research_data_quality_gate",
        lambda **_: _passing_data_quality_gate(reports_dir),
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "reports",
            "next-candidate-signal-binding",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--project-root",
            str(tmp_path),
            "--signal-input-policy-path",
            str(policy_paths["signal_input_policy_path"]),
            "--signal-binding-policy-path",
            str(policy_paths["signal_binding_policy_path"]),
        ],
    )
    assert result.exit_code == 0, result.output

    validation_result = runner.invoke(
        app,
        [
            "reports",
            "validate-next-candidate-signal-binding",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validation_result.exit_code == 0, validation_result.output
    report_path = binding.default_executable_binding_json_path(
        binding.SIGNAL_BINDING_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    validation_path = binding.default_executable_binding_json_path(
        binding.SIGNAL_BINDING_VALIDATION_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    assert report_path.exists()
    assert validation_path.exists()
    assert json.loads(validation_path.read_text(encoding="utf-8"))["status"] == "PASS"


def test_reader_brief_summarizes_signal_binding(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_next_research_cycle_inputs(reports_dir, tmp_path)
    policy_paths = _write_signal_binding_inputs(tmp_path, RUN_DATE)
    _write_contract_and_validation(reports_dir)
    payload = binding.build_next_candidate_signal_binding_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        project_root=tmp_path,
        signal_input_policy_path=policy_paths["signal_input_policy_path"],
        signal_binding_policy_path=policy_paths["signal_binding_policy_path"],
        data_quality_gate=_passing_data_quality_gate(reports_dir),
    )
    validation = binding.validate_executable_binding_payload(
        payload,
        expected_report_type=binding.SIGNAL_BINDING_REPORT_TYPE,
    )
    signal_path = binding.write_executable_binding_json(
        payload,
        binding.default_executable_binding_json_path(
            binding.SIGNAL_BINDING_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    validation_path = binding.write_executable_binding_json(
        validation,
        binding.default_executable_binding_json_path(
            binding.SIGNAL_BINDING_VALIDATION_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    report_index = {
        "reports": [
            {
                "report_id": binding.SIGNAL_BINDING_REPORT_TYPE,
                "latest_artifact_path": str(signal_path),
            },
            {
                "report_id": binding.SIGNAL_BINDING_VALIDATION_REPORT_TYPE,
                "latest_artifact_path": str(validation_path),
            },
        ]
    }

    summary = reader_brief._executable_signal_binding_summary(report_index)

    assert summary["availability"] == "AVAILABLE"
    assert summary["validation_status"] == "PASS"
    assert summary["candidate_id"] == (
        "median_plus_regime_mismatch_filter_research_redesign_v2"
    )
    assert summary["official_target_weights"] is False
    assert summary["hypothetical_research_weight_produced"] is False
    assert summary["backfill_metrics_produced"] is False
    assert summary["production_effect"] == "none"


def test_research_weight_binding_converts_signal_to_hypothetical_weights(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_next_research_cycle_inputs(reports_dir, tmp_path)
    policy_paths = _write_signal_binding_inputs(tmp_path, RUN_DATE)
    weight_policy_path = _write_weight_binding_policy(tmp_path)
    _write_contract_and_validation(reports_dir)
    _write_signal_binding_and_validation(reports_dir, tmp_path, policy_paths)

    payload = binding.build_next_candidate_research_weight_binding_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        weight_binding_policy_path=weight_policy_path,
        data_quality_gate=_passing_data_quality_gate(reports_dir),
    )

    assert payload["status"] == binding.WEIGHT_BINDING_COMPLETE_WITH_WARNINGS
    current = payload["hypothetical_research_weight"]
    previous = payload["previous_hypothetical_weight"]
    assert current["research_only"] is True
    assert current["official_target_weights"] is False
    assert current["weights"] == {
        "QQQ": 0.3,
        "SMH": 0.3,
        "SOXX": 0.25,
        "SPY": 0.15,
        "CASH": 0.0,
    }
    assert previous["weights"] == {
        "QQQ": 0.0,
        "SMH": 0.0,
        "SOXX": 0.0,
        "SPY": 1.0,
        "CASH": 0.0,
    }
    assert payload["rotation_delta"]["SPY"] == -0.85
    assert payload["turnover_proxy"] == 0.85
    assert payload["risk_state"] == "risk_on"
    assert payload["constraint_hit"] == []
    assert payload["research_weight_binding"]["broker_order_produced"] is False
    assert payload["research_weight_binding"]["backfill_metrics_produced"] is False

    validation = binding.validate_executable_binding_payload(
        payload,
        expected_report_type=binding.WEIGHT_BINDING_REPORT_TYPE,
    )
    assert validation["status"] == "PASS"
    assert validation["summary"]["failed_check_count"] == 0


def test_research_weight_binding_blocks_upstream_signal_binding(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_next_research_cycle_inputs(reports_dir, tmp_path)
    policy_paths = _write_signal_binding_inputs(tmp_path, RUN_DATE)
    weight_policy_path = _write_weight_binding_policy(tmp_path)
    _write_contract_and_validation(reports_dir)
    (tmp_path / "data" / "etf_portfolio" / "signals.csv").unlink()
    _write_signal_binding_and_validation(reports_dir, tmp_path, policy_paths)

    payload = binding.build_next_candidate_research_weight_binding_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        weight_binding_policy_path=weight_policy_path,
        data_quality_gate=_passing_data_quality_gate(reports_dir),
    )

    assert payload["status"] == binding.WEIGHT_BINDING_BLOCKED
    assert "signal_binding_blocked" in payload["blocking_reasons"]
    assert payload["blocking_reason"]
    validation = binding.validate_executable_binding_payload(
        payload,
        expected_report_type=binding.WEIGHT_BINDING_REPORT_TYPE,
    )
    assert validation["status"] == "PASS"


def test_research_weight_binding_validation_rejects_missing_research_only(
    tmp_path: Path,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_next_research_cycle_inputs(reports_dir, tmp_path)
    policy_paths = _write_signal_binding_inputs(tmp_path, RUN_DATE)
    weight_policy_path = _write_weight_binding_policy(tmp_path)
    _write_contract_and_validation(reports_dir)
    _write_signal_binding_and_validation(reports_dir, tmp_path, policy_paths)
    payload = binding.build_next_candidate_research_weight_binding_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        weight_binding_policy_path=weight_policy_path,
        data_quality_gate=_passing_data_quality_gate(reports_dir),
    )
    payload["hypothetical_research_weight"]["research_only"] = False

    validation = binding.validate_executable_binding_payload(
        payload,
        expected_report_type=binding.WEIGHT_BINDING_REPORT_TYPE,
    )

    assert validation["status"] == "FAIL"
    assert any(
        issue["issue_id"] == "weight_outputs_research_only"
        for issue in validation["blocking_issues"]
    )


def test_research_weight_binding_cli_writes_report_and_validation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_next_research_cycle_inputs(reports_dir, tmp_path)
    policy_paths = _write_signal_binding_inputs(tmp_path, RUN_DATE)
    weight_policy_path = _write_weight_binding_policy(tmp_path)
    _write_contract_and_validation(reports_dir)
    _write_signal_binding_and_validation(reports_dir, tmp_path, policy_paths)
    monkeypatch.setattr(
        reports_cli,
        "_run_next_research_data_quality_gate",
        lambda **_: _passing_data_quality_gate(reports_dir),
    )
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "reports",
            "next-candidate-research-weight-binding",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--weight-binding-policy-path",
            str(weight_policy_path),
        ],
    )
    assert result.exit_code == 0, result.output

    validation_result = runner.invoke(
        app,
        [
            "reports",
            "validate-next-candidate-research-weight-binding",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validation_result.exit_code == 0, validation_result.output
    report_path = binding.default_executable_binding_json_path(
        binding.WEIGHT_BINDING_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    validation_path = binding.default_executable_binding_json_path(
        binding.WEIGHT_BINDING_VALIDATION_REPORT_TYPE,
        reports_dir,
        RUN_DATE,
    )
    assert report_path.exists()
    assert validation_path.exists()
    assert json.loads(validation_path.read_text(encoding="utf-8"))["status"] == "PASS"


def test_reader_brief_summarizes_research_weight_binding(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    _write_next_research_cycle_inputs(reports_dir, tmp_path)
    policy_paths = _write_signal_binding_inputs(tmp_path, RUN_DATE)
    weight_policy_path = _write_weight_binding_policy(tmp_path)
    _write_contract_and_validation(reports_dir)
    _write_signal_binding_and_validation(reports_dir, tmp_path, policy_paths)
    payload = binding.build_next_candidate_research_weight_binding_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        weight_binding_policy_path=weight_policy_path,
        data_quality_gate=_passing_data_quality_gate(reports_dir),
    )
    validation = binding.validate_executable_binding_payload(
        payload,
        expected_report_type=binding.WEIGHT_BINDING_REPORT_TYPE,
    )
    weight_path = binding.write_executable_binding_json(
        payload,
        binding.default_executable_binding_json_path(
            binding.WEIGHT_BINDING_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    validation_path = binding.write_executable_binding_json(
        validation,
        binding.default_executable_binding_json_path(
            binding.WEIGHT_BINDING_VALIDATION_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    report_index = {
        "reports": [
            {
                "report_id": binding.WEIGHT_BINDING_REPORT_TYPE,
                "latest_artifact_path": str(weight_path),
            },
            {
                "report_id": binding.WEIGHT_BINDING_VALIDATION_REPORT_TYPE,
                "latest_artifact_path": str(validation_path),
            },
        ]
    }

    summary = reader_brief._executable_research_weight_binding_summary(report_index)

    assert summary["availability"] == "AVAILABLE"
    assert summary["validation_status"] == "PASS"
    assert summary["candidate_id"] == (
        "median_plus_regime_mismatch_filter_research_redesign_v2"
    )
    assert summary["official_target_weights"] is False
    assert summary["broker_order_produced"] is False
    assert summary["backfill_metrics_produced"] is False
    assert summary["production_effect"] == "none"


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


def _write_contract_and_validation(reports_dir: Path) -> None:
    contract = binding.build_next_candidate_executable_binding_contract_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
    )
    validation = binding.validate_executable_binding_payload(contract)
    binding.write_executable_binding_json(
        contract,
        binding.default_executable_binding_json_path(
            binding.CONTRACT_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    binding.write_executable_binding_json(
        validation,
        binding.default_executable_binding_json_path(
            binding.CONTRACT_VALIDATION_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )


def _write_signal_binding_and_validation(
    reports_dir: Path,
    project_root: Path,
    policy_paths: dict[str, Path],
) -> None:
    signal_payload = binding.build_next_candidate_signal_binding_payload(
        as_of=RUN_DATE,
        reports_dir=reports_dir,
        project_root=project_root,
        signal_input_policy_path=policy_paths["signal_input_policy_path"],
        signal_binding_policy_path=policy_paths["signal_binding_policy_path"],
        data_quality_gate=_passing_data_quality_gate(reports_dir),
    )
    signal_validation = binding.validate_executable_binding_payload(
        signal_payload,
        expected_report_type=binding.SIGNAL_BINDING_REPORT_TYPE,
    )
    binding.write_executable_binding_json(
        signal_payload,
        binding.default_executable_binding_json_path(
            binding.SIGNAL_BINDING_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )
    binding.write_executable_binding_json(
        signal_validation,
        binding.default_executable_binding_json_path(
            binding.SIGNAL_BINDING_VALIDATION_REPORT_TYPE,
            reports_dir,
            RUN_DATE,
        ),
    )


def _passing_data_quality_gate(reports_dir: Path) -> dict[str, object]:
    return {
        "status": "PASS",
        "passed": True,
        "error_count": 0,
        "warning_count": 0,
        "report_path": str(reports_dir / "data_quality_2026-06-17.md"),
    }


def _write_weight_binding_policy(tmp_path: Path) -> Path:
    policy_path = tmp_path / "weight_binding_policy.yaml"
    policy_path.write_text(
        """
schema_version: 1
policy_id: test_research_weight_binding_policy
version: 2026-06-17
status: pilot_baseline
owner: tests
research_weight_universe: [QQQ, SMH, SOXX, SPY, CASH]
initial_previous_hypothetical_weight:
  QQQ: 0.0
  SMH: 0.0
  SOXX: 0.0
  SPY: 1.0
  CASH: 0.0
rotation_profiles:
  increase_ai_risk:
    QQQ: 0.30
    SMH: 0.30
    SOXX: 0.25
    SPY: 0.15
    CASH: 0.0
  hold_current_research_weight: previous
  reduce_ai_risk:
    QQQ: 0.10
    SMH: 0.10
    SOXX: 0.10
    SPY: 0.50
    CASH: 0.20
  move_to_cash_research_proxy:
    QQQ: 0.0
    SMH: 0.0
    SOXX: 0.0
    SPY: 0.0
    CASH: 1.0
  blocked:
    QQQ: 0.0
    SMH: 0.0
    SOXX: 0.0
    SPY: 0.0
    CASH: 0.0
constraints:
  min_weight: 0.0
  max_single_weight: 1.0
  total_weight: 1.0
  total_weight_tolerance: 0.000001
""".lstrip(),
        encoding="utf-8",
    )
    return policy_path


def _write_signal_binding_inputs(tmp_path: Path, as_of: date) -> dict[str, Path]:
    etf_dir = tmp_path / "data" / "etf_portfolio"
    processed_dir = tmp_path / "data" / "processed"
    reports_dir = tmp_path / "outputs" / "reports"
    etf_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    signal_date = as_of.isoformat()
    (etf_dir / "signals.csv").write_text(
        "\n".join(
            [
                "date,symbol,trend_score,momentum_score,relative_strength_score,"
                "risk_score,composite_score,direction,confidence,reason_codes,"
                "model_version,feature_version,created_at",
                f"{signal_date},QQQ,90,91,92,88,94,bullish,high,test,0.1.0,etf_features_v0_1,{signal_date}T00:00:00Z",
                f"{signal_date},SMH,91,92,93,89,96,bullish,high,test,0.1.0,etf_features_v0_1,{signal_date}T00:00:00Z",
                f"{signal_date},SOXX,90,92,94,90,95,bullish,high,test,0.1.0,etf_features_v0_1,{signal_date}T00:00:00Z",
                f"{signal_date},SPY,80,81,82,78,84,bullish,high,test,0.1.0,etf_features_v0_1,{signal_date}T00:00:00Z",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    feature_header = (
        "date,symbol,close,adj_close,volume,ret_20d,ret_60d,ret_120d,ma_20,ma_50,"
        "ma_100,ma_200,realized_vol_20d,drawdown_63d,rs_vs_spy_60d,rs_vs_qqq_60d,"
        "rs_vs_smh_60d,feature_version,created_at"
    )
    feature_rows = [
        f"{signal_date},CASH,1,1,0,0,0,0,1,1,1,1,0,0,0,0,0,etf_features_v0_1,{signal_date}T00:00:00Z",
        f"{signal_date},QQQ,100,100,1000,0.02,0.03,0.04,98,96,94,90,0.12,-0.03,0.02,0,0.01,etf_features_v0_1,{signal_date}T00:00:00Z",
        f"{signal_date},SMH,110,110,900,0.03,0.04,0.05,107,104,100,95,0.14,-0.04,0.03,0.01,0,etf_features_v0_1,{signal_date}T00:00:00Z",
        f"{signal_date},SOXX,105,105,850,0.025,0.035,0.045,102,100,97,93,0.13,-0.035,0.025,0.005,-0.005,etf_features_v0_1,{signal_date}T00:00:00Z",
        f"{signal_date},SPY,90,90,1500,0.01,0.015,0.02,89,88,86,84,0.09,-0.02,0,0,0,etf_features_v0_1,{signal_date}T00:00:00Z",
    ]
    (etf_dir / "features.csv").write_text(
        "\n".join([feature_header, *feature_rows]) + "\n",
        encoding="utf-8",
    )
    daily_header = "as_of,source_date,category,subject,feature,value,unit,lookback,source,notes"
    daily_rows = [
        f"{signal_date},{signal_date},{category},test,{category}_feature,1,index,1d,test,ok"
        for category in [
            "macro_liquidity",
            "price",
            "relative_strength",
            "risk_sentiment",
            "trend",
        ]
    ]
    (processed_dir / "features_daily.csv").write_text(
        "\n".join([daily_header, *daily_rows]) + "\n",
        encoding="utf-8",
    )
    snapshot = {
        "report_type": "signal_snapshot_report",
        "metadata": {
            "as_of": signal_date,
            "status": "OK",
            "required_signals": [
                "macro_liquidity",
                "trend_momentum",
                "sector_strength",
                "earnings_quality",
                "valuation_risk",
                "event_risk",
            ],
        },
        "signals": {
            "macro_liquidity": {},
            "trend_momentum": {},
            "sector_strength": {},
            "earnings_quality": {},
            "valuation_risk": {},
            "event_risk": {},
        },
    }
    (reports_dir / f"signal_snapshot_{signal_date}.json").write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    signal_policy = tmp_path / "signal_input_policy.yaml"
    signal_policy.write_text(
        f"""
schema_version: 1
policy_id: test_signal_input_policy
version: 2026-06-17
severity_order: [OK, WARNING, BLOCKING]
default_next_actions:
  OK: continue
  WARNING: review
  BLOCKING: stop
required_inputs:
  etf_signal_series:
    label: ETF signal series
    required: true
    input_type: csv_timeseries
    path: {(etf_dir / "signals.csv").as_posix()}
    date_column: date
    coverage_column: symbol
    stale_warning_days: 3
    stale_blocking_days: 7
    required_columns:
      - date
      - symbol
      - trend_score
      - momentum_score
      - relative_strength_score
      - risk_score
      - composite_score
      - direction
      - confidence
      - reason_codes
      - model_version
      - feature_version
      - created_at
    required_coverage_values: [QQQ, SMH, SOXX, SPY]
    schema_version_column: model_version
    allowed_schema_versions: [0.1.0]
    feature_version_column: feature_version
    allowed_feature_versions: [etf_features_v0_1]
  etf_feature_matrix:
    label: ETF feature matrix
    required: true
    input_type: csv_timeseries
    path: {(etf_dir / "features.csv").as_posix()}
    date_column: date
    coverage_column: symbol
    stale_warning_days: 3
    stale_blocking_days: 7
    required_columns:
      - date
      - symbol
      - close
      - adj_close
      - volume
      - ret_20d
      - ret_60d
      - ret_120d
      - ma_20
      - ma_50
      - ma_100
      - ma_200
      - realized_vol_20d
      - drawdown_63d
      - rs_vs_spy_60d
      - rs_vs_qqq_60d
      - rs_vs_smh_60d
      - feature_version
      - created_at
    required_coverage_values: [CASH, QQQ, SMH, SOXX, SPY]
    feature_version_column: feature_version
    allowed_feature_versions: [etf_features_v0_1]
  daily_feature_records:
    label: Daily feature records
    required: true
    input_type: csv_timeseries
    path: {(processed_dir / "features_daily.csv").as_posix()}
    date_column: as_of
    coverage_column: category
    stale_warning_days: 2
    stale_blocking_days: 5
    required_columns:
      - as_of
      - source_date
      - category
      - subject
      - feature
      - value
      - unit
      - lookback
      - source
      - notes
    required_coverage_values: [macro_liquidity, price, relative_strength, risk_sentiment, trend]
  latest_signal_snapshot:
    label: Latest signal snapshot report
    required: true
    input_type: json_report
    path: {(reports_dir / f"signal_snapshot_{signal_date}.json").as_posix()}
    date_json_path: metadata.as_of
    report_type: signal_snapshot_report
    stale_warning_days: 3
    stale_blocking_days: 7
    status_json_path: metadata.status
    warning_statuses: [LIMITED]
    blocking_statuses: [FAIL, FAILED, BLOCKED]
    required_json_paths: [metadata.required_signals, signals]
    required_signal_keys:
      - macro_liquidity
      - trend_momentum
      - sector_strength
      - earnings_quality
      - valuation_risk
      - event_risk
""".lstrip(),
        encoding="utf-8",
    )
    signal_binding_policy = tmp_path / "signal_binding_policy.yaml"
    signal_binding_policy.write_text(
        """
schema_version: 1
policy_id: test_signal_binding_policy
version: 2026-06-17
status: pilot_baseline
owner: tests
score_normalization:
  composite_score_center: 50.0
  composite_score_span: 50.0
  normalized_min: -1.0
  normalized_max: 1.0
risk_state_rules:
  risk_on_direction_values: [bullish]
  risk_off_direction_values: [bearish]
rotation_state_rules:
  neutral_band: 0.0
""".lstrip(),
        encoding="utf-8",
    )
    return {
        "signal_input_policy_path": signal_policy,
        "signal_binding_policy_path": signal_binding_policy,
    }
