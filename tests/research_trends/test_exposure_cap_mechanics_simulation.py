from __future__ import annotations

import json
from pathlib import Path

import pytest
from regenerated_candidate_test_helpers import (
    build_scope_narrowed_forward_observe_readiness_fixture,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.exposure_cap_mechanics_simulation import (
    DATA_QUALITY_STATUS,
    OBJECTIVE_STATUS,
    REQUIRED_SIMULATION_OBJECTIVES,
    SAFETY_FIELDS,
    STATUS,
    TASK_ID,
    ExposureCapMechanicsSimulationError,
    build_exposure_cap_simulation_input_requirement_matrix,
    build_exposure_cap_simulation_metric_contract,
    build_exposure_cap_simulation_readiness_matrix,
    load_trading_2322_signal_validity_aging_runtime_artifacts,
    run_exposure_cap_mechanics_simulation,
)
from ai_trading_system.forward_observe_evidence_accumulation_plan import (
    run_forward_observe_evidence_accumulation_plan,
)
from ai_trading_system.risk_cap_cooldown_decay_design import (
    run_risk_cap_cooldown_decay_design,
)
from ai_trading_system.scope_narrowed_forward_observe_readiness_review import (
    CONFIRMATION_CANDIDATE_ID,
    RISK_APPETITE_ARCHIVE_CANDIDATE,
    RISK_CAP_CANDIDATE_ID,
    run_scope_narrowed_forward_observe_readiness_review,
)
from ai_trading_system.signal_validity_aging_runtime_design import (
    run_signal_validity_aging_runtime_design,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_exposure_cap_mechanics_simulation_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "exposure-cap-mechanics-simulation" in result.output


def test_exposure_cap_mechanics_simulation_policy_is_governed() -> None:
    policy = safe_load_yaml_path(
        Path("config/research/exposure_cap_mechanics_simulation_policy.yaml")
    )

    assert policy["policy_id"] == "exposure_cap_mechanics_simulation_policy"
    assert policy["version"] == "v1"
    assert policy["status"] == "pilot_research_source_blocked"
    assert policy["owner"] == "research_governance"
    assert policy["task_id"] == TASK_ID
    assert policy["market_regime"] == "ai_after_chatgpt"
    assert policy["source_dependency"]["required_task_id"] == (
        "TRADING-2322_SIGNAL_VALIDITY_AGING_RUNTIME_DESIGN"
    )
    assert policy["source_dependency"]["required_design_only"] is True
    assert policy["source_dependency"]["required_runtime_started"] is False
    assert policy["source_dependency"]["required_aging_runtime_started"] is False
    assert set(policy["simulation_objectives"]) == set(REQUIRED_SIMULATION_OBJECTIVES)
    assert policy["metric_contract"]["contract_status"] == (
        "SOURCE_BLOCKED_METRIC_CONTRACT_ONLY"
    )
    assert len(policy["metric_contract"]["metrics"]) == 4
    assert "runtime_observe_records" in policy["required_runtime_inputs"]
    assert "portfolio_exposure_history" in policy["required_runtime_inputs"]

    for field, expected in SAFETY_FIELDS.items():
        assert policy["safety"][field] == expected


def test_exposure_cap_simulation_builders_mark_source_blocked(
    tmp_path: Path,
) -> None:
    source = load_trading_2322_signal_validity_aging_runtime_artifacts(
        _write_trading_2322_source(tmp_path)
    )
    policy = safe_load_yaml_path(
        Path("config/research/exposure_cap_mechanics_simulation_policy.yaml")
    )

    readiness = build_exposure_cap_simulation_readiness_matrix(
        policy=policy,
        source=source,
    )
    requirements = build_exposure_cap_simulation_input_requirement_matrix(
        policy=policy,
        source=source,
    )
    metric_contract = build_exposure_cap_simulation_metric_contract(
        policy=policy,
        source=source,
        readiness_rows=readiness,
    )

    assert len(readiness) == 4
    assert {
        row["simulation_objective"] for row in readiness
    } == set(REQUIRED_SIMULATION_OBJECTIVES)
    assert all(row["readiness_status"] == OBJECTIVE_STATUS for row in readiness)
    assert all(row["simulation_ready"] is False for row in readiness)
    assert all(row["simulation_executed"] is False for row in readiness)
    assert all(row["effect_claim_allowed"] is False for row in readiness)
    assert len(requirements) == 20
    assert all(
        row["requirement_status"] == "MISSING_OR_NOT_CONSUMED_SOURCE_BLOCKED"
        for row in requirements
    )
    assert metric_contract["status"] == "SOURCE_BLOCKED_METRIC_CONTRACT_ONLY"
    assert metric_contract["metric_count"] == 4
    assert metric_contract["metric_result_generated"] is False
    assert metric_contract["effect_claim_generated"] is False
    assert metric_contract["executable_simulation_ready"] is False


def test_exposure_cap_mechanics_simulation_cli_writes_source_blocked_outputs(
    tmp_path: Path,
) -> None:
    source_dir = _write_trading_2322_source(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "exposure-cap-mechanics-simulation",
            "--source-dir",
            str(source_dir),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "exposure_cap_mechanics_simulation_summary.json",
        "exposure_cap_simulation_metric_contract.json",
        "exposure_cap_simulation_readiness_matrix.json",
        "exposure_cap_simulation_readiness_matrix.csv",
        "exposure_cap_simulation_input_requirement_matrix.json",
        "exposure_cap_simulation_input_requirement_matrix.csv",
        "exposure_cap_simulation_blocker_report.json",
        "exposure_cap_simulation_blocker_report.csv",
        "exposure_cap_simulation_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    for filename in (
        "exposure_cap_simulation_result.csv",
        "target_weights.csv",
        "rebalance_instruction.json",
        "broker_order.json",
    ):
        assert not (output_dir / filename).exists(), filename
    assert (docs_root / "exposure_cap_mechanics_simulation.md").exists()

    summary_payload = _read_json(
        output_dir / "exposure_cap_mechanics_simulation_summary.json"
    )
    summary = summary_payload["summary"]
    assert summary_payload["status"] == STATUS
    assert summary["status"] == STATUS
    assert summary["market_regime"] == "ai_after_chatgpt"
    assert summary["data_quality_status"] == DATA_QUALITY_STATUS
    assert summary["source_status"] == (
        "SIGNAL_VALIDITY_AGING_RUNTIME_DESIGN_READY_PROMOTION_BLOCKED"
    )
    assert summary["source_design_only"] is True
    assert summary["source_runtime_started"] is False
    assert summary["source_aging_runtime_started"] is False
    assert summary["simulation_objective_count"] == 4
    assert summary["blocked_objective_count"] == 4
    assert summary["input_requirement_count"] == 20
    assert summary["blocker_count"] == 24
    assert summary["metric_count"] == 4
    assert summary["simulation_readiness_status"] == "PASS_SOURCE_BLOCKED_EXPECTED"
    assert summary["exposure_cap_mechanics_simulation_cli_implemented"] is True
    assert summary["executable_simulation_ready"] is False
    assert summary["source_blocked_no_simulation"] is True
    assert summary["simulation_executed"] is False
    assert summary["simulation_result_generated"] is False
    assert summary["runtime_records_consumed"] is False
    assert summary["portfolio_exposure_history_consumed"] is False
    assert summary["turnover_records_consumed"] is False
    assert summary["target_weight_generated"] is False
    assert summary["max_exposure_number_generated"] is False
    assert summary["rebalance_instruction_generated"] is False
    assert summary["broker_order_generated"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"

    metric_contract = _read_json(
        output_dir / "exposure_cap_simulation_metric_contract.json"
    )
    assert metric_contract["status"] == "SOURCE_BLOCKED_METRIC_CONTRACT_ONLY"
    assert metric_contract["metric_result_generated"] is False
    assert metric_contract["effect_claim_generated"] is False
    assert metric_contract["executable_simulation_ready"] is False
    assert {
        row["metric_id"] for row in metric_contract["metrics"]
    } == {
        "max_exposure_delta_after_trigger",
        "cooldown_turnover_delta",
        "restore_lag_after_clear",
        "false_risk_cap_cost",
    }
    assert "target_weight_change" in metric_contract["blocked_actions"]

    readiness = _read_json(output_dir / "exposure_cap_simulation_readiness_matrix.json")
    assert {
        row["simulation_objective"] for row in readiness["rows"]
    } == set(REQUIRED_SIMULATION_OBJECTIVES)
    assert all(row["readiness_status"] == OBJECTIVE_STATUS for row in readiness["rows"])

    safety = _read_json(output_dir / "exposure_cap_simulation_safety_boundary.json")
    assert safety["does_not_read_runtime_records"] is True
    assert safety["does_not_read_portfolio_exposure_history"] is True
    assert safety["does_not_execute_exposure_cap_simulation"] is True
    assert safety["does_not_generate_simulation_result"] is True
    assert safety["does_not_generate_max_exposure_number"] is True


def test_exposure_cap_mechanics_simulation_direct_payload_has_string_paths(
    tmp_path: Path,
) -> None:
    payload = run_exposure_cap_mechanics_simulation(
        source_dir=_write_trading_2322_source(tmp_path),
        output_dir=tmp_path / "out",
        docs_root=tmp_path / "docs",
    )

    assert payload["status"] == STATUS
    assert all(isinstance(path, str) for path in payload["artifact_paths"].values())


def test_exposure_cap_mechanics_simulation_rejects_wrong_mode(
    tmp_path: Path,
) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "exposure-cap-mechanics-simulation",
            "--source-dir",
            str(_write_trading_2322_source(tmp_path)),
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "runtime_simulation",
        ],
    )

    assert result.exit_code != 0


def test_exposure_cap_mechanics_simulation_fails_closed_if_source_runtime_started(
    tmp_path: Path,
) -> None:
    source_dir = _write_trading_2322_source(tmp_path)
    summary_path = source_dir / "signal_validity_aging_runtime_design_summary.json"
    payload = _read_json(summary_path)
    payload["aging_runtime_started"] = True
    payload["summary"]["aging_runtime_started"] = True
    summary_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    with pytest.raises(ExposureCapMechanicsSimulationError, match="aging_runtime_started"):
        run_exposure_cap_mechanics_simulation(
            source_dir=source_dir,
            output_dir=tmp_path / "out",
            docs_root=tmp_path / "docs",
        )


def test_exposure_cap_mechanics_registry_catalog_and_flow_are_non_promotion() -> None:
    registry = safe_load_yaml_path(Path("config/report_registry.yaml"))
    entry = next(
        report
        for report in registry["reports"]
        if report["report_id"] == "exposure_cap_mechanics_simulation"
    )

    assert entry["command"] == "aits research trends exposure-cap-mechanics-simulation"
    assert entry["artifact_role"] == "exposure_cap_mechanics_simulation_source_blocked"
    assert entry["data_quality_status"] == DATA_QUALITY_STATUS
    assert entry["validation_status"] == STATUS
    assert entry["source_status"] == (
        "SIGNAL_VALIDITY_AGING_RUNTIME_DESIGN_READY_PROMOTION_BLOCKED"
    )
    assert entry["simulation_objective_count"] == 4
    assert entry["blocked_objective_count"] == 4
    assert entry["input_requirement_count"] == 20
    assert entry["blocker_count"] == 24
    assert entry["metric_count"] == 4
    assert entry["source_blocked_no_simulation"] is True
    assert entry["simulation_executed"] is False
    assert entry["simulation_result_generated"] is False
    assert entry["runtime_records_consumed"] is False
    assert entry["portfolio_exposure_history_consumed"] is False
    assert entry["turnover_records_consumed"] is False
    assert entry["target_weight_generated"] is False
    assert entry["max_exposure_number_generated"] is False
    assert entry["rebalance_instruction_generated"] is False
    assert entry["broker_order_generated"] is False
    assert entry["promotion_eligible"] is False
    assert entry["paper_shadow_allowed"] is False
    assert entry["production_allowed"] is False
    assert entry["production_effect"] == "none"
    assert entry["portfolio_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert entry["dynamic_promotion_status"] == "BLOCKED"

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    assert "exposure_cap_mechanics_simulation" in catalog
    assert STATUS in catalog
    assert DATA_QUALITY_STATUS in catalog
    assert "不是 exposure-cap mechanics simulation result" in catalog

    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    assert "TRADING-2323" in system_flow
    assert "exposure-cap-mechanics-simulation" in system_flow
    assert DATA_QUALITY_STATUS in system_flow
    assert "simulation_executed=false" in system_flow


def _write_trading_2322_source(tmp_path: Path) -> Path:
    source_dir = _write_trading_2321_source(tmp_path)
    output_dir = tmp_path / "trading_2322"
    run_signal_validity_aging_runtime_design(
        source_dir=source_dir,
        output_dir=output_dir,
        docs_root=tmp_path / "docs2322",
    )
    return output_dir


def _write_trading_2321_source(tmp_path: Path) -> Path:
    source_dir = _write_trading_2294_source(tmp_path)
    output_dir = tmp_path / "trading_2321"
    run_risk_cap_cooldown_decay_design(
        source_dir=source_dir,
        output_dir=output_dir,
        docs_root=tmp_path / "docs2321",
    )
    return output_dir


def _write_trading_2294_source(tmp_path: Path) -> Path:
    readiness_dir = _write_readiness_review_fixture(tmp_path)
    output_dir = tmp_path / "trading_2294"
    run_forward_observe_evidence_accumulation_plan(
        readiness_dir=readiness_dir,
        candidate=RISK_CAP_CANDIDATE_ID,
        target_assets="QQQ,SPY,SMH",
        horizons="5d,10d,20d",
        output_dir=output_dir,
        mode="evidence_accumulation_extension_plan",
        docs_root=tmp_path / "docs2294",
    )
    return output_dir


def _write_readiness_review_fixture(tmp_path: Path) -> Path:
    fixture = build_scope_narrowed_forward_observe_readiness_fixture(tmp_path)
    output_dir = tmp_path / "trading_2293"
    run_scope_narrowed_forward_observe_readiness_review(
        scope_validation_dir=fixture["scope_validation_dir"],
        scope_generator_dir=fixture["scope_narrowed_generator_dir"],
        scope_review_dir=fixture["scope_review_dir"],
        candidate=RISK_CAP_CANDIDATE_ID,
        rejected_candidates=CONFIRMATION_CANDIDATE_ID,
        archived_candidates=RISK_APPETITE_ARCHIVE_CANDIDATE,
        target_assets="QQQ,SPY,SMH",
        horizons="5d,10d,20d",
        output_dir=output_dir,
        mode="forward_observe_readiness_review",
        docs_root=tmp_path / "docs2293",
    )
    _force_ready_with_warnings_fixture(output_dir)
    return output_dir


def _force_ready_with_warnings_fixture(output_dir: Path) -> None:
    for filename in (
        "forward_observe_readiness_review_summary.json",
        "forward_observe_gate_checklist.json",
        "forward_observe_candidate_readiness_matrix.json",
        "forward_observe_next_task_recommendation.json",
    ):
        path = output_dir / filename
        payload = _read_json(path)
        payload["readiness_gate_status"] = "FORWARD_OBSERVE_READY_WITH_WARNINGS"
        payload["readiness_review_status"] = "FORWARD_OBSERVE_READINESS_READY_WITH_WARNINGS"
        payload["forward_observe_readiness_recommendation"] = True
        payload["forward_observe_started"] = False
        payload["next_task_recommendation"] = (
            "TRADING-2294_Evidence_Accumulation_Extension_Plan"
        )
        payload["readiness_warnings"] = [
            "DATA_QUALITY_PASS_WITH_WARNINGS",
            "TRIGGER_DIRECTION_SAMPLE_SPARSE",
        ]
        if "rows" in payload and isinstance(payload["rows"], list):
            for row in payload["rows"]:
                row["readiness_gate_status"] = "FORWARD_OBSERVE_READY_WITH_WARNINGS"
                row["readiness_review_status"] = (
                    "FORWARD_OBSERVE_READINESS_READY_WITH_WARNINGS"
                )
                row["forward_observe_readiness_recommendation"] = True
        path.write_text(json.dumps(payload), encoding="utf-8")


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))
