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
from ai_trading_system.forward_observe_evidence_accumulation_plan import (
    run_forward_observe_evidence_accumulation_plan,
)
from ai_trading_system.risk_cap_cooldown_decay_design import (
    DATA_QUALITY_STATUS,
    REQUIRED_STATES,
    SAFETY_FIELDS,
    STATUS,
    TASK_ID,
    RiskCapCooldownDecayDesignError,
    build_risk_cap_cooldown_decay_rule_matrix,
    build_risk_cap_execution_state_contract,
    build_risk_cap_execution_transition_matrix,
    build_risk_cap_exposure_cap_state_matrix,
    load_trading_2294_forward_observe_plan_artifacts,
    run_risk_cap_cooldown_decay_design,
)
from ai_trading_system.scope_narrowed_forward_observe_readiness_review import (
    CONFIRMATION_CANDIDATE_ID,
    RISK_APPETITE_ARCHIVE_CANDIDATE,
    RISK_CAP_CANDIDATE_ID,
    run_scope_narrowed_forward_observe_readiness_review,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_risk_cap_cooldown_decay_design_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "risk-cap-cooldown-decay-design" in result.output


def test_risk_cap_cooldown_decay_design_policy_is_governed() -> None:
    policy = safe_load_yaml_path(
        Path("config/research/risk_cap_cooldown_decay_design_policy.yaml")
    )

    assert policy["policy_id"] == "risk_cap_cooldown_decay_design_policy"
    assert policy["version"] == "v1"
    assert policy["status"] == "pilot_research_design_only"
    assert policy["owner"] == "research_governance"
    assert policy["task_id"] == TASK_ID
    assert policy["market_regime"] == "ai_after_chatgpt"
    assert policy["source_dependency"]["required_task_id"] == (
        "TRADING-2294_EVIDENCE_ACCUMULATION_EXTENSION_PLAN"
    )
    assert policy["source_dependency"]["required_runtime_started"] is False
    assert policy["source_dependency"]["required_forward_observe_started"] is False
    assert set(policy["required_execution_states"]) == REQUIRED_STATES
    assert policy["cooldown_decay_checkpoints"] == ["5d", "10d", "20d"]
    assert len(policy["transition_rules"]) == 5
    assert policy["cap_calibration_policy"]["cap_multiplier_defined"] is False
    assert policy["manual_review_policy"]["manual_review_only"] is True

    for field, expected in SAFETY_FIELDS.items():
        assert policy["safety"][field] == expected


def test_risk_cap_cooldown_decay_builders_preserve_design_boundary(
    tmp_path: Path,
) -> None:
    source = load_trading_2294_forward_observe_plan_artifacts(
        _write_trading_2294_source(tmp_path)
    )
    policy = safe_load_yaml_path(
        Path("config/research/risk_cap_cooldown_decay_design_policy.yaml")
    )

    state_contract = build_risk_cap_execution_state_contract(
        policy=policy,
        source=source,
    )
    rule_rows = build_risk_cap_cooldown_decay_rule_matrix(
        policy=policy,
        source=source,
    )
    exposure_rows = build_risk_cap_exposure_cap_state_matrix(
        policy=policy,
        source=source,
    )
    transition_rows = build_risk_cap_execution_transition_matrix(
        policy=policy,
        source=source,
    )

    assert state_contract["state_count"] == 4
    assert {row["state_id"] for row in state_contract["states"]} == REQUIRED_STATES
    assert all(row["state_executable_now"] is False for row in state_contract["states"])
    assert all(row["portfolio_effect"] == "none" for row in state_contract["states"])
    assert len(rule_rows) == 3
    assert all(row["rule_status"] == "DESIGN_ONLY_NOT_EXECUTABLE" for row in rule_rows)
    assert all(row["cap_multiplier_defined"] is False for row in rule_rows)
    assert len(exposure_rows) == 4
    assert all(row["max_exposure_number_generated"] is False for row in exposure_rows)
    assert all(row["rebalance_instruction_generated"] is False for row in exposure_rows)
    assert len(transition_rows) == 5
    assert all(
        row["automatic_transition_allowed"] is False for row in transition_rows
    )
    assert all(row["broker_action"] == "none" for row in transition_rows)


def test_risk_cap_cooldown_decay_design_cli_writes_outputs(
    tmp_path: Path,
) -> None:
    source_dir = _write_trading_2294_source(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "risk-cap-cooldown-decay-design",
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
        "risk_cap_cooldown_decay_design_summary.json",
        "risk_cap_execution_state_contract.json",
        "risk_cap_cooldown_decay_rule_matrix.json",
        "risk_cap_cooldown_decay_rule_matrix.csv",
        "risk_cap_exposure_cap_state_matrix.json",
        "risk_cap_exposure_cap_state_matrix.csv",
        "risk_cap_manual_review_mode_contract.json",
        "risk_cap_execution_transition_matrix.json",
        "risk_cap_execution_transition_matrix.csv",
        "risk_cap_cooldown_decay_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    for filename in ("target_weights.csv", "rebalance_instruction.json", "broker_order.json"):
        assert not (output_dir / filename).exists(), filename
    assert (docs_root / "risk_cap_cooldown_decay_design.md").exists()

    summary_payload = _read_json(output_dir / "risk_cap_cooldown_decay_design_summary.json")
    summary = summary_payload["summary"]
    assert summary_payload["status"] == STATUS
    assert summary["status"] == STATUS
    assert summary["market_regime"] == "ai_after_chatgpt"
    assert summary["data_quality_status"] == DATA_QUALITY_STATUS
    assert summary["source_status"] == (
        "FORWARD_OBSERVE_EVIDENCE_ACCUMULATION_PLAN_READY_PROMOTION_BLOCKED"
    )
    assert summary["source_observe_mode"] == "observe_only"
    assert summary["source_runtime_started"] is False
    assert summary["source_forward_observe_started"] is False
    assert summary["state_count"] == 4
    assert summary["cooldown_decay_rule_count"] == 3
    assert summary["exposure_cap_state_count"] == 4
    assert summary["transition_count"] == 5
    assert summary["target_weight_generated"] is False
    assert summary["rebalance_instruction_generated"] is False
    assert summary["broker_order_generated"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"

    state_contract = _read_json(output_dir / "risk_cap_execution_state_contract.json")
    assert {row["state_id"] for row in state_contract["states"]} == REQUIRED_STATES
    assert "target_weight" in state_contract["forbidden_outputs"]

    manual_review = _read_json(output_dir / "risk_cap_manual_review_mode_contract.json")
    assert manual_review["manual_review_only"] is True
    assert manual_review["automatic_approval_allowed"] is False
    assert "broker_order" in manual_review["blocked_actions"]

    safety = _read_json(output_dir / "risk_cap_cooldown_decay_safety_boundary.json")
    assert safety["does_not_read_portfolio_weights"] is True
    assert safety["does_not_write_portfolio_weights"] is True
    assert safety["does_not_generate_target_weights"] is True
    assert safety["does_not_generate_rebalance_instruction"] is True
    assert safety["does_not_generate_broker_order"] is True
    assert safety["does_not_start_forward_observe_runtime"] is True


def test_risk_cap_cooldown_decay_design_direct_payload_has_string_paths(
    tmp_path: Path,
) -> None:
    payload = run_risk_cap_cooldown_decay_design(
        source_dir=_write_trading_2294_source(tmp_path),
        output_dir=tmp_path / "out",
        docs_root=tmp_path / "docs",
    )

    assert payload["status"] == STATUS
    assert all(isinstance(path, str) for path in payload["artifact_paths"].values())


def test_risk_cap_cooldown_decay_design_rejects_wrong_mode(
    tmp_path: Path,
) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "risk-cap-cooldown-decay-design",
            "--source-dir",
            str(_write_trading_2294_source(tmp_path)),
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "promotion",
        ],
    )

    assert result.exit_code != 0


def test_risk_cap_cooldown_decay_design_fails_closed_if_runtime_started(
    tmp_path: Path,
) -> None:
    source_dir = _write_trading_2294_source(tmp_path)
    summary_path = source_dir / "forward_observe_evidence_accumulation_plan_summary.json"
    payload = _read_json(summary_path)
    payload["runtime_started"] = True
    summary_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    with pytest.raises(RiskCapCooldownDecayDesignError, match="runtime_started"):
        run_risk_cap_cooldown_decay_design(
            source_dir=source_dir,
            output_dir=tmp_path / "out",
            docs_root=tmp_path / "docs",
        )


def test_risk_cap_cooldown_decay_registry_catalog_and_flow_are_non_promotion() -> None:
    registry = safe_load_yaml_path(Path("config/report_registry.yaml"))
    entry = next(
        report
        for report in registry["reports"]
        if report["report_id"] == "risk_cap_cooldown_decay_design"
    )

    assert entry["command"] == "aits research trends risk-cap-cooldown-decay-design"
    assert entry["artifact_role"] == "risk_cap_cooldown_decay_design_only"
    assert entry["data_quality_status"] == DATA_QUALITY_STATUS
    assert entry["validation_status"] == STATUS
    assert entry["source_status"] == (
        "FORWARD_OBSERVE_EVIDENCE_ACCUMULATION_PLAN_READY_PROMOTION_BLOCKED"
    )
    assert entry["design_only"] is True
    assert entry["execution_runtime_started"] is False
    assert entry["portfolio_weights_read"] is False
    assert entry["portfolio_weights_written"] is False
    assert entry["target_weight_generated"] is False
    assert entry["max_exposure_number_generated"] is False
    assert entry["rebalance_instruction_generated"] is False
    assert entry["broker_order_generated"] is False
    assert entry["cooldown_state_executable"] is False
    assert entry["decay_rule_executable"] is False
    assert entry["exposure_cap_rule_executable"] is False
    assert entry["state_count"] == 4
    assert entry["transition_count"] == 5
    assert entry["promotion_eligible"] is False
    assert entry["paper_shadow_allowed"] is False
    assert entry["production_allowed"] is False
    assert entry["production_effect"] == "none"
    assert entry["portfolio_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert entry["dynamic_promotion_status"] == "BLOCKED"

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    assert "risk_cap_cooldown_decay_design" in catalog
    assert STATUS in catalog
    assert DATA_QUALITY_STATUS in catalog
    assert "不是执行层 runtime" in catalog

    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    assert "TRADING-2321" in system_flow
    assert "risk-cap-cooldown-decay-design" in system_flow
    assert DATA_QUALITY_STATUS in system_flow
    assert "target_weight_generated=false" in system_flow


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
