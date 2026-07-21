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
    run_risk_cap_cooldown_decay_design,
)
from ai_trading_system.scope_narrowed_forward_observe_readiness_review import (
    CONFIRMATION_CANDIDATE_ID,
    RISK_APPETITE_ARCHIVE_CANDIDATE,
    RISK_CAP_CANDIDATE_ID,
    run_scope_narrowed_forward_observe_readiness_review,
)
from ai_trading_system.signal_validity_aging_runtime_design import (
    DATA_QUALITY_STATUS,
    REQUIRED_LIFECYCLE_FIELDS,
    SAFETY_FIELDS,
    STATUS,
    TASK_ID,
    SignalValidityAgingRuntimeDesignError,
    build_signal_validity_aging_rule_matrix,
    build_signal_validity_lifecycle_contract,
    build_signal_validity_release_restore_rule_matrix,
    build_signal_validity_runtime_record_schema,
    build_signal_validity_trigger_aging_state_matrix,
    load_trading_2321_risk_cap_cooldown_decay_design_artifacts,
    run_signal_validity_aging_runtime_design,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_signal_validity_aging_runtime_design_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "signal-validity-aging-runtime-design" in result.output


def test_signal_validity_aging_runtime_design_policy_is_governed() -> None:
    policy = safe_load_yaml_path(
        Path("config/research/signal_validity_aging_runtime_design_policy.yaml")
    )

    assert policy["policy_id"] == "signal_validity_aging_runtime_design_policy"
    assert policy["version"] == "v1"
    assert policy["status"] == "pilot_research_design_only"
    assert policy["owner"] == "research_governance"
    assert policy["task_id"] == TASK_ID
    assert policy["market_regime"] == "unified_primary_2021"
    assert policy["source_dependency"]["required_task_id"] == (
        "TRADING-2321_RISK_CAP_COOLDOWN_DECAY_DESIGN"
    )
    assert policy["source_dependency"]["required_runtime_started"] is False
    assert policy["source_dependency"]["required_execution_runtime_started"] is False
    assert tuple(policy["required_lifecycle_fields"]) == REQUIRED_LIFECYCLE_FIELDS
    assert policy["aging_checkpoints"] == ["5d", "10d", "20d"]
    assert len(policy["aging_rules"]) == 5
    assert len(policy["trigger_aging_states"]) == 4
    assert len(policy["release_restore_rules"]) == 4
    assert policy["calibration_policy"]["validity_duration_defined"] is False
    assert policy["calibration_policy"]["decay_multiplier_defined"] is False
    assert policy["calibration_policy"]["staleness_threshold_defined"] is False
    assert policy["calibration_policy"]["release_threshold_defined"] is False

    for field, expected in SAFETY_FIELDS.items():
        assert policy["safety"][field] == expected


def test_signal_validity_aging_builders_preserve_design_boundary(
    tmp_path: Path,
) -> None:
    source = load_trading_2321_risk_cap_cooldown_decay_design_artifacts(
        _write_trading_2321_source(tmp_path)
    )
    policy = safe_load_yaml_path(
        Path("config/research/signal_validity_aging_runtime_design_policy.yaml")
    )

    lifecycle = build_signal_validity_lifecycle_contract(policy=policy, source=source)
    aging_rows = build_signal_validity_aging_rule_matrix(policy=policy, source=source)
    trigger_rows = build_signal_validity_trigger_aging_state_matrix(
        policy=policy,
        source=source,
    )
    release_rows = build_signal_validity_release_restore_rule_matrix(
        policy=policy,
        source=source,
    )
    runtime_schema = build_signal_validity_runtime_record_schema(
        policy=policy,
        source=source,
    )

    assert lifecycle["lifecycle_field_count"] == 6
    assert {
        row["field_id"] for row in lifecycle["lifecycle_fields"]
    } == set(REQUIRED_LIFECYCLE_FIELDS)
    assert all(
        row["runtime_field_executable_now"] is False
        for row in lifecycle["lifecycle_fields"]
    )
    assert len(aging_rows) == 5
    assert all(row["rule_status"] == "DESIGN_ONLY_NOT_EXECUTABLE" for row in aging_rows)
    assert all(row["decay_multiplier_defined"] is False for row in aging_rows)
    assert all(row["automatic_action_allowed"] is False for row in aging_rows)
    assert len(trigger_rows) == 4
    assert all(row["state_executable_now"] is False for row in trigger_rows)
    assert len(release_rows) == 4
    assert all(row["release_allowed_now"] is False for row in release_rows)
    assert all(row["automatic_restore_allowed"] is False for row in release_rows)
    assert runtime_schema["allowed_action_values"] == ["observe_only_design_contract"]
    assert set(REQUIRED_LIFECYCLE_FIELDS).issubset(runtime_schema["required_fields"])
    assert "broker_order" in runtime_schema["forbidden_fields"]


def test_signal_validity_aging_runtime_design_cli_writes_outputs(
    tmp_path: Path,
) -> None:
    source_dir = _write_trading_2321_source(tmp_path)
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "signal-validity-aging-runtime-design",
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
        "signal_validity_aging_runtime_design_summary.json",
        "signal_validity_lifecycle_contract.json",
        "signal_validity_aging_rule_matrix.json",
        "signal_validity_aging_rule_matrix.csv",
        "signal_validity_trigger_aging_state_matrix.json",
        "signal_validity_trigger_aging_state_matrix.csv",
        "signal_validity_release_restore_rule_matrix.json",
        "signal_validity_release_restore_rule_matrix.csv",
        "signal_validity_runtime_record_schema.json",
        "signal_validity_aging_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    for filename in ("target_weights.csv", "rebalance_instruction.json", "broker_order.json"):
        assert not (output_dir / filename).exists(), filename
    assert (docs_root / "signal_validity_aging_runtime_design.md").exists()

    summary_payload = _read_json(
        output_dir / "signal_validity_aging_runtime_design_summary.json"
    )
    summary = summary_payload["summary"]
    assert summary_payload["status"] == STATUS
    assert summary["status"] == STATUS
    assert summary["market_regime"] == "unified_primary_2021"
    assert summary["data_quality_status"] == DATA_QUALITY_STATUS
    assert summary["source_status"] == (
        "RISK_CAP_COOLDOWN_DECAY_DESIGN_READY_PROMOTION_BLOCKED"
    )
    assert summary["source_runtime_started"] is False
    assert summary["source_execution_runtime_started"] is False
    assert summary["source_state_count"] == 4
    assert summary["lifecycle_field_count"] == 6
    assert summary["aging_rule_count"] == 5
    assert summary["trigger_aging_state_count"] == 4
    assert summary["release_restore_rule_count"] == 4
    assert summary["aging_runtime_started"] is False
    assert summary["signal_validity_runtime_started"] is False
    assert summary["validity_runtime_executable"] is False
    assert summary["target_weight_generated"] is False
    assert summary["rebalance_instruction_generated"] is False
    assert summary["broker_order_generated"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"

    lifecycle = _read_json(output_dir / "signal_validity_lifecycle_contract.json")
    assert {
        row["field_id"] for row in lifecycle["lifecycle_fields"]
    } == set(REQUIRED_LIFECYCLE_FIELDS)
    assert "target_weight" in lifecycle["forbidden_outputs"]

    runtime_schema = _read_json(output_dir / "signal_validity_runtime_record_schema.json")
    assert runtime_schema["allowed_action_values"] == ["observe_only_design_contract"]
    assert set(REQUIRED_LIFECYCLE_FIELDS).issubset(runtime_schema["required_fields"])
    assert "rebalance_instruction" in runtime_schema["forbidden_fields"]

    safety = _read_json(output_dir / "signal_validity_aging_safety_boundary.json")
    assert safety["does_not_start_aging_runtime"] is True
    assert safety["does_not_write_runtime_records"] is True
    assert safety["does_not_read_portfolio_weights"] is True
    assert safety["does_not_write_portfolio_weights"] is True
    assert safety["does_not_generate_target_weights"] is True
    assert safety["does_not_generate_broker_order"] is True


def test_signal_validity_aging_runtime_design_direct_payload_has_string_paths(
    tmp_path: Path,
) -> None:
    payload = run_signal_validity_aging_runtime_design(
        source_dir=_write_trading_2321_source(tmp_path),
        output_dir=tmp_path / "out",
        docs_root=tmp_path / "docs",
    )

    assert payload["status"] == STATUS
    assert all(isinstance(path, str) for path in payload["artifact_paths"].values())


def test_signal_validity_aging_runtime_design_rejects_wrong_mode(
    tmp_path: Path,
) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "signal-validity-aging-runtime-design",
            "--source-dir",
            str(_write_trading_2321_source(tmp_path)),
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "runtime",
        ],
    )

    assert result.exit_code != 0


def test_signal_validity_aging_runtime_design_fails_closed_if_source_runtime_started(
    tmp_path: Path,
) -> None:
    source_dir = _write_trading_2321_source(tmp_path)
    summary_path = source_dir / "risk_cap_cooldown_decay_design_summary.json"
    payload = _read_json(summary_path)
    payload["runtime_started"] = True
    payload["summary"]["runtime_started"] = True
    summary_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    with pytest.raises(SignalValidityAgingRuntimeDesignError, match="runtime_started"):
        run_signal_validity_aging_runtime_design(
            source_dir=source_dir,
            output_dir=tmp_path / "out",
            docs_root=tmp_path / "docs",
        )


def test_signal_validity_aging_registry_catalog_and_flow_are_non_promotion() -> None:
    registry = safe_load_yaml_path(Path("config/report_registry.yaml"))
    entry = next(
        report
        for report in registry["reports"]
        if report["report_id"] == "signal_validity_aging_runtime_design"
    )

    assert entry["command"] == "aits research trends signal-validity-aging-runtime-design"
    assert entry["artifact_role"] == "signal_validity_aging_runtime_design_only"
    assert entry["data_quality_status"] == DATA_QUALITY_STATUS
    assert entry["validation_status"] == STATUS
    assert entry["source_status"] == "RISK_CAP_COOLDOWN_DECAY_DESIGN_READY_PROMOTION_BLOCKED"
    assert entry["design_only"] is True
    assert entry["aging_runtime_started"] is False
    assert entry["signal_validity_runtime_started"] is False
    assert entry["execution_runtime_started"] is False
    assert entry["portfolio_weights_read"] is False
    assert entry["portfolio_weights_written"] is False
    assert entry["target_weight_generated"] is False
    assert entry["rebalance_instruction_generated"] is False
    assert entry["broker_order_generated"] is False
    assert entry["validity_runtime_executable"] is False
    assert entry["aging_rule_executable"] is False
    assert entry["release_restore_rule_executable"] is False
    assert entry["lifecycle_field_count"] == 6
    assert entry["aging_rule_count"] == 5
    assert entry["trigger_aging_state_count"] == 4
    assert entry["release_restore_rule_count"] == 4
    assert entry["promotion_eligible"] is False
    assert entry["paper_shadow_allowed"] is False
    assert entry["production_allowed"] is False
    assert entry["production_effect"] == "none"
    assert entry["portfolio_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert entry["dynamic_promotion_status"] == "BLOCKED"

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    assert "signal_validity_aging_runtime_design" in catalog
    assert STATUS in catalog
    assert DATA_QUALITY_STATUS in catalog
    assert "不是启动 aging runtime" in catalog

    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    assert "TRADING-2322" in system_flow
    assert "signal-validity-aging-runtime-design" in system_flow
    assert DATA_QUALITY_STATUS in system_flow
    assert "aging_runtime_started=false" in system_flow


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
