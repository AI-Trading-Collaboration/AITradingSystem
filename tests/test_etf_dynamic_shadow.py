from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.etf_portfolio.dynamic_shadow import (
    DEFAULT_DYNAMIC_SHADOW_POLICY_CONFIG_PATH,
    DYNAMIC_SHADOW_SAFETY,
    DynamicShadowError,
    build_dynamic_shadow_approved_enrollment,
    build_dynamic_shadow_forward_update,
    build_dynamic_shadow_owner_approval,
    build_dynamic_shadow_review_package,
    build_dynamic_shadow_validation_report,
    build_dynamic_shadow_weekly_review,
    load_dynamic_shadow_policy_config,
    upsert_dynamic_shadow_candidate_registry,
    write_dynamic_shadow_approved_enrollment,
    write_dynamic_shadow_forward_update,
    write_dynamic_shadow_owner_approval,
    write_dynamic_shadow_review_package,
    write_dynamic_shadow_weekly_review,
)
from ai_trading_system.reports import reader_brief


def test_dynamic_shadow_policy_loads_and_rejects_unsafe(tmp_path: Path) -> None:
    policy = load_dynamic_shadow_policy_config()

    assert policy.safety.model_dump(mode="json") == DYNAMIC_SHADOW_SAFETY
    assert "approved_for_dynamic_shadow" in policy.owner_approval_policy.allowed_decisions
    assert "false_signal_count" in policy.forward_tracking.metrics

    raw = yaml.safe_load(DEFAULT_DYNAMIC_SHADOW_POLICY_CONFIG_PATH.read_text(encoding="utf-8"))
    raw["safety"]["auto_enrollment_without_owner_approval"] = True
    unsafe_path = tmp_path / "unsafe_dynamic_shadow.yaml"
    unsafe_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(DynamicShadowError):
        load_dynamic_shadow_policy_config(unsafe_path)

    raw = yaml.safe_load(DEFAULT_DYNAMIC_SHADOW_POLICY_CONFIG_PATH.read_text(encoding="utf-8"))
    raw["forward_tracking"]["metrics"].remove("constraint_hit_count")
    invalid_path = tmp_path / "invalid_dynamic_shadow.yaml"
    invalid_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(DynamicShadowError):
        load_dynamic_shadow_policy_config(invalid_path)


def test_dynamic_shadow_package_approval_and_enrollment_are_approved_only(
    tmp_path: Path,
) -> None:
    policy = load_dynamic_shadow_policy_config()
    package = build_dynamic_shadow_review_package(
        dynamic_robustness_report=_sample_robustness_report(),
        dynamic_calibration_report={"status": "PASS"},
        dynamic_calibration_validation={"status": "PASS"},
        dynamic_robustness_validation={"status": "PASS"},
        operations_validation={"status": "PASS"},
        policy=policy,
        source_paths={"dynamic_robustness_report": "sample_report.json"},
    )

    candidate = package["top_review_candidates"][0]
    assert candidate["hard_gate_status"] == "PASS"
    assert candidate["review_status"] == "owner_approval_required"
    assert package["review_summary"]["ready_after_owner_approval_count"] == 1

    with pytest.raises(DynamicShadowError):
        build_dynamic_shadow_owner_approval(
            review_package=package,
            candidate_id="dynamic-candidate-test",
            owner_decision="approved_for_dynamic_shadow",
            rationale="owner accepts forward shadow observation",
            confidence=0.8,
            policy=policy,
        )

    continue_review = build_dynamic_shadow_owner_approval(
        review_package=package,
        candidate_id="dynamic-candidate-test",
        owner_decision="continue_review",
        rationale="keep reviewing",
        confidence=0.5,
        policy=policy,
    )
    with pytest.raises(DynamicShadowError):
        build_dynamic_shadow_approved_enrollment(
            approval=continue_review,
            review_package=package,
            policy=policy,
        )

    approval = build_dynamic_shadow_owner_approval(
        review_package=package,
        candidate_id="dynamic-candidate-test",
        owner_decision="approved_for_dynamic_shadow",
        rationale="owner accepts forward shadow observation",
        confidence=0.8,
        decision_journal_link="journal:dynamic-shadow-test",
        policy=policy,
    )
    enrollment = build_dynamic_shadow_approved_enrollment(
        approval=approval,
        review_package=package,
        policy=policy,
        created_at=datetime(2026, 5, 1, tzinfo=UTC),
    )
    registry = upsert_dynamic_shadow_candidate_registry(_empty_registry(), enrollment)

    assert approval["approved_for_enrollment"] is True
    assert enrollment["tracking_status"] == "active_shadow"
    assert registry["candidate_count"] == 1
    assert registry["production_state_mutated"] is False

    package_paths = write_dynamic_shadow_review_package(package, output_dir=tmp_path / "packages")
    approval_paths = write_dynamic_shadow_owner_approval(
        approval,
        output_dir=tmp_path / "approvals",
    )
    enrollment_paths = write_dynamic_shadow_approved_enrollment(
        enrollment,
        output_dir=tmp_path / "enrollments",
    )
    assert package_paths["json"].exists()
    assert approval_paths["markdown"].exists()
    assert enrollment_paths["json"].exists()


def test_dynamic_shadow_forward_update_weekly_review_and_reader_brief(
    tmp_path: Path,
) -> None:
    policy = load_dynamic_shadow_policy_config()
    package = build_dynamic_shadow_review_package(
        dynamic_robustness_report=_sample_robustness_report(),
        dynamic_calibration_report={"status": "PASS"},
        dynamic_calibration_validation={"status": "PASS"},
        dynamic_robustness_validation={"status": "PASS"},
        operations_validation={"status": "PASS"},
        policy=policy,
    )
    approval = build_dynamic_shadow_owner_approval(
        review_package=package,
        candidate_id="dynamic-candidate-test",
        owner_decision="approved_for_dynamic_shadow",
        rationale="owner accepts forward shadow observation",
        confidence=0.8,
        decision_journal_link="journal:dynamic-shadow-test",
        policy=policy,
    )
    enrollment = build_dynamic_shadow_approved_enrollment(
        approval=approval,
        review_package=package,
        policy=policy,
        created_at=datetime(2026, 5, 1, tzinfo=UTC),
    )
    registry = upsert_dynamic_shadow_candidate_registry(_empty_registry(), enrollment)
    update = build_dynamic_shadow_forward_update(
        registry=registry,
        policy=policy,
        as_of="2026-06-05",
        data_quality_status="PASS",
        data_quality_report="validation_sample",
        robustness_reports_by_candidate={"dynamic-candidate-test": _sample_robustness_report()},
    )
    weekly = build_dynamic_shadow_weekly_review(
        forward_update=update,
        policy=policy,
        as_of="2026-06-05",
    )

    record = update["tracking_records"][0]
    metrics = record["metrics"]
    assert update["active_candidate_count"] == 1
    assert set(policy.forward_tracking.metrics).issubset(metrics)
    assert metrics["dynamic_candidate_return"] == pytest.approx(0.08)
    assert weekly["summary"]["candidate_count"] == 1
    assert weekly["candidate_reviews"][0]["review_status"] in {
        "active_shadow",
        "needs_more_data",
        "watch",
        "reject_pending_review",
    }

    package_paths = write_dynamic_shadow_review_package(package, output_dir=tmp_path / "packages")
    approval_paths = write_dynamic_shadow_owner_approval(
        approval,
        output_dir=tmp_path / "approvals",
    )
    enrollment_paths = write_dynamic_shadow_approved_enrollment(
        enrollment,
        output_dir=tmp_path / "enrollments",
    )
    update_paths = write_dynamic_shadow_forward_update(update, output_dir=tmp_path / "updates")
    weekly_paths = write_dynamic_shadow_weekly_review(weekly, output_dir=tmp_path / "weekly")
    summary = reader_brief._etf_dynamic_shadow_summary(
        {
            "reports": [
                _report_record("etf_dynamic_shadow_review_package", package_paths["json"]),
                _report_record("etf_dynamic_shadow_owner_approval", approval_paths["json"]),
                _report_record("etf_dynamic_shadow_enrollment", enrollment_paths["json"]),
                _report_record("etf_dynamic_shadow_forward_update", update_paths["json"]),
                _report_record("etf_dynamic_shadow_weekly_review", weekly_paths["json"]),
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["top_candidate"] == "dynamic-candidate-test"
    assert summary["latest_owner_decision"] == "approved_for_dynamic_shadow"
    assert summary["safety_status"].startswith("observe_only=true")
    assert summary["production_effect"] == "none"


def test_dynamic_shadow_validation_report_and_cli_pass(tmp_path: Path) -> None:
    validation = build_dynamic_shadow_validation_report()

    assert validation["status"] == "PASS"
    assert validation["failed_check_count"] == 0
    assert validation["approved_only_enrollment_required"] is True

    result = CliRunner().invoke(
        etf_app,
        [
            "dynamic-shadow",
            "validate",
            "--output-dir",
            str(tmp_path / "validation"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert result.exit_code == 0, result.output
    assert "status=PASS" in result.output
    assert "auto_enrollment_without_owner_approval=false" in result.output


def _sample_robustness_report() -> dict[str, object]:
    comparison = [
        _comparison_row("dynamic_candidate", 0.08, -0.08, 0.9),
        _comparison_row("static_base_candidate", 0.05, -0.09, 0.0),
        _comparison_row("current_etf_baseline", 0.04, -0.10, 0.0),
        _comparison_row("QQQ_buy_and_hold", 0.06, -0.11, 0.0),
        _comparison_row("SPY_buy_and_hold", 0.03, -0.07, 0.0),
        _comparison_row("SMH_buy_and_hold", 0.07, -0.12, 0.0),
    ]
    return {
        "schema_version": "etf_dynamic_robustness_report_v1",
        "report_type": "etf_dynamic_robustness_report",
        "dynamic_robustness_report_id": "dynamic-robustness-report_test",
        "status": "PASS",
        "summary": {
            "dynamic_candidate_id": "dynamic-candidate-test",
            "market_regime": "ai_after_chatgpt",
            "data_quality_status": "PASS",
            "dynamic_total_return": 0.08,
            "dynamic_max_drawdown": -0.08,
            "excess_vs_static_base": 0.03,
            "false_risk_off_count": 1,
            "false_risk_on_count": 1,
            "overfit_status": "PASS",
        },
        "comparison_table": comparison,
        "false_signal_diagnostics": {
            "false_risk_off": {"event_count": 1},
            "false_risk_on": {"event_count": 1},
        },
        "daily_path_summary": {
            "row_count": 30,
            "regime_switch_count": 2,
            "constraint_hit_count": 1,
        },
        "source_artifacts": {"data_quality_report": "validation_sample"},
        "safety": dict(DYNAMIC_SHADOW_SAFETY),
        **DYNAMIC_SHADOW_SAFETY,
        "commands_executed": False,
    }


def _comparison_row(
    comparison_id: str,
    total_return: float,
    max_drawdown: float,
    turnover: float,
) -> dict[str, object]:
    return {
        "comparison_id": comparison_id,
        "status": "AVAILABLE",
        "total_return": total_return,
        "CAGR": total_return,
        "max_drawdown": max_drawdown,
        "turnover": turnover,
        "trading_days": 30,
        "production_effect": "none",
        "broker_action": "none",
    }


def _empty_registry() -> dict[str, object]:
    return {
        "schema_version": "etf_dynamic_shadow_candidate_registry_v1",
        "active_candidates": [],
        "candidate_count": 0,
        "production_effect": "none",
        "broker_action": "none",
        "observe_only": True,
        "candidate_only": True,
        "manual_review_required": True,
        "production_state_mutated": False,
        "baseline_config_mutated": False,
        "official_target_weights_mutated": False,
        "automatic_candidate_promotion": False,
        "auto_enrollment_without_owner_approval": False,
    }


def _report_record(report_id: str, path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        "report_id": report_id,
        "latest_artifact_path": str(path),
        "latest_artifact_name": path.name,
        "artifact_date": "2026-06-05",
        "freshness_status": "FRESH",
        "artifact_status": payload.get("status", "PASS"),
        "exists": True,
        "age_days": 0,
    }
