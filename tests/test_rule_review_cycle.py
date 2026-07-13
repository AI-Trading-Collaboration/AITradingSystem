from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from dynamic_v3_confirmation_cycle_helpers import cycle_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_confirmation_cycle as cycle_module
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_cycle import (
    DynamicV3ConfirmationCycleError,
    create_rule_owner_decision,
    record_rule_owner_decision,
    rule_review_cycle_report_payload,
    run_rule_review_cycle,
    validate_rule_review_cycle_artifact,
)
from ai_trading_system.reports import reader_brief


@pytest.fixture(scope="module")
def review_cycle_bundle(tmp_path_factory: pytest.TempPathFactory) -> dict[str, object]:
    fixture = cycle_fixture(tmp_path_factory.mktemp("rule-review-cycle"))
    yield fixture
    fixture["_monkeypatch"].undo()


def test_rule_review_cycle_defaults_to_continue_tracking_without_policy_change(
    review_cycle_bundle: dict[str, object],
) -> None:
    fixture = review_cycle_bundle
    review_cycle = fixture["cycle"]
    matrix = review_cycle["rule_review_decision_matrix"]
    decisions = {row["target_id"]: row for row in matrix["targets"]}

    assert matrix["cycle_recommendation"] == "continue_tracking"
    assert matrix["policy_change_allowed"] is False
    assert decisions["limited_adjustment_vs_no_trade"]["rule_review_decision"] == (
        "CONTINUE_TRACKING"
    )
    assert list(decisions) == ["limited_adjustment_vs_no_trade"]
    assert all(row["policy_change_allowed"] is False for row in matrix["targets"])
    assert "Dynamic Rescue Rule Review Cycle" in review_cycle["reader_brief_section"]

    payload = rule_review_cycle_report_payload(
        cycle_id=review_cycle["cycle_id"],
        output_dir=fixture["cycle_dir"],
    )
    assert payload["cycle_id"] == review_cycle["cycle_id"]
    assert payload["input_snapshot"]["schema_version"] == "rule_review_cycle_input_snapshot.v2"
    assert payload["input_snapshot"]["registry_validation"]["status"] == "PASS"
    assert payload["input_snapshot"]["progress_validation"]["status"] == "PASS"
    assert payload["input_snapshot"]["evaluation_validation"]["status"] == "PASS"
    assert "progress_ready_for_evaluation_count: 0" in payload["reader_brief_section"]
    assert "evaluation_success_count: 0" in payload["reader_brief_section"]

    validation = validate_rule_review_cycle_artifact(
        cycle_id=review_cycle["cycle_id"],
        output_dir=fixture["cycle_dir"],
    )
    assert validation["status"] == "PASS"


def test_rule_review_cycle_rejects_any_output_view_tamper(
    review_cycle_bundle: dict[str, object],
) -> None:
    fixture = review_cycle_bundle
    cycle_dir = fixture["cycle"]["cycle_dir"]
    artifact_names = (
        "rule_review_cycle_manifest.json",
        "rule_review_decision_matrix.json",
        "rule_review_cycle_input_snapshot.json",
        "rule_review_cycle_report.md",
        "reader_brief_section.md",
    )

    for name in artifact_names:
        path = cycle_dir / name
        original = path.read_bytes()
        path.write_bytes(original + b"\n")
        validation = validate_rule_review_cycle_artifact(
            cycle_id=fixture["cycle"]["cycle_id"],
            output_dir=fixture["cycle_dir"],
        )
        assert validation["status"] == "FAIL", name
        path.write_bytes(original)

    validation = validate_rule_review_cycle_artifact(
        cycle_id=fixture["cycle"]["cycle_id"],
        output_dir=fixture["cycle_dir"],
    )
    assert validation["status"] == "PASS"


def test_rule_review_cycle_uses_generic_owner_review_and_preserves_source_actions() -> None:
    row = cycle_module._rule_review_decision_row(
        {"target_id": "future_target_without_special_case", "status": "active"},
        progress={
            "progress_status": "READY_FOR_EVALUATION",
            "available_forward_events": 12,
            "required_forward_events": 10,
        },
        evaluation={
            "evaluation_status": "FAILURE",
            "criteria_results": {
                "source_metric_max": {"required": 0.0, "actual": 0.1, "status": "FAIL"}
            },
            "failure_conditions": [
                {
                    "condition": "source_condition",
                    "criterion": "source_metric_max",
                    "action": "source_owned_manual_action",
                }
            ],
            "failure_conditions_triggered": [
                {
                    "condition": "source_condition",
                    "criterion": "source_metric_max",
                    "action": "source_owned_manual_action",
                }
            ],
            "recommendation": "source_owned_manual_action",
        },
    )

    assert row["rule_review_decision"] == "READY_FOR_OWNER_REVIEW"
    assert row["owner_action_required"] is True
    assert row["evaluation_recommendation"] == "source_owned_manual_action"
    assert row["failure_conditions_triggered"][0]["action"] == "source_owned_manual_action"
    assert row["policy_change_allowed"] is False


def test_rule_review_cycle_rejects_naive_cutoff_before_output(
    review_cycle_bundle: dict[str, object], tmp_path: Path
) -> None:
    fixture = review_cycle_bundle
    output_dir = tmp_path / "naive-cutoff-output"

    with pytest.raises(DynamicV3ConfirmationCycleError, match="timezone-aware"):
        run_rule_review_cycle(
            registry_id=fixture["registry"]["registry_id"],
            progress_id=fixture["progress"]["progress_id"],
            evaluation_id=fixture["evaluation"]["evaluation_id"],
            registry_dir=fixture["registry_dir"],
            progress_dir=fixture["progress_dir"],
            evaluation_dir=fixture["evaluation_dir"],
            output_dir=output_dir,
            generated_at=datetime(2026, 8, 1, 1),
        )

    assert not output_dir.exists()


@pytest.mark.parametrize(
    ("source_key", "artifact_name"),
    [
        ("registry", "registered_targets.yaml"),
        ("progress", "target_progress.jsonl"),
        ("evaluation", "target_evaluations.jsonl"),
    ],
)
def test_rule_review_cycle_rejects_live_source_drift_before_output(
    review_cycle_bundle: dict[str, object],
    tmp_path: Path,
    source_key: str,
    artifact_name: str,
) -> None:
    fixture = review_cycle_bundle
    source_dirs = {
        "registry": fixture["registry"]["registry_dir"],
        "progress": fixture["progress"]["progress_dir"],
        "evaluation": fixture["evaluation"]["evaluation_dir"],
    }
    source_path = source_dirs[source_key] / artifact_name
    original = source_path.read_bytes()
    output_dir = tmp_path / f"{source_key}-drift-output"
    try:
        source_path.write_bytes(original + b"\n")
        with pytest.raises(DynamicV3ConfirmationCycleError, match="validation failed"):
            run_rule_review_cycle(
                registry_id=fixture["registry"]["registry_id"],
                progress_id=fixture["progress"]["progress_id"],
                evaluation_id=fixture["evaluation"]["evaluation_id"],
                registry_dir=fixture["registry_dir"],
                progress_dir=fixture["progress_dir"],
                evaluation_dir=fixture["evaluation_dir"],
                output_dir=output_dir,
                generated_at=datetime(2026, 8, 1, 1, tzinfo=UTC),
            )
        assert not output_dir.exists()
    finally:
        source_path.write_bytes(original)


def test_reader_brief_surfaces_confirmation_cycle_fields(
    review_cycle_bundle: dict[str, object],
) -> None:
    fixture = review_cycle_bundle
    created = create_rule_owner_decision(
        cycle_id=fixture["cycle"]["cycle_id"],
        cycle_dir=fixture["cycle_dir"],
        journal_path=fixture["journal_path"],
        generated_at=datetime(2026, 6, 10, 4, tzinfo=UTC),
    )
    record_rule_owner_decision(
        decision_id=created["decision_id"],
        decision="continue_tracking",
        notes="Continue forward evidence accumulation.",
        journal_path=fixture["journal_path"],
        generated_at=datetime(2026, 6, 10, 5, tzinfo=UTC),
    )

    report_index = {
        "reports": [
            _report(
                "etf_dynamic_v3_forward_confirmation_plan",
                fixture["confirmation_plan_root"]
                / fixture["confirmation_plan_id"]
                / "confirmation_plan_manifest.json",
            ),
            _report(
                "etf_dynamic_v3_confirmation_registry",
                fixture["registry_yaml_path"],
            ),
            _report(
                "etf_dynamic_v3_confirmation_progress",
                fixture["progress"]["progress_dir"] / "confirmation_progress_manifest.json",
            ),
            _report(
                "etf_dynamic_v3_confirmation_evaluation",
                fixture["evaluation"]["evaluation_dir"] / "confirmation_evaluation_manifest.json",
            ),
            _report(
                "etf_dynamic_v3_rule_review_cycle",
                fixture["cycle"]["cycle_dir"] / "rule_review_cycle_manifest.json",
            ),
            _report(
                "etf_dynamic_v3_rule_owner_decision",
                fixture["journal_path"].parent / "rule_owner_decision_report.md",
            ),
        ]
    }

    summary = reader_brief._etf_dynamic_v3_sim_review_summary(report_index)

    assert summary["availability"] == "PARTIAL"
    assert summary["status"] == "AVAILABLE"
    assert summary["confirmation_registry_id"] == fixture["registry"]["registry_id"]
    assert summary["confirmation_progress_id"] == fixture["progress"]["progress_id"]
    assert summary["confirmation_evaluation_id"] == fixture["evaluation"]["evaluation_id"]
    assert summary["rule_review_cycle_id"] == fixture["cycle"]["cycle_id"]
    assert summary["rule_review_cycle_recommendation"] == "continue_tracking"
    assert summary["confirmation_ready_for_evaluation_count"] == 0
    assert summary["confirmation_insufficient_events_count"] == 1
    assert summary["confirmation_success_count"] == 0
    assert summary["confirmation_not_ready_count"] == 1
    assert summary["rule_review_policy_change_allowed"] is False
    assert summary["rule_owner_decision_id"] == created["decision_id"]
    assert summary["rule_owner_decision"] == "continue_tracking"


def _report(report_id: str, path: Path) -> dict[str, str]:
    return {"report_id": report_id, "latest_artifact_path": str(path)}
