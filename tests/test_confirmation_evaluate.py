from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from dynamic_v3_confirmation_cycle_helpers import evaluation_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_cycle import (
    DynamicV3ConfirmationCycleError,
    _evaluation_row_v2,
    confirmation_evaluation_report_payload,
    run_confirmation_evaluation,
    validate_confirmation_evaluation_artifact,
)


@pytest.fixture(scope="module")
def evaluation_bundle(tmp_path_factory: pytest.TempPathFactory) -> dict[str, object]:
    fixture = evaluation_fixture(tmp_path_factory.mktemp("confirmation-evaluation"))
    yield fixture
    fixture["_monkeypatch"].undo()


def test_confirmation_evaluate_not_ready_does_not_leak_partial_pass(
    evaluation_bundle: dict[str, object],
) -> None:
    fixture = evaluation_bundle
    evaluation = fixture["evaluation"]
    rows = {row["target_id"]: row for row in evaluation["target_evaluations"]}

    assert evaluation["confirmation_evaluation_summary"]["not_ready_count"] == 1
    assert evaluation["confirmation_evaluation_summary"]["success_count"] == 0
    assert evaluation["confirmation_evaluation_summary"]["failure_count"] == 0

    limited = rows["limited_adjustment_vs_no_trade"]
    assert limited["evaluation_status"] == "NOT_READY"
    assert all(
        row["status"] == "NOT_EVALUATED" and row["actual"] is None
        for row in limited["criteria_results"].values()
    )
    assert len(limited["failure_conditions"]) == 1
    assert limited["failure_conditions_triggered"] == []
    assert limited["recommendation"] == "continue_tracking"

    payload = confirmation_evaluation_report_payload(
        evaluation_id=evaluation["evaluation_id"],
        output_dir=fixture["evaluation_dir"],
    )
    assert payload["evaluation_id"] == evaluation["evaluation_id"]
    assert (
        validate_confirmation_evaluation_artifact(
            evaluation_id=evaluation["evaluation_id"],
            output_dir=fixture["evaluation_dir"],
        )["status"]
        == "PASS"
    )
    assert (evaluation["evaluation_dir"] / "confirmation_evaluation_input_snapshot.json").exists()


def test_confirmation_evaluate_validator_rejects_each_output_tamper(
    evaluation_bundle: dict[str, object],
) -> None:
    fixture = evaluation_bundle
    for artifact_name in (
        "confirmation_evaluation_manifest.json",
        "target_evaluations.jsonl",
        "confirmation_evaluation_summary.json",
        "confirmation_evaluation_input_snapshot.json",
        "confirmation_evaluation_report.md",
    ):
        path = fixture["evaluation"]["evaluation_dir"] / artifact_name
        original = path.read_text(encoding="utf-8")
        if path.suffix == ".json":
            payload = json.loads(original)
            payload["tampered"] = True
            tampered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
        elif path.suffix == ".jsonl":
            payload = json.loads(original.splitlines()[0])
            payload["tampered"] = True
            tampered = json.dumps(payload, sort_keys=True) + "\n"
        else:
            tampered = original + "tamper\n"
        path.write_text(tampered, encoding="utf-8")
        assert (
            validate_confirmation_evaluation_artifact(
                evaluation_id=fixture["evaluation"]["evaluation_id"],
                output_dir=fixture["evaluation_dir"],
            )["status"]
            == "FAIL"
        )
        path.write_text(original, encoding="utf-8")


def test_confirmation_evaluate_rejects_naive_cutoff_and_live_progress_drift(
    evaluation_bundle: dict[str, object],
) -> None:
    fixture = evaluation_bundle
    tmp_path = fixture["evaluation_dir"].parent
    with pytest.raises(DynamicV3ConfirmationCycleError, match="timezone-aware"):
        run_confirmation_evaluation(
            progress_id=fixture["progress"]["progress_id"],
            progress_dir=fixture["progress_dir"],
            output_dir=tmp_path / "naive-evaluation",
            generated_at=datetime(2026, 8, 1),
        )
    assert not (tmp_path / "naive-evaluation").exists()

    progress_report = fixture["progress"]["progress_dir"] / "confirmation_progress_report.md"
    progress_report.write_text(
        progress_report.read_text(encoding="utf-8") + "tamper\n", encoding="utf-8"
    )
    assert (
        validate_confirmation_evaluation_artifact(
            evaluation_id=fixture["evaluation"]["evaluation_id"],
            output_dir=fixture["evaluation_dir"],
        )["status"]
        == "FAIL"
    )
    with pytest.raises(DynamicV3ConfirmationCycleError, match="progress validation failed"):
        run_confirmation_evaluation(
            progress_id=fixture["progress"]["progress_id"],
            progress_dir=fixture["progress_dir"],
            output_dir=tmp_path / "drift-evaluation",
            generated_at=datetime(2026, 8, 1, tzinfo=UTC),
        )
    assert not (tmp_path / "drift-evaluation").exists()


def test_confirmation_evaluate_ready_requires_all_criteria_and_source_bound_failures() -> None:
    base = _ready_progress_row()
    success = _evaluation_row_v2(base, generated=datetime(2026, 8, 1, tzinfo=UTC))
    assert success["evaluation_status"] == "SUCCESS"
    assert success["failure_conditions_triggered"] == []

    failure = _evaluation_row_v2(
        {
            **base,
            "current_metrics": {
                **base["current_metrics"],
                "avg_relative_return": -0.01,
            },
        },
        generated=datetime(2026, 8, 1, tzinfo=UTC),
    )
    assert failure["evaluation_status"] == "FAILURE"
    assert failure["failure_conditions_triggered"][0] == {
        "condition": "underperforms_no_trade",
        "action": "tighten_or_disable_limited_adjustment_proposal",
        "criterion": "avg_relative_return_min",
        "required_boundary": 0.0,
        "actual": -0.01,
    }

    incomplete = _evaluation_row_v2(
        {**base, "current_metrics": {"avg_relative_return": 0.01}},
        generated=datetime(2026, 8, 1, tzinfo=UTC),
    )
    assert incomplete["evaluation_status"] == "REVIEW_REQUIRED"


def test_confirmation_evaluate_rejects_unknown_failure_condition() -> None:
    row = _ready_progress_row()
    row["failure_conditions"] = [
        {
            "target": row["target_id"],
            "condition": "unknown_failure_rule",
            "action": "manual_review",
        }
    ]
    with pytest.raises(DynamicV3ConfirmationCycleError, match="binding invalid"):
        _evaluation_row_v2(row, generated=datetime(2026, 8, 1, tzinfo=UTC))


def _ready_progress_row() -> dict[str, object]:
    return {
        "target_id": "limited_adjustment_vs_no_trade",
        "progress_status": "READY_FOR_EVALUATION",
        "success_criteria": {
            "avg_relative_return_min": 0.0,
            "avg_drawdown_delta_max": 0.0,
        },
        "current_metrics": {
            "avg_relative_return": 0.01,
            "avg_drawdown_delta": -0.01,
        },
        "failure_conditions": [
            {
                "target": "limited_adjustment_vs_no_trade",
                "condition": "underperforms_no_trade",
                "action": "tighten_or_disable_limited_adjustment_proposal",
            },
            {
                "target": "limited_adjustment_vs_no_trade",
                "condition": "drawdown_worsening_persists",
                "action": "do_not_loosen_rules",
            },
        ],
    }
