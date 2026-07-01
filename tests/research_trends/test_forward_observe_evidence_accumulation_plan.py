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
    ForwardObserveEvidenceAccumulationPlanError,
    load_forward_observe_evidence_accumulation_inputs,
)
from ai_trading_system.scope_narrowed_forward_observe_readiness_review import (
    CONFIRMATION_CANDIDATE_ID,
    RISK_APPETITE_ARCHIVE_CANDIDATE,
    RISK_CAP_CANDIDATE_ID,
    run_scope_narrowed_forward_observe_readiness_review,
)


def test_forward_observe_evidence_accumulation_plan_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "forward-observe-evidence-accumulation-plan" in result.output


def test_forward_observe_evidence_accumulation_plan_cli_writes_outputs(
    tmp_path: Path,
) -> None:
    readiness_dir = _write_readiness_review_fixture(tmp_path)
    output_dir = tmp_path / "evidence_accumulation_plan"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "forward-observe-evidence-accumulation-plan",
            "--readiness-dir",
            str(readiness_dir),
            "--candidate",
            RISK_CAP_CANDIDATE_ID,
            "--target-assets",
            "QQQ,SPY,SMH",
            "--horizons",
            "5d,10d,20d",
            "--output-dir",
            str(output_dir),
            "--mode",
            "evidence_accumulation_extension_plan",
            "--docs-root",
            str(docs_root),
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "forward_observe_evidence_accumulation_plan_summary.json",
        "forward_observe_evidence_accumulation_plan.json",
        "forward_observe_runtime_contract.json",
        "risk_cap_daily_observe_record_schema.json",
        "risk_cap_trigger_followup_schema.json",
        "forward_observe_storage_layout.json",
        "forward_observe_runtime_safety_boundary.json",
        "forward_observe_minimum_observation_policy.json",
        "forward_observe_weekly_review_cadence.json",
        "forward_observe_evidence_decision_matrix.json",
        "forward_observe_evidence_decision_matrix.csv",
        "forward_observe_runtime_design.md",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename

    summary = _read_json(output_dir / "forward_observe_evidence_accumulation_plan_summary.json")
    assert summary["candidate_reviewed"] == RISK_CAP_CANDIDATE_ID
    assert summary["source_readiness_gate_status"] == "FORWARD_OBSERVE_READY_WITH_WARNINGS"
    assert summary["forward_observe_started"] is False
    assert summary["runtime_started"] is False
    assert summary["daily_report_integration"] == "design_only"
    assert summary["weekly_report_integration"] == "design_only"
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"

    daily_schema = _read_json(output_dir / "risk_cap_daily_observe_record_schema.json")
    assert daily_schema["allowed_action_values"] == ["observe_only"]
    assert "data_quality_status" in daily_schema["required_fields"]
    assert "target_weight" in daily_schema["forbidden_fields"]

    followup_schema = _read_json(output_dir / "risk_cap_trigger_followup_schema.json")
    assert followup_schema["followup_horizons"] == ["5d", "10d", "20d"]
    assert "blocked_data_quality" in followup_schema["status_values"]

    assert (docs_root / "forward_observe_runtime_design.md").exists()
    assert (docs_root / "risk_cap_daily_observe_record_schema.md").exists()


def test_forward_observe_evidence_accumulation_plan_rejects_promotion_mode(
    tmp_path: Path,
) -> None:
    readiness_dir = _write_readiness_review_fixture(tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "forward-observe-evidence-accumulation-plan",
            "--readiness-dir",
            str(readiness_dir),
            "--mode",
            "promotion",
        ],
    )

    assert result.exit_code != 0


def test_forward_observe_evidence_accumulation_plan_rejects_unsafe_input(
    tmp_path: Path,
) -> None:
    readiness_dir = _write_readiness_review_fixture(tmp_path)
    summary_path = readiness_dir / "forward_observe_readiness_review_summary.json"
    payload = _read_json(summary_path)
    payload["promotion_allowed"] = True
    summary_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ForwardObserveEvidenceAccumulationPlanError):
        load_forward_observe_evidence_accumulation_inputs(
            readiness_dir=readiness_dir,
            candidate=RISK_CAP_CANDIDATE_ID,
            target_assets="QQQ,SPY,SMH",
            horizons="5d,10d,20d",
        )


def _write_readiness_review_fixture(tmp_path: Path) -> Path:
    fixture = build_scope_narrowed_forward_observe_readiness_fixture(tmp_path)
    output_dir = tmp_path / "forward_observe_readiness"
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
        docs_root=tmp_path / "readiness_docs",
    )
    _force_ready_with_warnings_fixture(output_dir)
    return output_dir


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


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
        payload["next_task_recommendation"] = "TRADING-2294_Evidence_Accumulation_Extension_Plan"
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
