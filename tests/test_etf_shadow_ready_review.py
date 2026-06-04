from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.etf_portfolio.shadow_ready_review import (
    DEFAULT_SHADOW_READY_REVIEW_POLICY_CONFIG_PATH,
    SHADOW_READY_REVIEW_SAFETY,
    ShadowReadyReviewError,
    aggregate_shadow_ready_review_candidates,
    build_near_shadow_review_summary,
    build_shadow_candidate_approved_enrollment,
    build_shadow_candidate_owner_approval,
    build_shadow_candidate_review_package,
    build_shadow_candidate_review_validation_report,
    load_shadow_ready_review_policy_config,
    load_shadow_review_diagnostics_artifacts,
    rank_shadow_ready_review_candidates,
)
from ai_trading_system.reports import reader_brief

GENERATED_AT = datetime(2026, 6, 4, 12, 0, tzinfo=UTC)
SHAPE_ID = "weight_shape_010_8ce67406f0"
WEIGHT_SET_ID = "weight_set_010"


def test_shadow_ready_review_policy_loads_and_rejects_unsafe(tmp_path: Path) -> None:
    policy = load_shadow_ready_review_policy_config()

    assert policy.safety.model_dump(mode="json") == SHADOW_READY_REVIEW_SAFETY
    assert policy.review_thresholds.min_shadow_ready_appearances == 2
    assert policy.enrollment_limits.max_enroll_per_review > 0

    raw = yaml.safe_load(
        DEFAULT_SHADOW_READY_REVIEW_POLICY_CONFIG_PATH.read_text(encoding="utf-8")
    )
    raw["safety"]["production_effect"] = "mutate_config"
    unsafe_path = tmp_path / "unsafe_shadow_review.yaml"
    unsafe_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(ShadowReadyReviewError):
        load_shadow_ready_review_policy_config(unsafe_path)

    raw = yaml.safe_load(
        DEFAULT_SHADOW_READY_REVIEW_POLICY_CONFIG_PATH.read_text(encoding="utf-8")
    )
    raw["ranking_weights"]["balanced_exposure_score"] = 0.15
    invalid_path = tmp_path / "invalid_weights.yaml"
    invalid_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(ShadowReadyReviewError):
        load_shadow_ready_review_policy_config(invalid_path)


def test_loader_aggregation_ranking_package_and_near_shadow(tmp_path: Path) -> None:
    diagnostics_dir = _write_diagnostics_artifacts(tmp_path)
    artifacts = load_shadow_review_diagnostics_artifacts(
        diagnostics_dir=diagnostics_dir,
        latest=True,
        generated_at=GENERATED_AT,
    )
    policy = load_shadow_ready_review_policy_config()
    aggregation = aggregate_shadow_ready_review_candidates(
        artifacts,
        policy=policy,
        generated_at=GENERATED_AT,
    )
    ranking = rank_shadow_ready_review_candidates(
        aggregation,
        policy=policy,
        generated_at=GENERATED_AT,
    )
    near_shadow = build_near_shadow_review_summary(artifacts, generated_at=GENERATED_AT)
    package = build_shadow_candidate_review_package(
        artifacts=artifacts,
        aggregation=aggregation,
        ranking=ranking,
        near_shadow_summary=near_shadow,
        policy=policy,
        generated_at=GENERATED_AT,
    )

    assert artifacts["artifact_status"] == "PASS"
    assert aggregation["shape_candidates"][0]["source_weight_set_ids"] == [WEIGHT_SET_ID]
    assert ranking["ranked_candidates"][0]["shape_id"] == SHAPE_ID
    assert ranking["ranked_candidates"][0]["review_status"] == "review_recommended"
    assert near_shadow["common_gaps"][0]["id"] == "FORWARD_EVIDENCE_MISSING"
    assert package["review_summary"]["top_candidate"] == SHAPE_ID
    assert package["top_review_candidates"][0]["source_weight_set_ids"] == [WEIGHT_SET_ID]
    assert package["production_effect"] == "none"


def test_owner_approval_and_enrollment_require_approval(tmp_path: Path) -> None:
    package = _build_package(tmp_path)
    policy = load_shadow_ready_review_policy_config()

    with pytest.raises(ShadowReadyReviewError):
        build_shadow_candidate_owner_approval(
            review_package=package,
            shape_id=SHAPE_ID,
            owner_decision="place_order",
            rationale="unsafe",
            confidence=0.8,
            policy=policy,
            created_at=GENERATED_AT,
        )

    approval = build_shadow_candidate_owner_approval(
        review_package=package,
        shape_id=SHAPE_ID,
        selected_weight_set_id=WEIGHT_SET_ID,
        owner_decision="approved_for_shadow",
        rationale="Owner approves forward shadow observation only.",
        confidence=0.82,
        decision_journal_link="decision_journal:test",
        policy=policy,
        created_at=GENERATED_AT,
    )
    enrollment = build_shadow_candidate_approved_enrollment(
        approval=approval,
        review_package=package,
        policy=policy,
        created_at=GENERATED_AT,
    )

    assert approval["owner_decision"] == "approved_for_shadow"
    assert enrollment["selected_weight_set_id"] == WEIGHT_SET_ID
    assert enrollment["forward_tracking_link"]["next_review_due"] == "2026-06-18"
    assert enrollment["production_state_mutated"] is False

    continue_review = build_shadow_candidate_owner_approval(
        review_package=package,
        shape_id=SHAPE_ID,
        selected_weight_set_id=WEIGHT_SET_ID,
        owner_decision="continue_review",
        rationale="Continue review.",
        confidence=0.5,
        policy=policy,
        created_at=GENERATED_AT,
    )
    with pytest.raises(ShadowReadyReviewError):
        build_shadow_candidate_approved_enrollment(
            approval=continue_review,
            review_package=package,
            policy=policy,
            created_at=GENERATED_AT,
        )


def test_shadow_review_cli_validation_and_reader_brief(tmp_path: Path) -> None:
    diagnostics_dir = _write_diagnostics_artifacts(tmp_path)
    runner = CliRunner()
    package_result = runner.invoke(
        etf_app,
        [
            "shadow-review",
            "package",
            "--diagnostics-dir",
            str(diagnostics_dir),
            "--output-dir",
            str(tmp_path / "packages"),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    assert package_result.exit_code == 0, package_result.output
    package_path = next((tmp_path / "packages").glob("*.json"))
    package = json.loads(package_path.read_text(encoding="utf-8"))

    approval_result = runner.invoke(
        etf_app,
        [
            "shadow-review",
            "approve",
            "--package",
            str(package_path),
            "--shape",
            SHAPE_ID,
            "--selected-weight-set-id",
            WEIGHT_SET_ID,
            "--owner-decision",
            "approved_for_shadow",
            "--rationale",
            "Owner approves forward shadow observation only.",
            "--confidence",
            "0.82",
            "--decision-journal-link",
            "decision_journal:test",
            "--output-dir",
            str(tmp_path / "approvals"),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    assert approval_result.exit_code == 0, approval_result.output
    approval_path = next((tmp_path / "approvals").glob("*.json"))

    enrollment_result = runner.invoke(
        etf_app,
        [
            "shadow-review",
            "enroll-approved",
            "--approval",
            str(approval_path),
            "--package",
            str(package_path),
            "--output-dir",
            str(tmp_path / "enrollments"),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    assert enrollment_result.exit_code == 0, enrollment_result.output
    enrollment_path = next((tmp_path / "enrollments").glob("*.json"))

    validation = build_shadow_candidate_review_validation_report(
        generated_at=GENERATED_AT,
    )
    assert validation["status"] == "PASS"

    validate_result = runner.invoke(
        etf_app,
        [
            "shadow-review",
            "validate",
            "--output-dir",
            str(tmp_path / "validation"),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    assert validate_result.exit_code == 0, validate_result.output

    summary = reader_brief._etf_shadow_candidate_review_summary(
        {
            "reports": [
                _report_record("etf_shadow_candidate_review_package", package_path),
                _report_record("etf_shadow_candidate_owner_approval", approval_path),
                _report_record("etf_shadow_candidate_enrollment", enrollment_path),
            ]
        }
    )
    assert summary["availability"] == "AVAILABLE"
    assert summary["top_candidate"] == package["review_summary"]["top_candidate"]
    assert summary["latest_owner_decision"] == "approved_for_shadow"
    assert summary["approved_enrollment_count"] == 1
    assert summary["safety_status"].startswith("observe_only=true")

    missing = reader_brief._etf_shadow_candidate_review_summary({"reports": []})
    assert missing["availability"] == "MISSING"


def _build_package(tmp_path: Path) -> dict[str, object]:
    diagnostics_dir = _write_diagnostics_artifacts(tmp_path)
    artifacts = load_shadow_review_diagnostics_artifacts(
        diagnostics_dir=diagnostics_dir,
        latest=True,
        generated_at=GENERATED_AT,
    )
    policy = load_shadow_ready_review_policy_config()
    package = build_shadow_candidate_review_package(
        artifacts=artifacts,
        policy=policy,
        generated_at=GENERATED_AT,
    )
    return package


def _write_diagnostics_artifacts(tmp_path: Path) -> Path:
    diagnostics_dir = tmp_path / "search_diagnostics"
    diagnostics_dir.mkdir(parents=True, exist_ok=True)
    stem = "historical_weight_search_diagnostics_20260604T120000"
    diagnostics_path = diagnostics_dir / f"{stem}.json"
    stable_path = diagnostics_dir / f"{stem}_stable_shapes.csv"
    near_path = diagnostics_dir / f"{stem}_near_shadow.csv"
    payload = _diagnostics_payload()
    diagnostics_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    with stable_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "stable_shape_id",
                "shape_key",
                "cross_preset_stability_score",
                "appearance_count",
                "preset_count",
                "search_count",
                "average_rank",
                "rank_consistency",
                "weight_shape_similarity",
                "regime_failure_count",
                "readiness_counts",
                "overfit_risk_counts",
                "representative_weights",
                "observe_only",
                "candidate_only",
                "production_effect",
                "broker_action",
                "manual_review_required",
            ],
        )
        writer.writeheader()
        shape = payload["cross_preset_stable_shapes"][0]
        writer.writerow(
            {
                "stable_shape_id": shape["stable_shape_id"],
                "shape_key": shape["shape_key"],
                "cross_preset_stability_score": shape["cross_preset_stability_score"],
                "appearance_count": shape["appearance_count"],
                "preset_count": shape["preset_count"],
                "search_count": shape["search_count"],
                "average_rank": shape["average_rank"],
                "rank_consistency": shape["rank_consistency"],
                "weight_shape_similarity": shape["weight_shape_similarity"],
                "regime_failure_count": shape["regime_failure_count"],
                "readiness_counts": json.dumps(shape["readiness_counts"]),
                "overfit_risk_counts": json.dumps(shape["overfit_risk_counts"]),
                "representative_weights": json.dumps(shape["representative_weights"]),
                "observe_only": "true",
                "candidate_only": "true",
                "production_effect": "none",
                "broker_action": "none",
                "manual_review_required": "true",
            }
        )
    with near_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "search_id",
                "preset_id",
                "rank",
                "weight_set_id",
                "shape_key",
                "overfit_risk",
                "distance_to_shadow_ready",
                "main_gaps",
                "rescue_suggestions",
                "observe_only",
                "candidate_only",
                "production_effect",
                "broker_action",
                "manual_review_required",
            ],
        )
        writer.writeheader()
        near = payload["near_shadow_candidates"][0]
        writer.writerow(
            {
                "search_id": near["search_id"],
                "preset_id": near["preset_id"],
                "rank": near["rank"],
                "weight_set_id": near["weight_set_id"],
                "shape_key": near["shape_key"],
                "overfit_risk": near["overfit_risk"],
                "distance_to_shadow_ready": near["distance_to_shadow_ready"],
                "main_gaps": json.dumps(near["main_gaps"]),
                "rescue_suggestions": json.dumps(near["rescue_suggestions"]),
                "observe_only": "true",
                "candidate_only": "true",
                "production_effect": "none",
                "broker_action": "none",
                "manual_review_required": "true",
            }
        )
    return diagnostics_dir


def _diagnostics_payload() -> dict[str, object]:
    return {
        "schema_version": "etf_weight_search_diagnostics_v1",
        "report_type": "etf_weight_search_diagnostics",
        "generated_at": GENERATED_AT.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "data_quality_status": "PASS",
        "run_manifest": {"run_id": "shadow-review-test-run"},
        "cross_preset_stable_shapes": [
            {
                "stable_shape_id": SHAPE_ID,
                "shape_key": "SPY=0.35|QQQ=0.35|SMH=0.15|SOXX=0.05|CASH=0.10",
                "representative_weights": {
                    "SPY": 0.35,
                    "QQQ": 0.35,
                    "SMH": 0.15,
                    "SOXX": 0.05,
                    "CASH": 0.10,
                },
                "appearance_count": 4,
                "shadow_ready_appearance_count": 3,
                "preset_count": 3,
                "search_count": 2,
                "preset_ids": ["ai_cycle_recent", "last_3y", "last_5y"],
                "search_ids": [
                    "etf_initial_weight_search_v1",
                    "etf_initial_weight_balanced_lower_semiconductor_v1",
                ],
                "average_rank": 2.25,
                "best_rank": 1,
                "cross_preset_stability_score": 0.82,
                "rank_consistency": 0.78,
                "weight_shape_similarity": 0.95,
                "regime_failure_count": 8,
                "readiness_counts": {"shadow_ready": 3, "needs_manual_review": 1},
                "overfit_risk_counts": {"low": 2, "medium": 2},
                "appearance_examples": [
                    {
                        "search_id": "etf_initial_weight_search_v1",
                        "preset_id": "ai_cycle_recent",
                        "rank": 1,
                        "weight_set_id": WEIGHT_SET_ID,
                        "forward_readiness_status": "shadow_ready",
                        "overfit_risk": "medium",
                    }
                ],
                **SHADOW_READY_REVIEW_SAFETY,
            }
        ],
        "near_shadow_candidates": [
            {
                "search_id": "etf_initial_weight_search_v1",
                "preset_id": "last_2y",
                "rank": 4,
                "weight_set_id": "weight_set_011",
                "shape_key": "SPY=0.30|QQQ=0.35|SMH=0.20|SOXX=0.05|CASH=0.10",
                "overfit_risk": "medium",
                "distance_to_shadow_ready": 0.2,
                "main_gaps": ["FORWARD_EVIDENCE_MISSING"],
                "rescue_suggestions": ["continue_forward_observation"],
                **SHADOW_READY_REVIEW_SAFETY,
            }
        ],
        **SHADOW_READY_REVIEW_SAFETY,
    }


def _report_record(report_id: str, path: Path) -> dict[str, object]:
    return {
        "report_id": report_id,
        "latest_artifact_path": str(path),
        "latest_artifact_name": path.name,
        "artifact_date": "2026-06-04",
        "freshness_status": "FRESH",
        "artifact_status": "PASS",
        "exists": True,
        "age_days": 0,
    }
