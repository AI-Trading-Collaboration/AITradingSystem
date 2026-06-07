from __future__ import annotations

import json
from pathlib import Path

from dynamic_v3_position_readiness_helpers import position_advisory_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    build_position_review_pack,
    validate_position_review_artifact,
)
from ai_trading_system.reports import reader_brief


def test_position_review_pack_go_no_go_stays_not_production_ready(tmp_path: Path) -> None:
    fixture = position_advisory_fixture(tmp_path)
    result = build_position_review_pack(
        shortlist_id=fixture["shortlist"]["shortlist_id"],
        cluster_id=fixture["cluster"]["cluster_id"],
        shadow_shortlist_id=fixture["shadow"]["shadow_shortlist_id"],
        advisory_id=fixture["advisory"]["advisory_id"],
        shortlist_dir=tmp_path / "shortlist",
        cluster_dir=tmp_path / "candidate_cluster",
        shadow_shortlist_dir=tmp_path / "shadow_shortlist",
        advisory_dir=tmp_path / "position_advisory",
        output_dir=tmp_path / "position_review",
    )

    decision = result["go_no_go_decision"]
    assert decision["shadow_observation_readiness"] == "READY_WITH_WARNINGS"
    assert decision["position_advisory_readiness"] == "TARGET_ONLY"
    assert decision["production_readiness"] == "NOT_READY"
    assert decision["broker_action_allowed"] is False
    assert (
        validate_position_review_artifact(
            review_id=result["review_id"],
            output_dir=tmp_path / "position_review",
        )["status"]
        == "PASS"
    )


def test_position_readiness_chain_flows_into_reader_brief(tmp_path: Path) -> None:
    fixture = position_advisory_fixture(tmp_path)
    review = build_position_review_pack(
        shortlist_id=fixture["shortlist"]["shortlist_id"],
        cluster_id=fixture["cluster"]["cluster_id"],
        shadow_shortlist_id=fixture["shadow"]["shadow_shortlist_id"],
        advisory_id=fixture["advisory"]["advisory_id"],
        shortlist_dir=tmp_path / "shortlist",
        cluster_dir=tmp_path / "candidate_cluster",
        shadow_shortlist_dir=tmp_path / "shadow_shortlist",
        advisory_dir=tmp_path / "position_advisory",
        output_dir=tmp_path / "position_review",
    )
    first_candidate = fixture["shortlist"]["candidates"][0]
    leaderboard_dir = tmp_path / "leaderboard"
    leaderboard_dir.mkdir(parents=True)
    leaderboard_path = leaderboard_dir / "leaderboard.json"
    leaderboard_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "status": "completed",
                "candidate_count": fixture["shortlist"]["manifest"]["shortlist_count"],
                "evaluator_mode": "real_dynamic_v3_rescue",
                "evaluator_version": "test",
                "metrics_source": "real_evaluation_artifact",
                "not_for_investment_decision": False,
                "production_candidate_generated": False,
                "automatic_candidate_promotion": False,
                "shadow_enrollment_allowed": False,
                "top_eligible_candidates": [
                    {
                        "candidate_id": first_candidate["candidate_id"],
                        "gate": "OBSERVE_ONLY",
                        "score": first_candidate["shortlist_score"],
                    }
                ],
                "most_common_reject_reasons": [],
                "recommended_next_actions": ["owner_review_required"],
                "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
            }
        ),
        encoding="utf-8",
    )

    summary = reader_brief._etf_dynamic_v3_parameter_research_summary(
        {
            "reports": [
                _report_record("etf_dynamic_v3_parameter_sweep_leaderboard", leaderboard_path),
                _report_record(
                    "etf_dynamic_v3_shortlist",
                    tmp_path
                    / "shortlist"
                    / fixture["shortlist"]["shortlist_id"]
                    / "shortlist_manifest.json",
                ),
                _report_record(
                    "etf_dynamic_v3_candidate_cluster",
                    tmp_path
                    / "candidate_cluster"
                    / fixture["cluster"]["cluster_id"]
                    / "cluster_manifest.json",
                ),
                _report_record(
                    "etf_dynamic_v3_shadow_shortlist",
                    tmp_path
                    / "shadow_shortlist"
                    / fixture["shadow"]["shadow_shortlist_id"]
                    / "shadow_shortlist_manifest.json",
                ),
                _report_record(
                    "etf_dynamic_v3_position_advisory",
                    tmp_path
                    / "position_advisory"
                    / fixture["advisory"]["advisory_id"]
                    / "position_advisory_manifest.json",
                ),
                _report_record(
                    "etf_dynamic_v3_position_review",
                    tmp_path
                    / "position_review"
                    / review["review_id"]
                    / "position_review_manifest.json",
                ),
            ]
        }
    )

    assert summary["shortlist_count"] == fixture["shortlist"]["manifest"]["shortlist_count"]
    assert (
        summary["shadow_shortlist_candidate_count"]
        == fixture["shadow"]["manifest"]["shadow_candidate_count"]
    )
    assert summary["position_advisory_status"] == "TARGET_ONLY"
    assert summary["position_advisory_broker_action_allowed"] is False
    assert summary["position_advisory_readiness"] == "TARGET_ONLY"
    assert summary["production_readiness"] == "NOT_READY"
    assert summary["production_candidate_generated"] is False
    assert summary["shadow_enrollment_allowed"] is False


def _report_record(report_id: str, path: Path) -> dict[str, object]:
    return {
        "report_id": report_id,
        "latest_artifact_path": str(path),
        "latest_artifact_name": path.name,
        "artifact_date": "2026-06-08",
        "freshness_status": "FRESH",
        "artifact_status": "PASS",
        "exists": True,
        "age_days": 0,
    }
