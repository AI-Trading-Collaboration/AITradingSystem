from __future__ import annotations

from ai_trading_system.refined_candidate_local_edge_scope_review import (
    build_risk_appetite_reject_record,
)


def test_risk_appetite_reject_record_current_form_boundary() -> None:
    record = build_risk_appetite_reject_record(
        state_rows=[
            {
                "refined_candidate_id": "risk_appetite_refined_confidence_v1",
                "original_candidate_id": "risk_appetite",
                "recommended_research_status": ("REFINED_ACTUAL_PATH_VALIDATED_REJECT_RECOMMENDED"),
            }
        ],
        comparison_rows=[
            {
                "refined_candidate_id": "risk_appetite_refined_confidence_v1",
                "original_candidate_id": "risk_appetite",
                "comparison_label": "REFINED_WORSE",
            }
        ],
        reject_candidates=("risk_appetite_refined_confidence_v1",),
    )

    assert record["reject_scope"] == "current_form"
    assert record["recommended_future_action"] == "archive_current_form"
    assert record["promotion_allowed"] is False
    assert record["paper_shadow_allowed"] is False
    assert record["production_allowed"] is False
    assert record["broker_action"] == "none"
    assert record["risk_appetite_concept_permanently_rejected"] is False
