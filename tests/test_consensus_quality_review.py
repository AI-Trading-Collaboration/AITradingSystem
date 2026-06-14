from __future__ import annotations

from dynamic_v3_weight_batch_search_helpers import run_consensus_quality_review_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def test_consensus_quality_review_records_dispersion_and_failure_reason(tmp_path) -> None:
    fixture = run_consensus_quality_review_fixture(tmp_path)
    review = fixture["consensus_review"]

    assert review["manifest"]["status"] == "PASS"
    assert review["consensus_dispersion_summary"]["dispersion_status"]
    assert review["ensemble_method_quality"]
    assert review["consensus_failure_reasons"]["primary_failure_reason"]
    assert review["manifest"]["broker_action_allowed"] is False

    validation = weight_search.validate_consensus_quality_review_artifact(
        consensus_review_id=review["consensus_review_id"],
        output_dir=tmp_path / "consensus_quality_review",
    )
    assert validation["status"] == "PASS"
