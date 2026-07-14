from __future__ import annotations

from dynamic_v3_system_target_helpers import (
    report_index_for_review_fixture,
    run_review_fixture,
    write_long_market_cache,
    write_paper_shadow_backfill_config,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.reports import reader_brief


def test_smoothed_review_pack_keeps_research_only_boundary_and_reader_brief(tmp_path) -> None:
    prices_path, rates_path = write_long_market_cache(tmp_path / "market_cache")
    config = write_paper_shadow_backfill_config(tmp_path, prices_path=prices_path)
    smoothed = system_target.run_smoothed_backfill(
        config_path=config["config_path"],
        output_dir=tmp_path / "smoothed_backfill",
        paper_shadow_backfill_dir=tmp_path / "paper_shadow_backfill",
        price_cache_path=prices_path,
        rates_cache_path=rates_path,
    )
    risk_capped = smoothed["source_risk_capped_backfill"]
    comparison = system_target.run_smoothed_comparison(
        smoothed_backfill_id=smoothed["smoothed_backfill_id"],
        baseline_backfill_id=smoothed["source_paper_shadow_backfill"]["backfill_id"],
        risk_capped_backfill_id=risk_capped["risk_capped_backfill_id"],
        smoothed_backfill_dir=tmp_path / "smoothed_backfill",
        baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
        risk_capped_backfill_dir=tmp_path / "risk_capped_backfill",
        output_dir=tmp_path / "smoothed_comparison",
    )

    review = system_target.build_smoothed_review_pack(
        comparison_id=comparison["comparison_id"],
        smoothed_backfill_id=smoothed["smoothed_backfill_id"],
        comparison_dir=tmp_path / "smoothed_comparison",
        smoothed_backfill_dir=tmp_path / "smoothed_backfill",
        output_dir=tmp_path / "smoothed_review",
    )

    decision = review["smoothed_decision"]
    assert decision["decision"] == "CONTINUE_OBSERVATION"
    assert decision["recommended_method"] is None
    assert decision["secondary_method"] is None
    assert decision["observation_candidates"]
    assert not any(row["promotion_eligible"] for row in decision["candidate_evidence"])
    assert decision["research_target_only"] is True
    assert decision["not_official_target_weights"] is True
    assert decision["broker_action_allowed"] is False
    assert decision["production_effect"] == "none"
    assert decision["requires_forward_confirmation"] is True
    assert "Dynamic Rescue Smoothed Research Method Review" in review["reader_brief_section"]

    validation = system_target.validate_smoothed_review_artifact(
        review_id=review["review_id"],
        output_dir=tmp_path / "smoothed_review",
    )
    assert validation["status"] == "PASS"

    review_fixture = run_review_fixture(tmp_path / "reader_brief_review")
    smoothed_target = system_target.generate_smoothed_limited_target(
        target_id=review_fixture["target"]["target_id"],
        model_target_dir=tmp_path / "reader_brief_review" / "model_target",
        output_dir=tmp_path / "smoothed_limited",
    )
    report_index = report_index_for_review_fixture(review_fixture)
    report_index["reports"].extend(
        [
            {
                "report_id": "etf_dynamic_v3_smoothed_limited",
                "latest_artifact_path": str(
                    smoothed_target["smoothed_dir"] / "smoothed_limited_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_smoothed_backfill",
                "latest_artifact_path": str(
                    smoothed["smoothed_backfill_dir"] / "smoothed_backfill_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_smoothed_comparison",
                "latest_artifact_path": str(
                    comparison["comparison_dir"] / "smoothed_comparison_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_smoothed_review",
                "latest_artifact_path": str(
                    review["review_dir"] / "smoothed_review_manifest.json"
                ),
            },
        ]
    )
    summary = reader_brief._etf_dynamic_v3_system_target_summary(report_index)

    assert summary["smoothed_id"] == smoothed_target["smoothed_id"]
    assert summary["smoothed_backfill_id"] == smoothed["smoothed_backfill_id"]
    assert summary["smoothed_comparison_id"] == comparison["comparison_id"]
    assert summary["smoothed_review_id"] == review["review_id"]
    assert summary["smoothed_decision"] == decision["decision"]
    assert summary["smoothed_requires_forward_confirmation"] is True
    assert summary["broker_action_allowed"] is False
