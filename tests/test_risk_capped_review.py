from __future__ import annotations

from datetime import datetime, timedelta

from dynamic_v3_system_target_helpers import (
    report_index_for_review_fixture,
    run_review_fixture,
    write_long_market_cache,
    write_paper_shadow_backfill_config,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.reports import reader_brief


def test_risk_capped_review_pack_keeps_research_only_boundary(tmp_path) -> None:
    prices_path, rates_path = write_long_market_cache(tmp_path / "market_cache")
    config = write_paper_shadow_backfill_config(tmp_path, prices_path=prices_path)
    backfill = system_target.run_risk_capped_backfill(
        config_path=config["config_path"],
        output_dir=tmp_path / "risk_capped_backfill",
        paper_shadow_backfill_dir=tmp_path / "paper_shadow_backfill",
        price_cache_path=prices_path,
        rates_cache_path=rates_path,
    )
    comparison = system_target.run_risk_capped_comparison(
        risk_capped_backfill_id=backfill["risk_capped_backfill_id"],
        baseline_backfill_id=backfill["source_paper_shadow_backfill"]["backfill_id"],
        risk_capped_backfill_dir=tmp_path / "risk_capped_backfill",
        baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "risk_capped_comparison",
    )

    review = system_target.build_risk_capped_review_pack(
        comparison_id=comparison["comparison_id"],
        risk_capped_backfill_id=backfill["risk_capped_backfill_id"],
        comparison_dir=tmp_path / "risk_capped_comparison",
        risk_capped_backfill_dir=tmp_path / "risk_capped_backfill",
        output_dir=tmp_path / "risk_capped_review",
    )

    decision = review["risk_capped_decision"]
    assert decision["candidate_method"] == "risk_capped_limited_adjustment"
    assert decision["research_target_only"] is True
    assert decision["not_official_target_weights"] is True
    assert decision["broker_action_allowed"] is False
    assert decision["production_effect"] == "none"
    assert decision["requires_forward_confirmation"] is True
    assert "Dynamic Rescue Risk-Capped Research Method Review" in review["reader_brief_section"]

    validation = system_target.validate_risk_capped_review_artifact(
        review_id=review["review_id"],
        output_dir=tmp_path / "risk_capped_review",
    )
    assert validation["status"] == "PASS"

    review_fixture = run_review_fixture(tmp_path / "reader_brief_review")
    target_generated = datetime.fromisoformat(
        review_fixture["target"]["manifest"]["generated_at"]
    )
    risk_capped_target = system_target.generate_risk_capped_limited_target(
        target_id=review_fixture["target"]["target_id"],
        model_target_dir=tmp_path / "reader_brief_review" / "model_target",
        output_dir=tmp_path / "risk_capped_limited",
        generated_at=target_generated + timedelta(seconds=1),
    )
    report_index = report_index_for_review_fixture(review_fixture)
    report_index["reports"].extend(
        [
            {
                "report_id": "etf_dynamic_v3_risk_capped_limited",
                "latest_artifact_path": str(
                    risk_capped_target["risk_capped_dir"]
                    / "risk_capped_limited_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_risk_capped_backfill",
                "latest_artifact_path": str(
                    backfill["risk_capped_backfill_dir"]
                    / "risk_capped_backfill_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_risk_capped_comparison",
                "latest_artifact_path": str(
                    comparison["comparison_dir"]
                    / "risk_capped_comparison_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_risk_capped_review",
                "latest_artifact_path": str(
                    review["review_dir"] / "risk_capped_review_manifest.json"
                ),
            },
        ]
    )
    summary = reader_brief._etf_dynamic_v3_system_target_summary(report_index)

    assert summary["risk_capped_id"] == risk_capped_target["risk_capped_id"]
    assert summary["risk_capped_backfill_id"] == backfill["risk_capped_backfill_id"]
    assert summary["risk_capped_comparison_id"] == comparison["comparison_id"]
    assert summary["risk_capped_review_id"] == review["review_id"]
    assert summary["risk_capped_decision"] == decision["decision"]
    assert summary["risk_capped_requires_forward_confirmation"] is True
    assert summary["broker_action_allowed"] is False
