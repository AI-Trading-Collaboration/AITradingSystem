from __future__ import annotations

from dynamic_v3_system_target_helpers import run_smoothed_review_chain_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.reports import reader_brief


def test_smoothed_watch_pack_outputs_reader_brief_section_and_safety(tmp_path) -> None:
    fixture = run_smoothed_review_chain_fixture(tmp_path)
    attribution = system_target.run_smoothed_review_attribution(
        review_id=fixture["review"]["review_id"],
        comparison_id=fixture["comparison"]["comparison_id"],
        backfill_id=fixture["smoothed"]["smoothed_backfill_id"],
        review_dir=tmp_path / "smoothed_review",
        comparison_dir=tmp_path / "smoothed_comparison",
        backfill_dir=tmp_path / "smoothed_backfill",
        output_dir=tmp_path / "smoothed_review_attribution",
    )
    benefit_lag = system_target.run_smoothing_benefit_lag_drilldown(
        smoothed_backfill_id=fixture["smoothed"]["smoothed_backfill_id"],
        comparison_id=fixture["comparison"]["comparison_id"],
        backfill_dir=tmp_path / "smoothed_backfill",
        comparison_dir=tmp_path / "smoothed_comparison",
        output_dir=tmp_path / "smoothing_benefit_lag",
    )
    regime = system_target.run_smoothed_regime_validation(
        smoothed_backfill_id=fixture["smoothed"]["smoothed_backfill_id"],
        smoothed_backfill_dir=tmp_path / "smoothed_backfill",
        baseline_backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "smoothed_regime_validation",
    )
    confirmation = system_target.register_smoothed_confirmation_targets(
        review_id=fixture["review"]["review_id"],
        regime_validation_id=regime["regime_validation_id"],
        review_dir=tmp_path / "smoothed_review",
        regime_validation_dir=tmp_path / "smoothed_regime_validation",
        output_dir=tmp_path / "smoothed_forward_confirmation",
    )

    result = system_target.run_smoothed_watch_pack(
        review_attribution_id=attribution["attribution_id"],
        benefit_lag_id=benefit_lag["drilldown_id"],
        regime_validation_id=regime["regime_validation_id"],
        confirmation_id=confirmation["confirmation_id"],
        attribution_dir=tmp_path / "smoothed_review_attribution",
        benefit_lag_dir=tmp_path / "smoothing_benefit_lag",
        regime_validation_dir=tmp_path / "smoothed_regime_validation",
        confirmation_dir=tmp_path / "smoothed_forward_confirmation",
        output_dir=tmp_path / "smoothed_watch_pack",
    )

    summary = result["smoothed_watch_summary"]
    assert summary["candidate_method"] == "smooth_weights_3d_limited_adjustment"
    assert summary["forward_confirmation_status"] == "IN_PROGRESS"
    assert summary["sideways_validation_status"] != "INSUFFICIENT_DATA"
    assert summary["recovery_lag_status"] != "INSUFFICIENT_DATA"
    assert summary["research_target_only"] is True
    assert summary["not_official_target_weights"] is True
    assert summary["broker_action_allowed"] is False
    assert summary["production_effect"] == "none"
    assert "Dynamic Rescue Smoothed Method Watch" in result["reader_brief_section"]

    validation = system_target.validate_smoothed_watch_pack_artifact(
        watch_pack_id=result["watch_pack_id"],
        output_dir=tmp_path / "smoothed_watch_pack",
    )
    assert validation["status"] == "PASS"

    report_index = {
        "reports": [
            {
                "report_id": "etf_dynamic_v3_smoothed_backfill",
                "latest_artifact_path": str(
                    fixture["smoothed"]["smoothed_backfill_dir"]
                    / "smoothed_backfill_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_smoothed_comparison",
                "latest_artifact_path": str(
                    fixture["comparison"]["comparison_dir"] / "smoothed_comparison_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_smoothed_review",
                "latest_artifact_path": str(
                    fixture["review"]["review_dir"] / "smoothed_review_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_smoothed_watch_pack",
                "latest_artifact_path": str(
                    result["watch_pack_dir"] / "smoothed_watch_manifest.json"
                ),
            },
        ]
    }
    reader_summary = reader_brief._etf_dynamic_v3_system_target_summary(report_index)
    assert reader_summary["smoothed_watch_pack_id"] == result["watch_pack_id"]
    assert reader_summary["smoothed_watch_forward_confirmation_status"] == "IN_PROGRESS"
    assert reader_summary["smoothed_watch_sideways_validation_status"] == summary[
        "sideways_validation_status"
    ]
    assert reader_summary["smoothed_watch_recovery_lag_status"] == summary["recovery_lag_status"]
