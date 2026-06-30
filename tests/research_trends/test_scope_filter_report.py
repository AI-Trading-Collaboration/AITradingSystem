from __future__ import annotations

from pathlib import Path

from regenerated_candidate_test_helpers import (
    build_scope_narrowed_candidate_regeneration_input_fixture,
)

from ai_trading_system.scope_narrowed_candidate_generators_regenerate import (
    run_scope_narrowed_candidate_generators_regenerate,
)


def test_scope_filter_report_counts_and_reasons(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_candidate_regeneration_input_fixture(tmp_path)
    payload = run_scope_narrowed_candidate_generators_regenerate(
        scope_review_dir=fixture["scope_review_dir"],
        refined_generator_dir=fixture["refined_generator_dir"],
        refined_validation_dir=fixture["refined_validation_dir"],
        include_candidates=(
            "baseline_plus_trend_structure_refined_confidence_v1,"
            "volatility_regime_refined_confidence_v1"
        ),
        archive_candidates="risk_appetite_refined_confidence_v1",
        target_assets="QQQ,SPY,SMH",
        horizons="5d,10d,20d",
        output_dir=tmp_path / "out",
        mode="scope_narrowed_regeneration",
        docs_root=tmp_path / "docs",
    )

    rows = {
        row["scope_narrowed_candidate_id"]: row
        for row in payload["candidate_registry"]["rows"]
    }
    baseline = rows["baseline_plus_trend_structure_scope_narrowed_confirmation_v1"]
    volatility = rows["volatility_regime_scope_narrowed_risk_cap_v1"]
    assert baseline["active_record_count"] == 2
    assert baseline["inactive_record_count"] == 2
    assert volatility["active_record_count"] == 2
    assert volatility["inactive_record_count"] == 2
