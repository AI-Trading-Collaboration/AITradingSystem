from __future__ import annotations

from pathlib import Path

from regenerated_candidate_test_helpers import (
    build_scope_narrowed_candidate_regeneration_input_fixture,
)

from ai_trading_system.scope_narrowed_candidate_generators_regenerate import (
    run_scope_narrowed_candidate_generators_regenerate,
)


def test_confirmation_candidate_recodes_only_active_confirmation_scope(tmp_path: Path) -> None:
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

    assert payload["summary"]["validation_status"] == "PASS"
    artifact = payload["candidate_registry"]["rows"][0]
    assert artifact["scope_narrowed_candidate_id"] == (
        "baseline_plus_trend_structure_scope_narrowed_confirmation_v1"
    )
    assert artifact["usage_role"] == "confirmation_only"

    prediction = (
        payload["candidate_registry"]["rows"][0]["scope_narrowed_candidate_id"]
    )
    assert prediction == "baseline_plus_trend_structure_scope_narrowed_confirmation_v1"
