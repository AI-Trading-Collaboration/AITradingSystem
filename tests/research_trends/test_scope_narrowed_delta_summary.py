from __future__ import annotations

import json
from pathlib import Path

from regenerated_candidate_test_helpers import (
    build_scope_narrowed_candidate_regeneration_input_fixture,
)

from ai_trading_system.scope_narrowed_candidate_generators_regenerate import (
    run_scope_narrowed_candidate_generators_regenerate,
)


def test_scope_delta_summary_is_distribution_only(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_candidate_regeneration_input_fixture(tmp_path)
    output_dir = tmp_path / "out"
    run_scope_narrowed_candidate_generators_regenerate(
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
        output_dir=output_dir,
        mode="scope_narrowed_regeneration",
        docs_root=tmp_path / "docs",
    )
    path = output_dir / "scope_narrowed_original_vs_refined_vs_scope_delta_summary.json"
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["forbidden_outcome_comparison_executed"] is False
    assert payload["candidate_rows"][0]["scope_narrowed_distribution"]["record_count"] == 4
    assert "actual_path_improvement" not in json.dumps(payload)
