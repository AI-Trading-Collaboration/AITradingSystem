from __future__ import annotations

import json
from pathlib import Path

from regenerated_candidate_test_helpers import (
    build_scope_narrowed_candidate_regeneration_input_fixture,
)

from ai_trading_system.scope_narrowed_candidate_generators_regenerate import (
    run_scope_narrowed_candidate_generators_regenerate,
)


def test_risk_cap_candidate_keeps_only_allowed_risk_cap_records(tmp_path: Path) -> None:
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
    path = (
        output_dir
        / "volatility_regime_scope_narrowed_risk_cap_v1"
        / "scope_narrowed_candidate_prediction_artifact.json"
    )
    artifact = json.loads(path.read_text(encoding="utf-8"))

    assert artifact["usage_role"] == "risk_cap_only"
    active_directions = {row["signal_direction"] for row in artifact["active_prediction_records"]}
    assert active_directions <= {"risk_off", "volatility_expansion"}
    inactive_directions = {
        row["signal_direction"]
        for row in artifact["inactive_prediction_records"]
        if row["signal_direction"] in {"risk_on", "volatility_compression"}
    }
    assert inactive_directions == {"risk_on", "volatility_compression"}
    assert all("risk_cap_score" in row for row in artifact["prediction_records"])
    assert "sell_signal" not in artifact["prediction_records"][0]
    assert "buy_signal" not in artifact["prediction_records"][0]
    assert "target_weight" not in artifact["prediction_records"][0]
    assert artifact["broker_action"] == "none"
