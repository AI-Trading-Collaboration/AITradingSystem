from __future__ import annotations

from dynamic_v3_system_target_helpers import build_model_target_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_smoothed_generation_preserves_weight_sum_and_safety(tmp_path) -> None:
    fixture = build_model_target_fixture(tmp_path)

    result = system_target.generate_smoothed_limited_target(
        target_id=fixture["target_id"],
        model_target_dir=tmp_path / "model_target",
        output_dir=tmp_path / "smoothed_limited",
        regime_context="strong_recovery",
    )

    rows = result["smoothed_target_weights"]
    methods = {row["target_method"] for row in rows}
    assert "smooth_weights_3d_limited_adjustment" in methods
    assert "smooth_weights_5d_limited_adjustment" in methods
    for row in rows:
        assert abs(sum(row["smoothed_weights"].values()) - 1.0) <= 0.000001
        assert row["research_target_only"] is True
        assert row["not_official_target_weights"] is True
        assert row["broker_action_allowed"] is False
    assert result["smoothing_events"]
    assert result["lag_events"]

    validation = system_target.validate_smoothed_limited_artifact(
        smoothed_id=result["smoothed_id"],
        output_dir=tmp_path / "smoothed_limited",
    )
    assert validation["status"] == "PASS"


def test_smoothed_sideways_context_uses_stronger_smoothing(tmp_path) -> None:
    fixture = build_model_target_fixture(tmp_path)

    result = system_target.generate_smoothed_limited_target(
        target_id=fixture["target_id"],
        model_target_dir=tmp_path / "model_target",
        output_dir=tmp_path / "smoothed_limited",
        regime_context="sideways_choppy",
    )

    by_method = {row["target_method"]: row for row in result["smoothed_target_weights"]}
    assert by_method["smooth_weights_3d_limited_adjustment"]["alpha"] == 0.375
    assert by_method["smooth_weights_5d_limited_adjustment"]["alpha"] == 0.2625
