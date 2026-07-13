from __future__ import annotations

from datetime import UTC, datetime

from dynamic_v3_system_target_helpers import run_selection_review_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_limited_instability_diagnosis_lists_unstable_windows(tmp_path) -> None:
    fixture = run_selection_review_fixture(tmp_path)
    backfill_id = fixture["backfill"]["backfill_id"]
    consistency = system_target.run_limited_consistency_check(
        backfill_id=backfill_id,
        backfill_dir=tmp_path / "paper_shadow_backfill",
        rolling_eval_dir=tmp_path / "paper_shadow_rolling_eval",
        regime_review_dir=tmp_path / "paper_shadow_regime_review",
        stability_dir=tmp_path / "paper_shadow_stability",
        output_dir=tmp_path / "limited_consistency",
        generated_at=datetime(2026, 1, 7, 7, tzinfo=UTC),
    )

    diagnosis = system_target.run_limited_instability_diagnosis(
        backfill_id=backfill_id,
        consistency_id=consistency["consistency_id"],
        backfill_dir=tmp_path / "paper_shadow_backfill",
        consistency_dir=tmp_path / "limited_consistency",
        rolling_eval_dir=tmp_path / "paper_shadow_rolling_eval",
        output_dir=tmp_path / "limited_instability",
        generated_at=datetime(2026, 1, 7, 10, tzinfo=UTC),
    )

    summary = diagnosis["instability_reason_summary"]
    inventory = diagnosis["unstable_window_inventory"]

    assert diagnosis["manifest"]["status"] == "PASS"
    assert summary["target_method"] == "limited_adjustment"
    assert summary["unstable_window_count"] == len(inventory)
    assert summary["unstable_window_count"] > 0
    assert summary["recommendation"] in {
        "continue_diagnosis",
        "consider_regime_gate",
        "consider_risk_cap",
        "insufficient_data",
    }
    assert inventory[0]["failure_type"] in {
        "return_underperformance",
        "drawdown_worse",
        "risk_adjusted_worse",
        "turnover_high",
        "mixed",
    }
    assert inventory[0]["severity"] in {"LOW", "MEDIUM", "HIGH"}
    assert diagnosis["rolling_failure_pattern"]["patterns"]
    assert diagnosis["manifest"]["broker_action_allowed"] is False

    validation = system_target.validate_limited_instability_artifact(
        instability_id=diagnosis["instability_id"],
        output_dir=tmp_path / "limited_instability",
    )
    assert validation["status"] == "PASS"
