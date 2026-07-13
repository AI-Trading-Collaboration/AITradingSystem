from __future__ import annotations

from datetime import UTC, datetime

import pytest
from dynamic_v3_system_target_helpers import run_selection_review_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_limited_risk_attribution_outputs_return_drawdown_and_exposure(tmp_path) -> None:
    fixture = run_selection_review_fixture(tmp_path)
    attribution = system_target.run_limited_risk_attribution(
        backfill_id=fixture["backfill"]["backfill_id"],
        backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "limited_risk_attribution",
        generated_at=datetime(2026, 1, 7, 11, tzinfo=UTC),
    )

    returns = attribution["return_contribution_by_symbol"]
    drawdown = attribution["drawdown_contribution_by_symbol"]
    exposure = attribution["exposure_shift_attribution"]
    events = attribution["risk_worsening_events"]

    assert returns["target_method"] == "limited_adjustment"
    assert {row["symbol"] for row in returns["symbols"]} >= {"QQQ", "SMH", "SOXX"}
    assert returns["top_positive_contributors"]
    assert drawdown["target_method"] == "limited_adjustment"
    assert drawdown["top_drawdown_contributors"]
    assert exposure["risk_worsening_source"] in {
        "higher_risk_asset_exposure",
        "higher_semiconductor_exposure",
        "lower_cash",
        "timing_error",
        "mixed",
        "unknown",
    }
    assert exposure["broker_action_allowed"] is False
    assert events
    assert events[0]["risk_worsening_type"] in {
        "drawdown_deeper",
        "volatility_higher",
        "turnover_higher",
        "mixed",
    }

    validation = system_target.validate_limited_risk_attribution_artifact(
        risk_attribution_id=attribution["risk_attribution_id"],
        output_dir=tmp_path / "limited_risk_attribution",
    )
    assert validation["status"] == "PASS"

    assert attribution["manifest"]["input_snapshot_schema"] == (
        "limited_risk_attribution_input_snapshot.v2"
    )
    assert attribution["manifest"]["bounded_price_rows_used"] is True
    assert attribution["manifest"]["cache_commitments_visible"] is True

    with pytest.raises(ValueError, match="chronology"):
        system_target.run_limited_risk_attribution(
            backfill_id=fixture["backfill"]["backfill_id"],
            backfill_dir=tmp_path / "paper_shadow_backfill",
            output_dir=tmp_path / "limited_risk_attribution_past",
            generated_at=datetime(2024, 3, 1, 11, tzinfo=UTC),
        )
