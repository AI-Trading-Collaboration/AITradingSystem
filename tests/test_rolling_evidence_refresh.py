from __future__ import annotations

from pathlib import Path
from typing import Any

from dynamic_v3_outcome_loop_helpers import run_rolling_refresh_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_outcome_accumulation as accumulation


def test_rolling_evidence_refresh_records_downstream_ids_and_delta(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    result = run_rolling_refresh_fixture(tmp_path, monkeypatch)["refresh"]
    refreshed = result["refreshed_artifacts"]
    delta = result["evidence_delta_summary"]

    assert refreshed["outcome_dashboard_id"]
    assert refreshed["limited_vs_notrade_id"]
    assert refreshed["consensus_risk_id"]
    assert refreshed["owner_attribution_id"]
    assert refreshed["shadow_aging_id"]
    assert refreshed["weekly_advisory_review_id"]
    assert delta["before"]["forward_available"] == 0
    assert delta["after"]["forward_available"] == 1
    assert delta["after"]["limited_vs_notrade_available_count"] == 1
    assert delta["after"]["limited_vs_notrade_confidence"] == "LOW"
    assert delta["after"]["consensus_target_risk"] == "INSUFFICIENT_DATA"
    assert delta["material_change"] is True
    assert (
        accumulation.validate_rolling_evidence_refresh_artifact(
            refresh_id=result["refresh_id"],
            output_dir=tmp_path / "rolling_evidence_refresh",
        )["status"]
        == "PASS"
    )
