from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dynamic_v3_paper_tracking_helpers import paper_config_path, write_daily_advisory

from ai_trading_system.etf_portfolio import dynamic_v3_outcome_accumulation as accumulation
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    init_paper_portfolio,
    track_advisory_outcome,
)


def test_outcome_dashboard_aggregates_modes_pending_reasons_and_reader_brief(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(accumulation, "DEFAULT_LATEST_POINTER_DIR", tmp_path / "latest")
    config_path = paper_config_path(tmp_path)
    init_paper_portfolio(config_path=config_path, output_dir=tmp_path / "paper_portfolio")
    advisory = write_daily_advisory(tmp_path, as_of="2026-06-08")
    track_advisory_outcome(
        daily_advisory_id=advisory["daily_advisory_id"],
        config_path=config_path,
        output_dir=tmp_path / "advisory_outcome",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
    )
    repair_dir = tmp_path / "backfill_repair" / "repair-1"
    repair_dir.mkdir(parents=True, exist_ok=True)
    (repair_dir / "backfill_repair_manifest.json").write_text(
        json.dumps({"repair_id": "repair-1", "broker_action_allowed": False}, sort_keys=True),
        encoding="utf-8",
    )
    (repair_dir / "repaired_outcome_windows.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "replay_event_id": "event-1",
                        "variant": "limited_adjustment",
                        "window_days": 1,
                        "outcome_status": "AVAILABLE",
                        "return": 0.02,
                    },
                    sort_keys=True,
                ),
                json.dumps(
                    {
                        "replay_event_id": "event-1",
                        "variant": "no_trade",
                        "window_days": 5,
                        "outcome_status": "PENDING",
                    },
                    sort_keys=True,
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    sim_dir = tmp_path / "historical_paper_sim" / "sim-1"
    sim_dir.mkdir(parents=True, exist_ok=True)
    (sim_dir / "historical_paper_sim_manifest.json").write_text(
        json.dumps({"sim_id": "sim-1", "status": "PASS"}, sort_keys=True),
        encoding="utf-8",
    )
    (sim_dir / "simulated_performance_summary.json").write_text(
        json.dumps({"simulation_status": "PASS"}, sort_keys=True),
        encoding="utf-8",
    )

    result = accumulation.build_outcome_dashboard(
        output_dir=tmp_path / "outcome_dashboard",
        advisory_outcome_dir=tmp_path / "advisory_outcome",
        repair_dir=tmp_path / "backfill_repair",
        paper_sim_dir=tmp_path / "historical_paper_sim",
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )

    matrix = result["outcome_availability_matrix"]["summary"]
    assert matrix["forward_outcome"]["pending"] == 4
    assert matrix["historical_replay"]["available"] == 1
    assert matrix["backtest_simulation"]["available"] == 1
    assert result["reader_brief"]["available_count"] == 2
    assert result["pending_reason_dashboard"]["top_pending_reasons"][0]["reason"] == (
        "future_window_not_reached"
    )
    assert (
        accumulation.validate_outcome_dashboard_artifact(
            dashboard_id=result["dashboard_id"],
            output_dir=tmp_path / "outcome_dashboard",
        )["status"]
        == "PASS"
    )

