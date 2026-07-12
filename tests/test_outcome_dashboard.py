from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from dynamic_v3_paper_tracking_helpers import (
    paper_config_path,
    write_validated_daily_advisory,
)

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
    advisory = write_validated_daily_advisory(tmp_path, as_of=date(2026, 6, 8))
    track_advisory_outcome(
        daily_advisory_id=advisory["daily_advisory_id"],
        config_path=config_path,
        output_dir=tmp_path / "advisory_outcome",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 15, tzinfo=UTC),
    )
    result = accumulation.build_outcome_dashboard(
        output_dir=tmp_path / "outcome_dashboard",
        advisory_outcome_dir=tmp_path / "advisory_outcome",
        backfill_dir=tmp_path / "backfilled_outcome",
        repair_dir=tmp_path / "backfill_repair",
        paper_sim_dir=tmp_path / "historical_paper_sim",
        diagnosis_dir=tmp_path / "replay_diagnosis",
        outcome_due_dir=tmp_path / "outcome_due",
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )

    matrix = result["outcome_availability_matrix"]["summary"]
    assert matrix["forward_outcome"]["pending"] == 4
    assert matrix["historical_replay"]["available"] == 0
    assert matrix["backtest_simulation"]["available"] == 0
    assert result["reader_brief"]["available_count"] == 0
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

    dashboard_dir = Path(result["dashboard_dir"])
    matrix_path = dashboard_dir / "outcome_availability_matrix.json"
    original_matrix = matrix_path.read_text(encoding="utf-8")
    matrix_path.write_text(
        original_matrix.replace('"pending": 4', '"pending": 3'), encoding="utf-8"
    )
    assert (
        accumulation.validate_outcome_dashboard_artifact(
            dashboard_id=result["dashboard_id"],
            output_dir=tmp_path / "outcome_dashboard",
        )["status"]
        == "FAIL"
    )
    matrix_path.write_text(original_matrix, encoding="utf-8")

    outcome_manifest = (
        tmp_path
        / "advisory_outcome"
        / next((tmp_path / "advisory_outcome").iterdir()).name
        / "advisory_outcome_manifest.json"
    )
    original_source = outcome_manifest.read_text(encoding="utf-8")
    outcome_manifest.write_text(original_source + " ", encoding="utf-8")
    assert (
        accumulation.validate_outcome_dashboard_artifact(
            dashboard_id=result["dashboard_id"],
            output_dir=tmp_path / "outcome_dashboard",
        )["status"]
        == "FAIL"
    )
    outcome_manifest.write_text(original_source, encoding="utf-8")
    assert (
        accumulation.validate_outcome_dashboard_artifact(
            dashboard_id=result["dashboard_id"],
            output_dir=tmp_path / "outcome_dashboard",
        )["status"]
        == "PASS"
    )
