from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from dynamic_v3_paper_tracking_helpers import (
    paper_config_path,
    write_market_cache,
    write_shadow_shortlist_and_monitoring,
    write_validated_owner_review,
)

from ai_trading_system.etf_portfolio import dynamic_v3_outcome_accumulation as accumulation
from ai_trading_system.etf_portfolio import dynamic_v3_paper_tracking as paper_tracking
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    apply_owner_review_to_paper_portfolio,
    init_paper_portfolio,
    track_advisory_outcome,
)


def build_ready_outcome_update_fixture(tmp_path: Path, monkeypatch: Any) -> dict[str, Any]:
    monkeypatch.setattr(accumulation, "DEFAULT_LATEST_POINTER_DIR", tmp_path / "latest")
    config_path = paper_config_path(tmp_path)
    init_paper_portfolio(
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 9, tzinfo=UTC),
    )
    review = write_validated_owner_review(
        tmp_path,
        owner_decision="paper_adjustment",
        as_of=date(2026, 6, 8),
    )
    advisory = {"daily_advisory_id": review["daily_advisory_id"]}
    apply_owner_review_to_paper_portfolio(
        review_id=review["review_id"],
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
        owner_review_dir=tmp_path / "owner_review_journal",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        generated_at=datetime(2026, 6, 8, 14, tzinfo=UTC),
    )
    outcome = track_advisory_outcome(
        daily_advisory_id=advisory["daily_advisory_id"],
        config_path=config_path,
        output_dir=tmp_path / "advisory_outcome",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 15, tzinfo=UTC),
    )
    prices_path, rates_path = write_market_cache(tmp_path / "market_cache", start="2026-06-08")
    update_prices_path, update_rates_path = write_market_cache(
        tmp_path / "market_cache_update",
        start="2026-06-08",
        end="2026-06-10",
    )
    due = accumulation.run_outcome_due_scan(
        as_of=date(2026, 6, 10),
        output_dir=tmp_path / "outcome_due",
        advisory_outcome_dir=tmp_path / "advisory_outcome",
        prices_path=prices_path,
        rates_path=rates_path,
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )
    update_review = accumulation.run_outcome_update_review(
        due_id=due["due_id"],
        output_dir=tmp_path / "outcome_update_review",
        outcome_due_dir=tmp_path / "outcome_due",
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )
    return {
        "config_path": config_path,
        "advisory": advisory,
        "outcome": outcome,
        "prices_path": prices_path,
        "rates_path": rates_path,
        "update_prices_path": update_prices_path,
        "update_rates_path": update_rates_path,
        "due": due,
        "update_review": update_review,
    }


def run_safe_update_fixture(tmp_path: Path, monkeypatch: Any) -> dict[str, Any]:
    fixture = build_ready_outcome_update_fixture(tmp_path, monkeypatch)
    update = accumulation.run_outcome_update(
        update_review_id=fixture["update_review"]["update_review_id"],
        output_dir=tmp_path / "outcome_update",
        review_dir=tmp_path / "outcome_update_review",
        advisory_outcome_dir=tmp_path / "advisory_outcome",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        prices_path=fixture["update_prices_path"],
        rates_path=fixture["update_rates_path"],
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )
    return {**fixture, "update": update}


def run_rolling_refresh_fixture(tmp_path: Path, monkeypatch: Any) -> dict[str, Any]:
    fixture = run_safe_update_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(
        paper_tracking, "DEFAULT_DYNAMIC_V3_LATEST_POINTER_DIR", tmp_path / "latest"
    )
    shadow = write_shadow_shortlist_and_monitoring(tmp_path)
    refresh = accumulation.run_rolling_evidence_refresh(
        outcome_update_id=fixture["update"]["outcome_update_id"],
        output_dir=tmp_path / "rolling_evidence_refresh",
        outcome_update_dir=tmp_path / "outcome_update",
        outcome_dashboard_dir=tmp_path / "outcome_dashboard",
        limited_vs_notrade_dir=tmp_path / "limited_vs_notrade",
        consensus_risk_dir=tmp_path / "consensus_risk",
        owner_attribution_dir=tmp_path / "owner_attribution",
        shadow_aging_dir=tmp_path / "shadow_aging",
        weekly_advisory_review_dir=tmp_path / "weekly_advisory_review",
        advisory_outcome_dir=tmp_path / "advisory_outcome",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        owner_review_dir=tmp_path / "owner_review_journal",
        shadow_shortlist_dir=shadow["shadow_shortlist_dir"],
        shadow_monitor_run_dir=shadow["shadow_monitor_run_dir"],
        consensus_drift_dir=shadow["consensus_drift_dir"],
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        backfill_dir=tmp_path / "backfilled_outcome",
        repair_dir=tmp_path / "backfill_repair",
        historical_replay_dir=tmp_path / "historical_replay",
        paper_sim_dir=tmp_path / "historical_paper_sim",
        diagnosis_dir=tmp_path / "replay_diagnosis",
        outcome_due_dir=tmp_path / "outcome_due",
        config_path=fixture["config_path"],
        shadow_shortlist_id=shadow["shadow_shortlist_id"],
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )
    return {**fixture, "shadow": shadow, "refresh": refresh}
