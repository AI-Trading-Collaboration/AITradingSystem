from __future__ import annotations

import json
from pathlib import Path

import pytest

from ai_trading_system.trading_engine.parameters import shadow_backtest
from ai_trading_system.trading_engine.portfolio_candidate_review import (
    decide_portfolio_candidate,
    run_portfolio_candidate_review,
)
from ai_trading_system.trading_engine.portfolio_candidate_tracking import (
    run_portfolio_candidate_tracking,
)
from trading_engine.test_portfolio_candidate_review import _review_fixture
from trading_engine.test_portfolio_candidate_tracking import (
    _write_portfolio_candidate_tracking_config,
)


def test_tracking_blocked_when_market_data_freshness_is_stale(tmp_path: Path) -> None:
    fixture, _, review_config = _review_fixture(tmp_path)
    tracking_config = _write_portfolio_candidate_tracking_config(
        tmp_path,
        fixture["config_path"],
    )
    run_portfolio_candidate_review(as_of=fixture["as_of"], config_path=review_config)
    decide_portfolio_candidate(
        decision="watch",
        as_of=fixture["as_of"],
        reason="Continue shadow tracking.",
        config_path=review_config,
    )
    _write_freshness_artifact(
        tmp_path,
        fixture["as_of"],
        status="STALE",
        can_track=False,
        recommendation="tracking_blocked",
    )

    run = run_portfolio_candidate_tracking(
        as_of=fixture["as_of"],
        config_path=tracking_config,
    )

    assert run.payload["candidate"]["tracking_status"] == "tracking_blocked"
    assert run.payload["market_data_freshness"]["status"] == "STALE"
    assert "market_data_freshness_not_ready:STALE" in run.payload["date_roll_forward"][
        "block_reasons"
    ]


def test_tracking_degraded_when_market_data_lag_is_acceptable(tmp_path: Path) -> None:
    fixture, _, review_config = _review_fixture(tmp_path)
    tracking_config = _write_portfolio_candidate_tracking_config(
        tmp_path,
        fixture["config_path"],
    )
    run_portfolio_candidate_review(as_of=fixture["as_of"], config_path=review_config)
    decide_portfolio_candidate(
        decision="watch",
        as_of=fixture["as_of"],
        reason="Continue shadow tracking.",
        config_path=review_config,
    )
    _write_freshness_artifact(
        tmp_path,
        fixture["as_of"],
        status="ACCEPTABLE_LAG",
        can_track=True,
        recommendation="degraded_tracking",
    )

    run = run_portfolio_candidate_tracking(
        as_of=fixture["as_of"],
        config_path=tracking_config,
    )

    assert run.payload["candidate"]["tracking_status"] == "degraded_tracking"
    assert run.payload["market_data_freshness"]["tracking_readiness"] == "can_track"


def test_shadow_backtest_references_market_data_freshness_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, _, review_config = _review_fixture(tmp_path)
    tracking_config = _write_portfolio_candidate_tracking_config(
        tmp_path,
        fixture["config_path"],
    )
    run_portfolio_candidate_review(as_of=fixture["as_of"], config_path=review_config)
    decide_portfolio_candidate(
        decision="watch",
        as_of=fixture["as_of"],
        reason="Continue shadow tracking.",
        config_path=review_config,
    )
    freshness_path = _write_freshness_artifact(
        tmp_path,
        fixture["as_of"],
        status="ACCEPTABLE_LAG",
        can_track=True,
        recommendation="degraded_tracking",
    )
    run_portfolio_candidate_tracking(
        as_of=fixture["as_of"],
        config_path=tracking_config,
    )
    monkeypatch.setattr(shadow_backtest, "PROJECT_ROOT", tmp_path)

    shadow_run = shadow_backtest.run_shadow_parameter_backtest(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        dry_run=True,
    )

    decision = shadow_run.payload["promotion_decision"]
    assert decision["status"] == "rejected"
    assert decision["supporting_artifacts"]["market_data_freshness"] == str(freshness_path)
    assert "Market data freshness is ACCEPTABLE_LAG" in decision["reason"]


def test_candidate_tracking_reads_market_data_refresh_report(tmp_path: Path) -> None:
    fixture, _, review_config = _review_fixture(tmp_path)
    tracking_config = _write_portfolio_candidate_tracking_config(
        tmp_path,
        fixture["config_path"],
    )
    run_portfolio_candidate_review(as_of=fixture["as_of"], config_path=review_config)
    decide_portfolio_candidate(
        decision="watch",
        as_of=fixture["as_of"],
        reason="Continue shadow tracking.",
        config_path=review_config,
    )
    _write_freshness_artifact(
        tmp_path,
        fixture["as_of"],
        status="OK",
        can_track=True,
        recommendation="active_tracking",
    )
    refresh_path = _write_refresh_artifact(tmp_path, fixture["as_of"], status="OK")

    run = run_portfolio_candidate_tracking(
        as_of=fixture["as_of"],
        config_path=tracking_config,
    )

    freshness = run.payload["market_data_freshness"]
    assert run.payload["candidate"]["tracking_status"] == "active_tracking"
    assert freshness["refresh_status"] == "OK"
    assert freshness["refresh_report"] == str(refresh_path)
    assert run.payload["supporting_artifacts"]["market_data_refresh"] == str(refresh_path)


def test_shadow_backtest_references_market_data_refresh_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, _, review_config = _review_fixture(tmp_path)
    tracking_config = _write_portfolio_candidate_tracking_config(
        tmp_path,
        fixture["config_path"],
    )
    run_portfolio_candidate_review(as_of=fixture["as_of"], config_path=review_config)
    decide_portfolio_candidate(
        decision="watch",
        as_of=fixture["as_of"],
        reason="Continue shadow tracking.",
        config_path=review_config,
    )
    _write_freshness_artifact(
        tmp_path,
        fixture["as_of"],
        status="OK",
        can_track=True,
        recommendation="active_tracking",
    )
    refresh_path = _write_refresh_artifact(tmp_path, fixture["as_of"], status="OK")
    run_portfolio_candidate_tracking(
        as_of=fixture["as_of"],
        config_path=tracking_config,
    )
    monkeypatch.setattr(shadow_backtest, "PROJECT_ROOT", tmp_path)

    shadow_run = shadow_backtest.run_shadow_parameter_backtest(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        dry_run=True,
    )

    decision = shadow_run.payload["promotion_decision"]
    assert decision["status"] == "rejected"
    assert decision["supporting_artifacts"]["market_data_refresh"] == str(refresh_path)
    assert "Market data refresh is OK" in decision["reason"]


def _write_freshness_artifact(
    tmp_path: Path,
    as_of,
    *,
    status: str,
    can_track: bool,
    recommendation: str,
) -> Path:
    path = (
        tmp_path
        / "artifacts"
        / "data_freshness"
        / as_of.isoformat()
        / "market_data_freshness_summary.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "report_type": "market_data_freshness",
        "metadata": {
            "run_id": f"market-data-freshness-{as_of.isoformat()}",
            "generated_at": "2026-01-20T00:00:00+00:00",
            "status": status,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
        },
        "data_dates": {
            "tracking_date": as_of.isoformat(),
            "effective_data_date": as_of.isoformat(),
            "latest_registry_date": as_of.isoformat(),
            "latest_manifest_date": as_of.isoformat(),
        },
        "freshness": {
            "status": status,
            "lag_trading_days": 0,
            "lag_calendar_days": 0,
            "reason": f"Market data freshness is {status}.",
        },
        "tracking_readiness": {
            "can_track": can_track,
            "readiness": "can_track" if can_track else "cannot_track",
            "tracking_status_recommendation": recommendation,
            "reason": "unit test freshness readiness",
        },
        "asset_coverage": {"status": "OK", "assets": {}},
        "safety": {
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "production_write_allowed": False,
            "data_downloaded_by_freshness_check": False,
            "fake_price_rows_generated": False,
            "data_quality_gate_lowered": False,
        },
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path = path.with_suffix(".md")
    markdown_path.write_text("# Market Data Freshness Summary\n", encoding="utf-8")
    return path


def _write_refresh_artifact(tmp_path: Path, as_of, *, status: str) -> Path:
    path = (
        tmp_path
        / "artifacts"
        / "data_refresh"
        / as_of.isoformat()
        / "market_data_refresh_summary.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "report_type": "market_data_refresh",
        "metadata": {
            "run_id": f"market-data-refresh-{as_of.isoformat()}",
            "generated_at": "2026-01-20T00:00:00+00:00",
            "status": status,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
        },
        "before": {"freshness_status": "STALE"},
        "actions": {
            "target_date": as_of.isoformat(),
            "fetched_assets": ["GOOGL"],
            "updated_price_cache_registry": True,
            "refreshed_backtest_manifest": True,
        },
        "after": {
            "freshness_status": "OK",
            "effective_data_date": as_of.isoformat(),
            "tracking_readiness": "can_track",
            "candidate_tracking_status": "active_tracking",
        },
        "safety": {
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "production_write_allowed": False,
            "fake_price_rows_generated": False,
            "synthetic_latest_bar_generated": False,
            "data_quality_gate_lowered": False,
        },
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path = path.with_suffix(".md")
    markdown_path.write_text("# Market Data Refresh Summary\n", encoding="utf-8")
    return path
