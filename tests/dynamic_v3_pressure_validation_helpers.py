from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    run_defensive_pressure_compare,
    run_defensive_rule_review,
    run_pressure_outcome_backfill,
)

GENERATED_AT = datetime(2026, 6, 30, tzinfo=UTC)


def write_pressure_tag_fixture(tmp_path: Path) -> dict[str, Any]:
    pressure_tag_dir = tmp_path / "pressure_regime_tag"
    tag_id = "pressure-tag-fixture"
    tag_dir = pressure_tag_dir / tag_id
    tag_dir.mkdir(parents=True)
    config_path = tmp_path / "pressure_tag_config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "schema_version: 1",
                "thresholds:",
                "  tech_drawdown_pct: -0.05",
                "  semiconductor_pullback_pct: -0.06",
                "  risk_off_volatility_percentile: 0.75",
                "",
            ]
        ),
        encoding="utf-8",
    )
    _write_json(
        tag_dir / "pressure_regime_manifest.json",
        {
            "schema_version": 1,
            "tag_id": tag_id,
            "config_path": str(config_path),
            "status": "PASS",
            "production_effect": "none",
            "broker_action_allowed": False,
        },
    )
    _write_jsonl(
        tag_dir / "regime_window_tags.jsonl",
        [
            {
                "date": "2026-06-03",
                "window_days": 20,
                "regime_tags": [],
                "metrics": {
                    "qqq_drawdown": -0.048,
                    "smh_drawdown": -0.058,
                    "realized_volatility": 0.11,
                },
            },
            {
                "date": "2026-06-10",
                "window_days": 20,
                "regime_tags": ["tech_drawdown"],
                "metrics": {
                    "qqq_drawdown": -0.071,
                    "smh_drawdown": -0.041,
                    "realized_volatility": 0.18,
                },
            },
            {
                "date": "2026-06-17",
                "window_days": 20,
                "regime_tags": ["semiconductor_pullback"],
                "metrics": {
                    "qqq_drawdown": -0.022,
                    "smh_drawdown": -0.083,
                    "realized_volatility": 0.13,
                },
            },
        ],
    )
    _write_jsonl(
        tag_dir / "outcome_regime_tags.jsonl",
        [
            {
                "outcome_id": "forward-outcome-1",
                "daily_advisory_id": "daily-1",
                "as_of": "2026-06-03",
                "window_days": 5,
                "regime_tags": [],
            }
        ],
    )
    _write_json(
        tag_dir / "pressure_regime_summary.json",
        {
            "schema_version": 1,
            "pressure_window_count": 2,
            "pressure_tagged_outcomes": 0,
            "defensive_validation_relevant_outcomes": 0,
            "pressure_samples": {
                "tech_drawdown": 1,
                "risk_off": 0,
                "semiconductor_pullback": 1,
                "sideways_choppy": 0,
                "strong_recovery": 0,
                "ai_trend": 0,
            },
            "production_effect": "none",
            "broker_action_allowed": False,
        },
    )
    return {
        "pressure_tag_dir": pressure_tag_dir,
        "pressure_tag_id": tag_id,
        "pressure_tag_artifact_dir": tag_dir,
    }


def write_backtest_sim_outcome_fixture(tmp_path: Path) -> dict[str, Any]:
    outcome_dir = tmp_path / "backtest_sim_outcome"
    sim_outcome_id = "sim-outcome-fixture"
    artifact_dir = outcome_dir / sim_outcome_id
    artifact_dir.mkdir(parents=True)
    _write_json(
        artifact_dir / "sim_outcome_manifest.json",
        {
            "schema_version": 1,
            "sim_outcome_id": sim_outcome_id,
            "status": "PASS",
            "outcome_mode": "BACKTEST_SIMULATION",
            "pit_safety_status": "SIMULATION_NOT_PIT",
            "production_effect": "none",
            "broker_action_allowed": False,
        },
    )
    rows = []
    rows.extend(
        _sim_window_rows(
            sim_event_id="sim-event-tech",
            as_of="2026-06-10",
            window_days=5,
            regime_label="tech_drawdown",
            no_trade_return=-0.020,
            no_trade_drawdown=-0.080,
            defensive_return=-0.005,
            defensive_drawdown=-0.040,
        )
    )
    rows.extend(
        _sim_window_rows(
            sim_event_id="sim-event-semi",
            as_of="2026-06-17",
            window_days=10,
            regime_label="semiconductor_pullback",
            no_trade_return=-0.030,
            no_trade_drawdown=-0.090,
            defensive_return=-0.010,
            defensive_drawdown=-0.050,
        )
    )
    _write_jsonl(artifact_dir / "simulated_outcome_windows.jsonl", rows)
    return {
        "backtest_sim_outcome_dir": outcome_dir,
        "sim_outcome_id": sim_outcome_id,
        "sim_outcome_artifact_dir": artifact_dir,
    }


def run_pressure_backfill_fixture(tmp_path: Path) -> dict[str, Any]:
    pressure = write_pressure_tag_fixture(tmp_path)
    sim = write_backtest_sim_outcome_fixture(tmp_path)
    backfill_dir = tmp_path / "pressure_outcome_backfill"
    backfill = run_pressure_outcome_backfill(
        start=date(2026, 6, 1),
        end=date(2026, 6, 30),
        output_dir=backfill_dir,
        pressure_tag_dir=pressure["pressure_tag_dir"],
        advisory_outcome_dir=tmp_path / "advisory_outcome",
        backfilled_outcome_dir=tmp_path / "backfilled_outcome",
        backtest_sim_outcome_dir=sim["backtest_sim_outcome_dir"],
        generated_at=GENERATED_AT,
    )
    return {
        **pressure,
        **sim,
        "pressure_backfill_dir": backfill_dir,
        "pressure_backfill": backfill,
    }


def run_defensive_pressure_compare_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_pressure_backfill_fixture(tmp_path)
    compare_dir = tmp_path / "defensive_pressure_compare"
    comparison = run_defensive_pressure_compare(
        pressure_backfill_id=fixture["pressure_backfill"]["pressure_backfill_id"],
        backfill_dir=fixture["pressure_backfill_dir"],
        output_dir=compare_dir,
        generated_at=GENERATED_AT,
    )
    return {
        **fixture,
        "defensive_pressure_compare_dir": compare_dir,
        "defensive_pressure_compare": comparison,
    }


def run_defensive_rule_review_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_defensive_pressure_compare_fixture(tmp_path)
    review_dir = tmp_path / "defensive_rule_review"
    review = run_defensive_rule_review(
        comparison_id=fixture["defensive_pressure_compare"]["comparison_id"],
        comparison_dir=fixture["defensive_pressure_compare_dir"],
        output_dir=review_dir,
        generated_at=GENERATED_AT,
    )
    return {
        **fixture,
        "defensive_rule_review_dir": review_dir,
        "defensive_rule_review": review,
    }


def write_weekly_cycle_fixture(tmp_path: Path) -> dict[str, Any]:
    weekly_cycle_dir = tmp_path / "confirmation_cycle_weekly"
    weekly_cycle_id = "weekly-cycle-fixture"
    artifact_dir = weekly_cycle_dir / weekly_cycle_id
    artifact_dir.mkdir(parents=True)
    _write_json(
        artifact_dir / "weekly_cycle_summary.json",
        {
            "schema_version": 1,
            "weekly_cycle_id": weekly_cycle_id,
            "status": "PASS",
            "as_of": "2026-06-30",
            "production_effect": "none",
            "broker_action_allowed": False,
        },
    )
    return {"weekly_cycle_dir": weekly_cycle_dir, "weekly_cycle_id": weekly_cycle_id}


def _sim_window_rows(
    *,
    sim_event_id: str,
    as_of: str,
    window_days: int,
    regime_label: str,
    no_trade_return: float,
    no_trade_drawdown: float,
    defensive_return: float,
    defensive_drawdown: float,
) -> list[dict[str, Any]]:
    return [
        _sim_row(
            sim_event_id=sim_event_id,
            as_of=as_of,
            window_days=window_days,
            regime_label=regime_label,
            variant="no_trade",
            return_value=no_trade_return,
            max_drawdown=no_trade_drawdown,
            relative_to_no_trade=0.0,
            turnover=0.0,
        ),
        _sim_row(
            sim_event_id=sim_event_id,
            as_of=as_of,
            window_days=window_days,
            regime_label=regime_label,
            variant="defensive_limited_adjustment",
            return_value=defensive_return,
            max_drawdown=defensive_drawdown,
            relative_to_no_trade=defensive_return - no_trade_return,
            turnover=0.02,
        ),
        _sim_row(
            sim_event_id=sim_event_id,
            as_of=as_of,
            window_days=window_days,
            regime_label=regime_label,
            variant="limited_adjustment",
            return_value=no_trade_return + 0.004,
            max_drawdown=no_trade_drawdown + 0.015,
            relative_to_no_trade=0.004,
            turnover=0.015,
        ),
        _sim_row(
            sim_event_id=sim_event_id,
            as_of=as_of,
            window_days=window_days,
            regime_label=regime_label,
            variant="consensus_target",
            return_value=no_trade_return + 0.002,
            max_drawdown=no_trade_drawdown + 0.010,
            relative_to_no_trade=0.002,
            turnover=0.012,
        ),
    ]


def _sim_row(
    *,
    sim_event_id: str,
    as_of: str,
    window_days: int,
    regime_label: str,
    variant: str,
    return_value: float,
    max_drawdown: float,
    relative_to_no_trade: float,
    turnover: float,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "sim_event_id": sim_event_id,
        "as_of": as_of,
        "window_days": window_days,
        "regime_label": regime_label,
        "variant": variant,
        "return": return_value,
        "max_drawdown": max_drawdown,
        "relative_to_no_trade": relative_to_no_trade,
        "turnover": turnover,
        "outcome_status": "AVAILABLE",
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
