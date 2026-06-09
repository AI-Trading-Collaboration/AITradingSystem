from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ai_trading_system.config import configured_price_tickers, configured_rate_series, load_universe
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    SCHEMA_VERSION,
)


def paper_snapshot_path(tmp_path: Path) -> Path:
    path = tmp_path / "current_portfolio_snapshot.example.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "schema_version": 1,
                "as_of": "2026-06-07",
                "base_currency": "USD",
                "account_type": "manual_snapshot",
                "source": "manual",
                "total_equity": 100000.0,
                "cash": {"symbol": "CASH", "weight": 0.20, "value": 20000.0},
                "positions": [
                    {"symbol": "QQQ", "weight": 0.50, "value": 50000.0, "currency": "USD"},
                    {"symbol": "SMH", "weight": 0.20, "value": 20000.0, "currency": "USD"},
                    {"symbol": "TLT", "weight": 0.10, "value": 10000.0, "currency": "USD"},
                ],
                "metadata": {"owner_reviewed": True, "broker_imported": False},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def paper_config_path(tmp_path: Path, *, snapshot_path: Path | None = None) -> Path:
    path = tmp_path / "paper_portfolio_v1.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "schema_version": 1,
                "policy_metadata": {
                    "policy_id": "paper_portfolio_v1_test",
                    "owner": "tests",
                    "version": "v1",
                    "status": "test",
                    "rationale": "Focused tests for paper-only advisory simulation.",
                    "intended_effect": "No broker action or production mutation.",
                    "review_condition": "Test fixture only.",
                },
                "paper_portfolio": {
                    "enabled": True,
                    "mode": "advisory_simulation_only",
                    "base_currency": "USD",
                    "initial_source": "manual_snapshot",
                    "initial_snapshot_path": str(snapshot_path or paper_snapshot_path(tmp_path)),
                },
                "safety": {
                    "broker_action_allowed": False,
                    "broker_action_taken": False,
                    "require_owner_review": True,
                    "allow_auto_apply_advisory": False,
                },
                "simulation": {
                    "price_source": "existing_price_cache",
                    "use_adjusted_close": True,
                    "transaction_cost_bps": 0,
                    "slippage_bps": 0,
                    "min_trade_threshold": 0.01,
                    "max_single_day_total_adjustment": 0.10,
                    "max_single_symbol_adjustment": 0.05,
                },
                "outcome_tracking": {"windows_trading_days": [1, 5, 10, 20]},
                "promotion_clock_v2": {
                    "min_days_observed": 30,
                    "min_rebalance_count": 3,
                    "max_drift_warning_count": 1,
                    "max_high_disagreement_count": 1,
                    "max_downgrade_warning_count": 0,
                    "min_outcome_score": -0.02,
                    "downgrade_outcome_score": -0.05,
                },
                "ledger": {"immutable_events": True, "allow_rebuild_from_events": True},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def write_daily_advisory(
    tmp_path: Path,
    *,
    daily_advisory_id: str = "daily-1",
    recommended_action: str = "manual_review",
    as_of: str = "2026-06-07",
) -> dict[str, Any]:
    advisory_dir = tmp_path / "position_advisory_daily" / daily_advisory_id
    advisory_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_position_advisory_daily_manifest",
        "daily_advisory_id": daily_advisory_id,
        "shadow_monitor_run_id": "monitor-1",
        "shadow_shortlist_id": "shadow-shortlist-1",
        "as_of": as_of,
        "status": "PASS",
        "mode": "SNAPSHOT_DELTA",
        "recommended_action": recommended_action,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
    }
    actions = {
        "schema_version": SCHEMA_VERSION,
        "mode": "SNAPSHOT_DELTA",
        "recommended_action": recommended_action,
        "owner_approval_required": True,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
    }
    consensus_rows = [
        {"symbol": "QQQ", "median_target_weight": 0.45, "mean_target_weight": 0.45},
        {"symbol": "SMH", "median_target_weight": 0.25, "mean_target_weight": 0.25},
        {"symbol": "SOXX", "median_target_weight": 0.10, "mean_target_weight": 0.10},
        {"symbol": "TLT", "median_target_weight": 0.05, "mean_target_weight": 0.05},
        {"symbol": "CASH", "median_target_weight": 0.15, "mean_target_weight": 0.15},
    ]
    delta_row = {
        "daily_advisory_id": daily_advisory_id,
        "candidate_id": "candidate-a",
        "current_weights": {"QQQ": 0.50, "SMH": 0.20, "TLT": 0.10, "CASH": 0.20},
        "target_weights": {
            "QQQ": 0.45,
            "SMH": 0.25,
            "SOXX": 0.10,
            "TLT": 0.05,
            "CASH": 0.15,
        },
        "deltas": {"QQQ": -0.05, "SMH": 0.05, "SOXX": 0.10, "TLT": -0.05, "CASH": -0.05},
    }
    (advisory_dir / "daily_advisory_manifest.json").write_text(
        json.dumps(manifest, sort_keys=True),
        encoding="utf-8",
    )
    (advisory_dir / "daily_advisory_actions.json").write_text(
        json.dumps(actions, sort_keys=True),
        encoding="utf-8",
    )
    pd.DataFrame(consensus_rows).to_csv(advisory_dir / "daily_consensus_weights.csv", index=False)
    (advisory_dir / "daily_position_deltas.jsonl").write_text(
        json.dumps(delta_row, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return {"daily_advisory_id": daily_advisory_id, "advisory_dir": advisory_dir}


def write_owner_review(
    tmp_path: Path,
    *,
    review_id: str = "review-1",
    daily_advisory_id: str = "daily-1",
    owner_decision: str = "monitor",
    recommended_action: str = "manual_review",
    as_of: str = "2026-06-07",
) -> dict[str, Any]:
    review_dir = tmp_path / "owner_review_journal"
    review_dir.mkdir(parents=True, exist_ok=True)
    record = {
        "schema_version": SCHEMA_VERSION,
        "review_id": review_id,
        "daily_advisory_id": daily_advisory_id,
        "as_of": as_of,
        "recommended_action": recommended_action,
        "owner_decision": owner_decision,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "paper_action": {"enabled": owner_decision == "paper_adjustment", "notes": "paper only"},
        "manual_notes": "",
        "created_at": datetime(2026, 6, 7, tzinfo=UTC).isoformat(),
        "updated_at": datetime(2026, 6, 7, tzinfo=UTC).isoformat(),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
    }
    (review_dir / "owner_review_journal.jsonl").write_text(
        json.dumps(record, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (review_dir / "latest_owner_review.json").write_text(
        json.dumps(record, sort_keys=True),
        encoding="utf-8",
    )
    return {"review_id": review_id, "review_dir": review_dir, "record": record}


def write_market_cache(
    tmp_path: Path,
    *,
    start: str = "2026-06-05",
    end: str = "2026-07-10",
) -> tuple[Path, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    universe = load_universe()
    tickers = configured_price_tickers(universe)
    series = configured_rate_series(universe)
    dates = pd.bdate_range(start, end)
    price_rows = []
    for ticker_index, ticker in enumerate(tickers):
        level = 100.0 + ticker_index
        for day_value in dates:
            level *= 1.001 + ticker_index * 0.00001
            price_rows.append(
                {
                    "date": day_value.date().isoformat(),
                    "ticker": ticker,
                    "open": round(level, 6),
                    "high": round(level * 1.01, 6),
                    "low": round(level * 0.99, 6),
                    "close": round(level, 6),
                    "adj_close": round(level, 6),
                    "volume": 0 if ticker == "^VIX" else 1_000_000,
                }
            )
    rate_rows = []
    for series_index, name in enumerate(series):
        value = 4.0 + series_index
        if name == "DTWEXBGS":
            value = 120.0
        for day_value in dates:
            rate_rows.append({"date": day_value.date().isoformat(), "series": name, "value": value})
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    pd.DataFrame(price_rows).to_csv(prices_path, index=False)
    pd.DataFrame(rate_rows).to_csv(rates_path, index=False)
    return prices_path, rates_path


def write_shadow_shortlist_and_monitoring(
    tmp_path: Path,
    *,
    degraded: bool = False,
) -> dict[str, Any]:
    shortlist_dir = tmp_path / "shadow_shortlist" / "shadow-shortlist-1"
    shortlist_dir.mkdir(parents=True, exist_ok=True)
    candidates = ["candidate-a", "candidate-b"]
    (shortlist_dir / "shadow_shortlist_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "shadow_shortlist_id": "shadow-shortlist-1",
                "status": "PASS",
                "candidate_count": len(candidates),
                "production_candidate_generated": False,
                "broker_action_taken": False,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (shortlist_dir / "shadow_shortlist_candidates.jsonl").write_text(
        "\n".join(json.dumps({"candidate_id": item}, sort_keys=True) for item in candidates) + "\n",
        encoding="utf-8",
    )
    monitor_root = tmp_path / "shadow_monitor_runs"
    drift_root = tmp_path / "consensus_drift"
    for index, day_value in enumerate(pd.bdate_range("2026-05-01", periods=31)):
        run_id = f"monitor-{index:02d}"
        run_dir = monitor_root / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "shadow_monitor_manifest.json").write_text(
            json.dumps(
                {
                    "schema_version": SCHEMA_VERSION,
                    "monitor_run_id": run_id,
                    "shadow_shortlist_id": "shadow-shortlist-1",
                    "as_of": day_value.date().isoformat(),
                    "status": "PASS",
                    "candidate_count": len(candidates),
                    "broker_action_allowed": False,
                    "broker_action_taken": False,
                    "production_candidate_generated": False,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        rows = []
        for candidate in candidates:
            is_degraded_candidate = degraded and candidate == "candidate-b"
            rows.append(
                {
                    "candidate_id": candidate,
                    "as_of": day_value.date().isoformat(),
                    "target_weights": {
                        "QQQ": 0.45 + (index % 4) * 0.01,
                        "SMH": 0.25,
                        "TLT": 0.05,
                        "CASH": 0.25 - (index % 4) * 0.01,
                    },
                    "live_vs_backtest_drift": {
                        "status": "WARN" if is_degraded_candidate and index >= 28 else "PASS"
                    },
                    "recommendation": (
                        "required_downgrade"
                        if is_degraded_candidate and index >= 30
                        else "continue_monitoring"
                    ),
                }
            )
        (run_dir / "shadow_candidate_daily_results.jsonl").write_text(
            "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
            encoding="utf-8",
        )
        drift_dir = drift_root / f"drift-{index:02d}"
        drift_dir.mkdir(parents=True, exist_ok=True)
        (drift_dir / "consensus_drift_manifest.json").write_text(
            json.dumps(
                {
                    "schema_version": SCHEMA_VERSION,
                    "drift_id": f"drift-{index:02d}",
                    "monitor_run_id": run_id,
                    "shadow_shortlist_id": "shadow-shortlist-1",
                    "as_of": day_value.date().isoformat(),
                    "status": "PASS",
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        (drift_dir / "candidate_drift_status.jsonl").write_text(
            "\n".join(
                json.dumps(
                    {
                        "candidate_id": candidate,
                        "disagreement_status": "CONSENSUS",
                    },
                    sort_keys=True,
                )
                for candidate in candidates
            )
            + "\n",
            encoding="utf-8",
        )
        (drift_dir / "consensus_drift_summary.json").write_text(
            json.dumps(
                {
                    "schema_version": SCHEMA_VERSION,
                    "drift_id": f"drift-{index:02d}",
                    "monitor_run_id": run_id,
                    "shadow_shortlist_id": "shadow-shortlist-1",
                    "as_of": day_value.date().isoformat(),
                    "disagreement_status": "CONSENSUS",
                    "position_advisory_implication": "continue_monitoring",
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
    return {
        "shadow_shortlist_id": "shadow-shortlist-1",
        "shadow_shortlist_dir": tmp_path / "shadow_shortlist",
        "shadow_monitor_run_dir": monitor_root,
        "consensus_drift_dir": drift_root,
    }


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
