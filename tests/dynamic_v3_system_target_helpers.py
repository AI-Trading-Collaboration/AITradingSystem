from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target

TARGET_AS_OF = date(2026, 1, 5)
EVALUATION_AS_OF = date(2026, 1, 8)


def write_model_target_config(tmp_path: Path) -> Path:
    advisory_path = tmp_path / "position_advisory_v1.yaml"
    advisory_path.write_text(
        "\n".join(
            [
                "schema_version: 1",
                "advisory_limits:",
                "  max_single_day_total_adjustment: 0.12",
                "  max_single_symbol_adjustment: 0.06",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "model_target_portfolio_v1.yaml"
    config_path.write_text(
        f"""
schema_version: 1
model_target:
  name: test_dynamic_v3_rescue_research_model_target_v1
  mode: research_target_only
  not_official_target_weights: true
  paper_shadow_only: true
source:
  shadow_shortlist: latest
  candidate_cluster: latest
  position_advisory_config: {advisory_path.as_posix()}
target_methods:
  enabled:
    - static_baseline
    - no_trade_baseline
    - consensus_target
    - limited_adjustment
    - defensive_limited_adjustment
    - equal_weight_shadow_candidates
    - selected_top_candidate
baseline:
  static_weights:
    QQQ: 0.50
    SMH: 0.20
    TLT: 0.10
    CASH: 0.20
method_policy:
  defensive_limited_adjustment:
    semiconductor_symbols:
      - SMH
      - SOXX
    growth_symbols:
      - QQQ
    semiconductor_reduction: 0.03
    growth_reduction: 0.02
    max_cash_weight: 0.35
  review_policy:
    preferred_method_order:
      - limited_adjustment
      - defensive_limited_adjustment
      - consensus_target
constraints:
  max_single_symbol_weight: 0.65
  max_semiconductor_weight: 0.35
  min_cash_weight: 0.05
  max_total_risk_asset_weight: 0.95
  semiconductor_symbols:
    - SMH
    - SOXX
  defensive_symbols:
    - CASH
    - TLT
safety:
  research_target_only: true
  paper_shadow_only: true
  not_official_target_weights: true
  broker_action_allowed: false
  broker_action_taken: false
  order_ticket_generated: false
  production_effect: none
  auto_apply: false
""".lstrip(),
        encoding="utf-8",
    )
    return config_path


def write_paper_shadow_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "paper_shadow_account_v1.yaml"
    config_path.write_text(
        """
schema_version: 1
paper_shadow_account:
  name: test_dynamic_v3_rescue_paper_shadow_account_v1
  mode: paper_shadow_only
  base_currency: USD
  initial_equity: 100000
  start_date: "2022-12-01"
  initial_method: static_baseline
tracking:
  target_methods:
    - static_baseline
    - no_trade_baseline
    - consensus_target
    - limited_adjustment
    - defensive_limited_adjustment
    - equal_weight_shadow_candidates
    - selected_top_candidate
baseline:
  static_weights:
    QQQ: 0.50
    SMH: 0.20
    TLT: 0.10
    CASH: 0.20
safety:
  research_target_only: true
  paper_shadow_only: true
  not_official_target_weights: true
  broker_action_allowed: false
  broker_action_taken: false
  production_effect: none
  order_ticket_generated: false
  auto_apply: false
""".lstrip(),
        encoding="utf-8",
    )
    return config_path


def write_target_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    daily_dir = tmp_path / "position_advisory_daily" / "daily-1"
    daily_dir.mkdir(parents=True, exist_ok=True)
    candidates = [
        {
            "candidate_id": "candidate-a",
            "shortlist_rank": 1,
            "shortlist_score": 0.91,
            "target_weights": {
                "QQQ": 0.55,
                "SMH": 0.24,
                "SOXX": 0.04,
                "TLT": 0.05,
                "CASH": 0.12,
            },
        },
        {
            "candidate_id": "candidate-b",
            "shortlist_rank": 2,
            "shortlist_score": 0.84,
            "target_weights": {
                "QQQ": 0.49,
                "SMH": 0.18,
                "SOXX": 0.08,
                "TLT": 0.10,
                "CASH": 0.15,
            },
        },
    ]
    _write_jsonl(daily_dir / "daily_candidate_targets.jsonl", candidates)
    (daily_dir / "daily_consensus_weights.csv").write_text(
        "\n".join(
            [
                "symbol,mean_target_weight",
                "QQQ,0.52",
                "SMH,0.21",
                "SOXX,0.06",
                "TLT,0.08",
                "CASH,0.13",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    _write_json(
        daily_dir / "daily_advisory_actions.json",
        {"daily_advisory_id": "daily-1", "consensus_status": "PASS"},
    )

    monitor_dir = tmp_path / "shadow_monitor_runs" / "monitor-1"
    monitor_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        monitor_dir / "shadow_monitor_manifest.json",
        {"monitor_run_id": "monitor-1", "status": "PASS"},
    )
    _write_jsonl(monitor_dir / "shadow_candidate_daily_results.jsonl", candidates)

    shortlist_dir = tmp_path / "shadow_shortlist" / "shortlist-1"
    shortlist_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        shortlist_dir / "shadow_shortlist_manifest.json",
        {"shadow_shortlist_id": "shortlist-1", "status": "PASS"},
    )

    drift_dir = tmp_path / "consensus_drift" / "drift-1"
    drift_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        drift_dir / "consensus_drift_summary.json",
        {"drift_id": "drift-1", "disagreement_status": "LOW_DISAGREEMENT"},
    )
    return {
        "position_advisory_daily_dir": tmp_path / "position_advisory_daily",
        "shadow_monitor_dir": tmp_path / "shadow_monitor_runs",
        "shadow_shortlist_dir": tmp_path / "shadow_shortlist",
        "consensus_drift_dir": tmp_path / "consensus_drift",
    }


def build_model_target_fixture(tmp_path: Path) -> dict[str, Any]:
    source_dirs = write_target_source_artifacts(tmp_path)
    config_path = write_model_target_config(tmp_path)
    result = system_target.generate_model_target(
        config_path=config_path,
        as_of=TARGET_AS_OF,
        output_dir=tmp_path / "model_target",
        generated_at=datetime(2026, 1, 5, tzinfo=UTC),
        **source_dirs,
    )
    return {"config_path": config_path, **source_dirs, **result}


def build_rebalanced_shadow_fixture(tmp_path: Path) -> dict[str, Any]:
    target = build_model_target_fixture(tmp_path)
    paper_config = write_paper_shadow_config(tmp_path)
    paper = system_target.init_paper_shadow_account(
        config_path=paper_config,
        output_dir=tmp_path / "paper_shadow",
        model_target_dir=tmp_path / "model_target",
        generated_at=datetime(2026, 1, 5, 1, tzinfo=UTC),
    )
    rebalance = system_target.simulate_model_rebalance(
        paper_shadow_id=paper["paper_shadow_id"],
        target_id=target["target_id"],
        paper_shadow_dir=tmp_path / "paper_shadow",
        model_target_dir=tmp_path / "model_target",
        output_dir=tmp_path / "model_rebalance",
        generated_at=datetime(2026, 1, 5, 2, tzinfo=UTC),
    )
    return {
        "target": target,
        "paper_config_path": paper_config,
        "paper": paper,
        "rebalance": rebalance,
    }


def run_performance_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = build_rebalanced_shadow_fixture(tmp_path)
    prices_path, rates_path = write_market_cache(tmp_path / "market_cache")
    performance = system_target.run_paper_shadow_performance(
        paper_shadow_id=fixture["paper"]["paper_shadow_id"],
        paper_shadow_dir=tmp_path / "paper_shadow",
        output_dir=tmp_path / "paper_shadow_performance",
        price_cache_path=prices_path,
        rates_cache_path=rates_path,
        as_of=EVALUATION_AS_OF,
        generated_at=datetime(2026, 1, 8, tzinfo=UTC),
    )
    return {
        **fixture,
        "prices_path": prices_path,
        "rates_path": rates_path,
        "performance": performance,
    }


def run_review_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_performance_fixture(tmp_path)
    review = system_target.build_system_target_review_pack(
        target_id=fixture["target"]["target_id"],
        paper_shadow_id=fixture["paper"]["paper_shadow_id"],
        performance_id=fixture["performance"]["performance_id"],
        model_target_dir=tmp_path / "model_target",
        paper_shadow_dir=tmp_path / "paper_shadow",
        performance_dir=tmp_path / "paper_shadow_performance",
        output_dir=tmp_path / "system_target_review",
        generated_at=datetime(2026, 1, 8, 1, tzinfo=UTC),
    )
    return {**fixture, "review": review}


def write_market_cache(root: Path) -> tuple[Path, Path]:
    root.mkdir(parents=True, exist_ok=True)
    prices_path = root / "prices_daily.csv"
    rates_path = root / "rates_daily.csv"
    symbols = ("QQQ", "SMH", "SOXX", "TLT")
    price_lines = ["date,ticker,open,high,low,close,adj_close,volume"]
    for symbol_index, symbol in enumerate(symbols):
        level = 100.0 + symbol_index
        for day_index, day in enumerate(("2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08")):
            level *= 1.002 + symbol_index * 0.0005 + day_index * 0.0001
            price_lines.append(
                f"{day},{symbol},{level:.4f},{level * 1.01:.4f},"
                f"{level * 0.99:.4f},{level:.4f},{level:.4f},1000000"
            )
    prices_path.write_text("\n".join(price_lines) + "\n", encoding="utf-8")
    rates_path.write_text(
        "\n".join(
            [
                "date,series,value",
                "2026-01-05,FEDFUNDS,4.0",
                "2026-01-06,FEDFUNDS,4.0",
                "2026-01-07,FEDFUNDS,4.0",
                "2026-01-08,FEDFUNDS,4.0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return prices_path, rates_path


def report_index_for_review_fixture(fixture: dict[str, Any]) -> dict[str, Any]:
    target_dir = fixture["target"]["target_dir"]
    paper_dir = fixture["paper"]["paper_shadow_dir"]
    rebalance_dir = fixture["rebalance"]["rebalance_dir"]
    performance_dir = fixture["performance"]["performance_dir"]
    review_dir = fixture["review"]["review_dir"]
    return {
        "reports": [
            {
                "report_id": "etf_dynamic_v3_model_target",
                "latest_artifact_path": str(target_dir / "model_target_manifest.json"),
            },
            {
                "report_id": "etf_dynamic_v3_paper_shadow",
                "latest_artifact_path": str(paper_dir / "paper_shadow_manifest.json"),
            },
            {
                "report_id": "etf_dynamic_v3_model_rebalance",
                "latest_artifact_path": str(rebalance_dir / "model_rebalance_manifest.json"),
            },
            {
                "report_id": "etf_dynamic_v3_paper_shadow_performance",
                "latest_artifact_path": str(
                    performance_dir / "paper_shadow_performance_manifest.json"
                ),
            },
            {
                "report_id": "etf_dynamic_v3_system_target_review",
                "latest_artifact_path": str(review_dir / "system_target_review_manifest.json"),
            },
        ]
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )
