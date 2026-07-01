from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ai_trading_system.post_2085_research_common import load_adjusted_price_matrix
from ai_trading_system.source_bound_static_etf_dry_run import (
    build_portfolio_level_trigger_map,
    build_risk_cap_trigger_alignment_matrix,
    build_simulation_calendar_from_sources,
    build_source_bound_static_etf_dry_run_rows,
    build_static_etf_baseline_exposure_schedule,
    load_baseline_decision_outputs,
    load_exposure_cap_policy,
    load_risk_cap_trigger_frame_from_source_binding,
    load_simulation_policy_outputs,
    load_source_binding_outputs,
    load_static_etf_config,
)

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "dry_run_only": True,
    "manual_review_only": True,
    "simulation_executed": False,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "portfolio_effect": "none",
    "real_portfolio_effect": "none",
    "target_weight_generated": False,
    "rebalance_instruction_generated": False,
    "broker_order_generated": False,
    "paper_shadow_order_generated": False,
    "production_decision_generated": False,
}

TARGET_ASSETS = ("QQQ", "SPY", "SMH")


def build_source_bound_static_etf_dry_run_fixture(tmp_path: Path) -> dict[str, Path]:
    policy_path = write_policy_fixture(tmp_path)
    trigger_path = write_risk_cap_trigger_fixture(tmp_path)
    return {
        "source_binding_dir": write_source_binding_fixture(
            tmp_path,
            policy_path=policy_path,
            trigger_path=trigger_path,
        ),
        "baseline_decision_dir": write_baseline_decision_fixture(tmp_path),
        "simulation_policy_dir": write_simulation_policy_fixture(tmp_path),
        "portfolio_config_dir": write_portfolio_config_fixture(tmp_path),
        "prices_path": write_prices_fixture(tmp_path),
        "rates_path": write_rates_fixture(tmp_path),
        "policy_path": policy_path,
        "trigger_path": trigger_path,
    }


def load_dry_run_components(fixture: dict[str, Path]) -> dict[str, Any]:
    baseline_decision = load_baseline_decision_outputs(fixture["baseline_decision_dir"])
    source_binding = load_source_binding_outputs(fixture["source_binding_dir"])
    load_simulation_policy_outputs(fixture["simulation_policy_dir"])
    policy = load_exposure_cap_policy(fixture["policy_path"])
    static_config = load_static_etf_config(fixture["portfolio_config_dir"], TARGET_ASSETS)
    trigger_frame = load_risk_cap_trigger_frame_from_source_binding(
        source_binding,
        TARGET_ASSETS,
    )
    price_matrix = load_adjusted_price_matrix(fixture["prices_path"], TARGET_ASSETS)
    simulation_dates = build_simulation_calendar_from_sources(
        trigger_frame=trigger_frame,
        price_matrix=price_matrix,
        target_assets=TARGET_ASSETS,
    )
    schedule_rows = build_static_etf_baseline_exposure_schedule(
        static_config=static_config,
        simulation_dates=simulation_dates,
    )
    trigger_map = build_portfolio_level_trigger_map(trigger_frame)
    alignment_rows = build_risk_cap_trigger_alignment_matrix(
        simulation_dates=simulation_dates,
        target_assets=TARGET_ASSETS,
        schedule_rows=schedule_rows,
        price_matrix=price_matrix,
        date_trigger_map=trigger_map,
        trigger_source_hash=str(trigger_frame.attrs.get("source_hash", "")),
    )
    dry_run_rows = build_source_bound_static_etf_dry_run_rows(
        policy=policy,
        simulation_dates=simulation_dates,
        schedule_rows=schedule_rows,
        price_matrix=price_matrix,
        alignment_rows=alignment_rows,
        date_trigger_map=trigger_map,
        target_assets=TARGET_ASSETS,
        data_quality_status="PASS",
    )
    return {
        "baseline_decision": baseline_decision,
        "source_binding": source_binding,
        "policy": policy,
        "static_config": static_config,
        "trigger_frame": trigger_frame,
        "price_matrix": price_matrix,
        "simulation_dates": simulation_dates,
        "schedule_rows": schedule_rows,
        "trigger_map": trigger_map,
        "alignment_rows": alignment_rows,
        "dry_run_rows": dry_run_rows,
    }


def write_source_binding_fixture(
    tmp_path: Path,
    *,
    policy_path: Path,
    trigger_path: Path,
) -> Path:
    output_dir = tmp_path / "source_binding"
    output_dir.mkdir()
    common = {
        "schema_version": "exposure_cap_simulation_source_binding.v1",
        "report_type": "exposure_cap_simulation_source_binding",
        "artifact_role": "exposure_cap_simulation_source_binding",
        "task_id": "TRADING-2324_EXPOSURE_CAP_SIMULATION_SOURCE_BINDING",
        "status": "EXPOSURE_CAP_SIMULATION_SOURCE_BOUND_DRY_RUN_READY_PROMOTION_BLOCKED",
        "market_regime": "ai_after_chatgpt",
        "selected_market_regime": "ai_after_chatgpt",
        "policy_path": str(policy_path),
        "target_assets": list(TARGET_ASSETS),
        **SAFETY_FIELDS,
    }
    for filename, payload in {
        "exposure_cap_source_binding_summary.json": common,
        "exposure_cap_source_inventory.json": {**common, "sources": []},
        "exposure_cap_source_gap_matrix.json": {**common, "rows": []},
        "risk_cap_trigger_series_binding_report.json": {
            **common,
            "source_path": str(trigger_path),
            "source_hash": file_sha256(trigger_path),
            "active_trigger_record_count": 3,
            "bound_asset_count": 3,
        },
        "market_data_binding_report.json": common,
        "portfolio_baseline_binding_report.json": {
            **common,
            "portfolio_source_mode": "synthetic_observe_only",
            "real_portfolio_baseline_bound": False,
        },
        "turnover_rebalance_assumption_report.json": common,
        "source_bound_dry_run_simulation_readiness.json": {
            **common,
            "dry_run_allowed": True,
            "full_simulation_allowed": False,
        },
        "source_bound_dry_run_safety_boundary.json": common,
        "exposure_cap_simulation_next_task_route.json": {
            **common,
            "next_task": "TRADING-2325_Portfolio_Baseline_Source_Decision",
        },
    }.items():
        write_json(output_dir / filename, payload)
    return output_dir


def write_baseline_decision_fixture(tmp_path: Path) -> Path:
    output_dir = tmp_path / "baseline_decision"
    output_dir.mkdir()
    common = {
        "schema_version": "portfolio_baseline_source_decision.v1",
        "report_type": "portfolio_baseline_source_decision",
        "artifact_role": "portfolio_baseline_source_decision",
        "task_id": "TRADING-2325_PORTFOLIO_BASELINE_SOURCE_DECISION",
        "status": "PORTFOLIO_BASELINE_SOURCE_DECISION_READY_PROMOTION_BLOCKED",
        "selected_baseline_for_2326": "static_etf_allocation_baseline",
        "target_assets": list(TARGET_ASSETS),
        **SAFETY_FIELDS,
    }
    for filename, payload in {
        "portfolio_baseline_source_decision_summary.json": common,
        "portfolio_baseline_candidate_matrix.json": {**common, "rows": []},
        "portfolio_baseline_source_feasibility_matrix.json": {**common, "rows": []},
        "portfolio_baseline_pit_reproducibility_audit.json": {**common, "rows": []},
        "portfolio_baseline_risk_matrix.json": {**common, "rows": []},
        "portfolio_baseline_field_requirement_matrix.json": {**common, "rows": []},
        "portfolio_baseline_recommendation.json": {
            **common,
            "recommended_baseline": "static_etf_allocation_baseline",
        },
        "recommended_exposure_cap_simulation_baseline.json": {
            **common,
            "selected_for_2326": "static_etf_allocation_baseline",
            "fallback_if_static_config_invalid": "synthetic_observe_only_baseline",
        },
        "exposure_cap_2326_task_route.json": {
            **common,
            "next_task": (
                "TRADING-2326_Source_Bound_Exposure_Cap_Dry_Run_"
                "With_Static_ETF_Baseline"
            ),
        },
        "portfolio_baseline_source_safety_boundary.json": common,
    }.items():
        write_json(output_dir / filename, payload)
    return output_dir


def write_simulation_policy_fixture(tmp_path: Path) -> Path:
    output_dir = tmp_path / "simulation_policy"
    output_dir.mkdir()
    common = {
        "schema_version": "exposure_cap_mechanics_simulation.v1",
        "report_type": "exposure_cap_mechanics_simulation",
        "artifact_role": "exposure_cap_mechanics_simulation_source_blocked",
        "task_id": "TRADING-2323_EXPOSURE_CAP_MECHANICS_SIMULATION",
        "status": "EXPOSURE_CAP_MECHANICS_SIMULATION_SOURCE_BLOCKED_NOT_EXECUTED",
        **SAFETY_FIELDS,
    }
    for filename in (
        "exposure_cap_mechanics_simulation_summary.json",
        "exposure_cap_simulation_readiness_matrix.json",
        "exposure_cap_simulation_metric_contract.json",
        "exposure_cap_simulation_safety_boundary.json",
    ):
        write_json(output_dir / filename, {**common, "rows": []})
    return output_dir


def write_portfolio_config_fixture(tmp_path: Path) -> Path:
    config_dir = tmp_path / "portfolio_config"
    config_dir.mkdir()
    (config_dir / "assets.yaml").write_text(
        """
policy_metadata:
  version: test_static_assets_v1
  status: test
assets:
  QQQ:
    asset_type: ETF
    sleeve: growth
    default_weight: 0.40
    risk_group: us_growth
  SPY:
    asset_type: ETF
    sleeve: core_market
    default_weight: 0.30
    risk_group: us_equity
  SMH:
    asset_type: ETF
    sleeve: ai_semiconductor
    default_weight: 0.15
    risk_group: semiconductor
  SOXX:
    asset_type: ETF
    default_weight: 0.00
    risk_group: semiconductor
  CASH:
    asset_type: CASH
    sleeve: defense
    default_weight: 0.15
    risk_group: cash
""".lstrip(),
        encoding="utf-8",
    )
    return config_dir


def write_policy_fixture(tmp_path: Path) -> Path:
    policy_path = tmp_path / "exposure_cap_policy.yaml"
    policy_path.write_text(
        """
policy_id: exposure_cap_simulation_source_binding_policy
version: v1
status: pilot_research_source_bound_dry_run
task_id: TRADING-2324_EXPOSURE_CAP_SIMULATION_SOURCE_BINDING
source_requirements:
  required_market_symbols:
    - QQQ
    - SPY
    - SMH
cap_policy:
  default_max_allowed_exposure: 1.0
  max_allowed_exposure_by_intensity:
    low: 0.85
    medium: 0.70
    high: 0.50
cooldown_policy:
  default_cooldown_days: 2
  cooldown_days_by_intensity:
    low: 1
    medium: 2
    high: 3
safety:
  research_only: true
  dry_run_only: true
  manual_review_only: true
  promotion_allowed: false
  paper_shadow_allowed: false
  production_allowed: false
  broker_action: none
  target_weight_generated: false
  rebalance_instruction_generated: false
  broker_order_generated: false
""".lstrip(),
        encoding="utf-8",
    )
    return policy_path


def write_risk_cap_trigger_fixture(tmp_path: Path) -> Path:
    path = tmp_path / "risk_cap_trigger_series.csv"
    path.write_text(
        "\n".join(
            [
                "target_asset,horizon,source_date,scope_active,usage_role,"
                "risk_cap_score,risk_cap_intensity,signal_direction",
                "QQQ,10d,2023-01-04,true,risk_cap_only,0.70,medium,risk_off",
                "SPY,10d,2023-01-06,true,risk_cap_only,0.90,high,risk_off",
                "SMH,10d,2023-01-10,true,risk_cap_only,0.40,low,risk_off",
            ]
        ),
        encoding="utf-8",
    )
    return path


def write_prices_fixture(tmp_path: Path) -> Path:
    path = tmp_path / "prices_daily.csv"
    dates = ["2023-01-03", "2023-01-04", "2023-01-05", "2023-01-06", "2023-01-09", "2023-01-10"]
    prices = {
        "QQQ": [100.0, 102.0, 101.0, 99.0, 103.0, 104.0],
        "SPY": [90.0, 91.0, 90.0, 88.0, 89.0, 90.0],
        "SMH": [80.0, 81.0, 82.0, 79.0, 80.0, 83.0],
    }
    lines = ["date,ticker,open,high,low,close,adj_close,volume"]
    for index, current in enumerate(dates):
        for ticker, values in prices.items():
            close = values[index]
            lines.append(
                f"{current},{ticker},{close},{close},{close},{close},{close},1000"
            )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def write_rates_fixture(tmp_path: Path) -> Path:
    path = tmp_path / "rates_daily.csv"
    dates = ["2023-01-03", "2023-01-04", "2023-01-05", "2023-01-06", "2023-01-09", "2023-01-10"]
    lines = ["date,series,value"]
    lines.extend(f"{current},DGS10,{3.5 + index * 0.01}" for index, current in enumerate(dates))
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def file_sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()
