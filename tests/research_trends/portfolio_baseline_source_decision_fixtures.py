from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def write_source_binding_fixture(tmp_path: Path) -> Path:
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
        "actual_requested_date_range": "2023-01-06..2026-06-18",
        "source_binding_status": "SOURCE_BOUND_WITH_SYNTHETIC_BASELINE",
        "dry_run_readiness_status": "SOURCE_BOUND_DRY_RUN_READY_WITH_SYNTHETIC_BASELINE",
        "portfolio_source_mode": "synthetic_observe_only",
        "dry_run_record_count": 12,
        "target_assets": ["QQQ", "SPY", "SMH"],
        "simulation_executed": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "target_weight_generated": False,
        "rebalance_instruction_generated": False,
        "broker_order_generated": False,
        "paper_shadow_order_generated": False,
        "production_decision_generated": False,
    }
    write_json(output_dir / "exposure_cap_source_binding_summary.json", common)
    write_json(
        output_dir / "exposure_cap_source_inventory.json",
        {**common, "sources": []},
    )
    write_json(output_dir / "exposure_cap_source_gap_matrix.json", {**common, "rows": []})
    write_json(output_dir / "risk_cap_trigger_series_binding_report.json", common)
    write_json(output_dir / "market_data_binding_report.json", common)
    write_json(
        output_dir / "portfolio_baseline_binding_report.json",
        {
            **common,
            "source_category": "portfolio_baseline",
            "portfolio_source_mode": "synthetic_observe_only",
            "synthetic_observe_only": True,
            "real_portfolio_baseline_bound": False,
            "coverage_start": "2023-01-06",
            "coverage_end": "2026-06-18",
            "row_count": 12,
        },
    )
    write_json(output_dir / "turnover_rebalance_assumption_report.json", common)
    write_json(
        output_dir / "source_bound_dry_run_simulation_readiness.json",
        {**common, "dry_run_allowed": True, "full_simulation_allowed": False},
    )
    write_json(output_dir / "source_bound_dry_run_safety_boundary.json", common)
    write_json(
        output_dir / "exposure_cap_simulation_next_task_route.json",
        {**common, "next_task": "TRADING-2325_Portfolio_Baseline_Source_Decision"},
    )
    write_json(output_dir / "source_bound_exposure_cap_dry_run_result.json", common)
    (output_dir / "source_bound_exposure_cap_dry_run_result.csv").write_text(
        "date,target_asset,baseline_exposure\n2023-01-06,QQQ,1.0\n",
        encoding="utf-8",
    )
    write_json(output_dir / "exposure_cap_vs_no_cap_comparison.json", common)
    write_json(output_dir / "exposure_cap_turnover_impact_report.json", common)
    write_json(output_dir / "exposure_cap_cooldown_impact_report.json", common)
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
        "source_blocked_no_simulation": True,
        "simulation_executed": False,
        "simulation_result_generated": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "target_weight_generated": False,
        "rebalance_instruction_generated": False,
        "broker_order_generated": False,
        "paper_shadow_order_generated": False,
        "production_decision_generated": False,
    }
    for filename in (
        "exposure_cap_mechanics_simulation_summary.json",
        "exposure_cap_simulation_readiness_matrix.json",
        "exposure_cap_simulation_metric_contract.json",
        "exposure_cap_simulation_input_requirement_matrix.json",
        "exposure_cap_simulation_blocker_report.json",
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
    default_weight: 0.40
  SPY:
    default_weight: 0.30
  SMH:
    default_weight: 0.15
  CASH:
    default_weight: 0.15
""".lstrip(),
        encoding="utf-8",
    )
    return config_dir


def write_paper_portfolio_fixture(tmp_path: Path) -> Path:
    path = tmp_path / "paper_portfolio_v1.yaml"
    path.write_text(
        """
policy_metadata:
  version: test_paper_portfolio_v1
  status: test
history:
  history_start: ''
  history_end: ''
""".lstrip(),
        encoding="utf-8",
    )
    return path


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
