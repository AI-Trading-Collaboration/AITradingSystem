from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from exposure_cap_diagnostics_review_fixtures import (
    build_exposure_cap_diagnostics_review_fixture,
)

from ai_trading_system.exposure_cap_diagnostics_review import (
    run_exposure_cap_vs_no_cap_diagnostics_review,
)


def build_dynamic_target_baseline_preparation_fixture(
    tmp_path: Path,
    *,
    source_kind: str = "ready",
) -> dict[str, Path]:
    upstream = build_exposure_cap_diagnostics_review_fixture(tmp_path)
    diagnostics_dir = tmp_path / "diagnostics"
    run_exposure_cap_vs_no_cap_diagnostics_review(
        dry_run_dir=upstream["dry_run_dir"],
        source_binding_dir=upstream["source_binding_dir"],
        baseline_decision_dir=upstream["baseline_decision_dir"],
        simulation_policy_dir=upstream["simulation_policy_dir"],
        output_dir=diagnostics_dir,
        docs_root=tmp_path / "diagnostics_docs",
    )
    force_2328_route(diagnostics_dir)
    candidate_root = tmp_path / "candidates"
    candidate_root.mkdir()
    if source_kind == "ready":
        write_ready_dynamic_source(candidate_root)
    elif source_kind == "schema_adapter":
        write_schema_adapter_dynamic_source(candidate_root)
    elif source_kind == "current_only":
        write_current_only_dynamic_source(candidate_root)
    elif source_kind == "missing":
        pass
    else:
        raise ValueError(source_kind)
    return {
        "diagnostics_dir": diagnostics_dir,
        "static_dry_run_dir": upstream["dry_run_dir"],
        "baseline_decision_dir": upstream["baseline_decision_dir"],
        "source_binding_dir": upstream["source_binding_dir"],
        "simulation_policy_dir": upstream["simulation_policy_dir"],
        "candidate_root": candidate_root,
    }


def force_2328_route(diagnostics_dir: Path) -> None:
    summary_path = diagnostics_dir / "exposure_cap_diagnostics_review_summary.json"
    summary = read_json(summary_path)
    summary["overall_recommendation"] = "MOVE_TO_DYNAMIC_TARGET_BASELINE_PREPARATION"
    summary["next_task"] = "TRADING-2328_Dynamic_Target_Baseline_Preparation"
    write_json(summary_path, summary)

    decision_path = diagnostics_dir / "exposure_cap_diagnostics_decision_matrix.json"
    decision = read_json(decision_path)
    decision["overall_recommendation"] = "MOVE_TO_DYNAMIC_TARGET_BASELINE_PREPARATION"
    write_json(decision_path, decision)

    route_path = diagnostics_dir / "exposure_cap_2328_task_route.json"
    route = read_json(route_path)
    route["next_task"] = "TRADING-2328_Dynamic_Target_Baseline_Preparation"
    write_json(route_path, route)


def write_ready_dynamic_source(root: Path) -> Path:
    path = root / "dynamic_strategy_target_exposure.json"
    rows = []
    for asset, exposure in (("QQQ", 0.4), ("SPY", 0.3), ("SMH", 0.2)):
        rows.append(
            {
                "date": "2023-01-06",
                "target_asset": asset,
                "target_exposure": exposure,
                "risk_asset_exposure": exposure,
                "asset_weight": exposure,
                "cash_weight": 0.1,
                "as_of_timestamp": "2023-01-05T21:00:00Z",
                "decision_timestamp": "2023-01-05T21:00:00Z",
                "valid_from": "2023-01-06",
                "valid_until": "2023-01-09",
                "rebalance_flag": False,
                "cooldown_state": "inactive",
                "source_artifact_hash": "fixture-source-hash",
                "signal_source_id": "fixture_signal",
                "advisory_id": "fixture_advisory_001",
                "horizon": "10d",
            }
        )
    write_json(
        path,
        {
            "artifact_role": "dynamic_strategy_target_exposure",
            "data_quality_status": "PASS_WITH_WARNINGS",
            "rows": rows,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
    )
    return path


def write_schema_adapter_dynamic_source(root: Path) -> Path:
    path = root / "dynamic_strategy_target_exposure_needs_adapter.json"
    rows = [
        {
            "date": "2023-01-06",
            "target_asset": asset,
            "recommended_weight": exposure,
            "horizon": "10d",
        }
        for asset, exposure in (("QQQ", 0.4), ("SPY", 0.3), ("SMH", 0.2))
    ]
    write_json(
        path,
        {
            "artifact_role": "dynamic_strategy_target_exposure",
            "rows": rows,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
    )
    return path


def write_current_only_dynamic_source(root: Path) -> Path:
    path = root / "dynamic_strategy_target_exposure_current.json"
    write_json(
        path,
        {
            "artifact_role": "dynamic_strategy_target_exposure",
            "rows": [
                {
                    "target_asset": "QQQ",
                    "target_exposure": 0.4,
                    "risk_asset_exposure": 0.4,
                    "asset_weight": 0.4,
                }
            ],
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
    )
    return path


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload
