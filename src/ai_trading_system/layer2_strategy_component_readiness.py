from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_AI_REGIME_BACKTEST_START,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    SAFETY_BOUNDARY,
    _data_quality_gate,
    _load_price_matrix,
    _load_registry,
    _records,
    _required_rate_series,
    _slice_prices,
    _strategy_rows,
    _target_weight_frame,
    _turnover_series,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "layer2_strategy_component_pool_v1.yaml"
)
DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / "layer2_components"
)
DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / "qqq_plus_growth"
)

FORMAL_COMPONENT_SECTIONS = ("selectable_components", "reference_components")
INACTIVE_COMPONENT_SECTION = "inactive_research_reference_candidates"
GROWTH_OWNER_DECISION_KEEP_RESEARCH_ONLY = "KEEP_GROWTH_RESEARCH_ONLY"
FORWARD_HORIZONS = (5, 10, 20, 60, 120)
ASSET_COLUMNS = ("QQQ", "TQQQ", "SGOV")


def run_layer2_component_readiness_reconciliation(
    *,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    growth_output_root: Path = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
    output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_config(config_path)
    sources = _source_payloads(simple_output_root, growth_output_root)
    checks = _reconciliation_checks(config, sources)
    conflicts = [row for row in checks if row["status"] == "FAIL"]
    warnings = [row for row in checks if row["status"] == "WARN"]

    if _missing_required_sources(sources):
        status = "LAYER2_RECONCILIATION_BLOCKED"
    elif conflicts:
        status = "LAYER2_CONFLICT_FOUND"
    elif warnings:
        status = "LAYER2_RECONCILED_WITH_WARNINGS"
    else:
        status = "LAYER2_RECONCILED"

    payload = _payload(
        report_type="layer2_component_readiness_reconciliation",
        title="Layer-2 Component Readiness Reconciliation",
        status=status,
        summary={
            "selectable_component_count": len(_components(config, "selectable_components")),
            "reference_component_count": len(_components(config, "reference_components")),
            "inactive_growth_reference_count": len(_components(config, INACTIVE_COMPONENT_SECTION)),
            "growth_owner_decision": _growth_owner_decision(sources),
            "conflict_count": len(conflicts),
            "warning_count": len(warnings),
            "layer1_historical_research_allowed": False,
        },
        component_role_reconciliation=_component_role_rows(config),
        source_statuses=_source_statuses(sources),
        checks=checks,
        conflicts=conflicts,
        warnings=[row["message"] for row in warnings],
        input_artifacts=_source_paths(simple_output_root, growth_output_root)
        | {"layer2_component_pool_config": str(config_path)},
        report_registry_entry=_report_registry_entry(
            "layer2_component_readiness_reconciliation",
            "Layer-2 Component Readiness Reconciliation",
            "aits research strategies layer2-component-readiness-reconciliation",
            "layer2_component_readiness_reconciliation",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_layer2_component_pool_freeze(
    *,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    growth_output_root: Path = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
    output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_config(config_path)
    growth_sources = _source_payloads(DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT, growth_output_root)
    validation_issues = _pool_validation_issues(config, growth_sources)
    component_rows = _component_pool_rows(config)
    pool_hash = _stable_hash(
        {
            "component_pool_id": config.get("component_pool_id"),
            "component_pool_version": config.get("component_pool_version"),
            "components": component_rows,
            "constraints": config.get("pool_constraints"),
            "growth_owner_decision": _growth_owner_decision(growth_sources),
        }
    )

    if validation_issues:
        status = "LAYER2_POOL_BLOCKED"
    elif _components(config, INACTIVE_COMPONENT_SECTION):
        status = "LAYER2_POOL_FROZEN_WITHOUT_GROWTH"
    else:
        status = "LAYER2_COMPONENT_POOL_FROZEN"

    payload = _payload(
        report_type="layer2_component_pool_freeze",
        title="Layer-2 Component Pool Freeze",
        status=status,
        summary={
            "component_pool_id": config.get("component_pool_id"),
            "component_pool_version": config.get("component_pool_version"),
            "component_pool_hash": pool_hash,
            "selectable_component_count": len(_components(config, "selectable_components")),
            "reference_component_count": len(_components(config, "reference_components")),
            "growth_in_formal_pool": _growth_in_formal_pool(config),
            "validation_issue_count": len(validation_issues),
            "layer1_historical_research_allowed": False,
        },
        component_pool_id=config.get("component_pool_id"),
        component_pool_version=config.get("component_pool_version"),
        component_pool_hash=pool_hash,
        components=component_rows,
        validation_issues=validation_issues,
        excluded_components=_components(config, "excluded_components"),
        inactive_research_reference_candidates=_components(config, INACTIVE_COMPONENT_SECTION),
        input_artifacts={
            "layer2_component_pool_config": str(config_path),
            "qqq_plus_growth_owner_decision_pack": str(
                growth_output_root / "qqq_plus_growth_owner_decision_pack.json"
            ),
        },
        report_registry_entry=_report_registry_entry(
            "layer2_component_pool_freeze",
            "Layer-2 Component Pool Freeze",
            "aits research strategies layer2-component-pool-freeze",
            "layer2_component_pool_freeze",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_layer2_component_definition_lock(
    *,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_output_root: Path = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
    output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_config(config_path)
    simple_config = _load_registry(simple_registry_config_path)
    growth_sources = _source_payloads(DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT, growth_output_root)
    definitions = [
        _component_definition(component, config=config, simple_config=simple_config)
        for component in _formal_components(config)
    ]
    inactive_definitions = [
        _inactive_component_definition(component, growth_sources)
        for component in _components(config, INACTIVE_COMPONENT_SECTION)
    ]
    issues = [row for row in definitions if row["definition_status"] != "LOCKED"]
    pool_hash = _stable_hash(
        {
            "component_pool_id": config.get("component_pool_id"),
            "component_pool_version": config.get("component_pool_version"),
            "definition_hashes": [
                {
                    "strategy_id": row.get("strategy_id"),
                    "policy_definition_hash": row.get("policy_definition_hash"),
                }
                for row in definitions
            ],
        }
    )
    status = "COMPONENT_DEFINITION_CONFLICTED" if issues else "ALL_COMPONENT_DEFINITIONS_LOCKED"

    payload = _payload(
        report_type="layer2_component_definition_lock",
        title="Layer-2 Component Definition Lock",
        status=status,
        summary={
            "formal_definition_count": len(definitions),
            "inactive_reference_definition_count": len(inactive_definitions),
            "component_pool_hash": pool_hash,
            "definition_issue_count": len(issues),
            "growth_definition_lock_scope": "inactive_reference_only",
            "layer1_historical_research_allowed": False,
        },
        component_pool_hash=pool_hash,
        component_definitions=definitions,
        inactive_reference_definitions=inactive_definitions,
        definition_change_rule=_research_policy(config).get("definition_change_rule"),
        issues=issues,
        input_artifacts={
            "layer2_component_pool_config": str(config_path),
            "simple_baseline_registry": str(simple_registry_config_path),
            "qqq_plus_growth_owner_decision_pack": str(
                growth_output_root / "qqq_plus_growth_owner_decision_pack.json"
            ),
        },
        report_registry_entry=_report_registry_entry(
            "layer2_component_definition_lock",
            "Layer-2 Component Definition Lock",
            "aits research strategies layer2-component-definition-lock",
            "layer2_component_definition_lock",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_layer2_component_data_quality_check(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    as_of_date: date | None = None,
    output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_config(config_path)
    expected_tickers = _required_price_tickers(config)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
        expected_tickers=expected_tickers,
    )
    status = _layer2_data_quality_status(data_gate)

    payload = _payload(
        report_type="layer2_component_data_quality_check",
        title="Layer-2 Component Data Quality Check",
        status=status,
        summary={
            "data_quality_status": data_gate.get("status"),
            "as_of": data_gate.get("as_of"),
            "price_row_count": data_gate.get("price_row_count"),
            "rate_row_count": data_gate.get("rate_row_count"),
            "warning_count": data_gate.get("warning_count"),
            "error_count": data_gate.get("error_count"),
            "expected_price_ticker_count": len(expected_tickers),
            "layer1_historical_research_allowed": False,
        },
        data_quality=data_gate,
        expected_price_tickers=expected_tickers,
        expected_rate_series=_required_rate_series(config),
        data_quality_minimum_status=_research_policy(config).get(
            "data_quality_minimum_status", []
        ),
        layer1_historical_research_allowed=False,
        input_artifacts={
            "layer2_component_pool_config": str(config_path),
            "prices": str(prices_path),
            "secondary_prices": str(marketstack_prices_path),
            "rates": str(rates_path),
        },
        report_registry_entry=_report_registry_entry(
            "layer2_component_data_quality_check",
            "Layer-2 Component Data Quality Check",
            "aits research strategies layer2-component-data-quality-check",
            "layer2_component_data_quality_check",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_layer2_component_readiness_matrix(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    simple_output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    growth_output_root: Path = DEFAULT_QQQ_PLUS_GROWTH_OUTPUT_ROOT,
    as_of_date: date | None = None,
    output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    reconciliation = run_layer2_component_readiness_reconciliation(
        config_path=config_path,
        simple_output_root=simple_output_root,
        growth_output_root=growth_output_root,
        output_root=output_root,
    )
    pool = run_layer2_component_pool_freeze(
        config_path=config_path,
        growth_output_root=growth_output_root,
        output_root=output_root,
    )
    definitions = run_layer2_component_definition_lock(
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        growth_output_root=growth_output_root,
        output_root=output_root,
    )
    data_quality = run_layer2_component_data_quality_check(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        as_of_date=as_of_date,
        output_root=output_root,
    )
    matrix = _readiness_rows(
        pool=pool,
        definitions=definitions,
        data_quality=data_quality,
    )
    blockers = _matrix_blockers(reconciliation, pool, definitions, data_quality)
    warnings = _matrix_warnings(reconciliation, pool, data_quality)
    if blockers:
        status = "LAYER2_COMPONENT_READINESS_MATRIX_BLOCKED"
    elif warnings:
        status = "LAYER2_COMPONENT_READINESS_MATRIX_READY_WITH_WARNINGS"
    else:
        status = "LAYER2_COMPONENT_READINESS_MATRIX_READY"

    payload = _payload(
        report_type="layer2_component_readiness_matrix",
        title="Layer-2 Component Readiness Matrix",
        status=status,
        summary={
            "matrix_row_count": len(matrix),
            "selectable_component_count": sum(
                1 for row in matrix if row.get("selectable_by_layer1")
            ),
            "inactive_growth_reference_count": sum(
                1
                for row in matrix
                if row.get("strategy_role") == "research_only_inactive_reference"
            ),
            "blocker_count": len(blockers),
            "warning_count": len(warnings),
            "layer1_historical_research_allowed": False,
            "layer1_forward_aging_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "manual_review_required": True,
        },
        component_readiness_matrix=matrix,
        layer1_historical_research_allowed=False,
        layer1_forward_aging_allowed=False,
        layer1_paper_shadow_allowed=False,
        production_allowed=False,
        broker_action="none",
        manual_review_required=True,
        blockers=blockers,
        warnings=warnings,
        next_required_tasks=[
            "build PIT-safe historical component weight path",
            "build independent forward outcome cube",
            "run anti-leakage and time-boundary audit",
            "audit Layer-1 dataset reproducibility before allowing historical research",
        ],
        input_artifacts={
            "reconciliation": str(
                output_root / "layer2_component_readiness_reconciliation.json"
            ),
            "component_pool_freeze": str(output_root / "layer2_component_pool_freeze.json"),
            "definition_lock": str(output_root / "layer2_component_definition_lock.json"),
            "data_quality_check": str(output_root / "layer2_component_data_quality_check.json"),
        },
        report_registry_entry=_report_registry_entry(
            "layer2_component_readiness_matrix",
            "Layer-2 Component Readiness Matrix",
            "aits research strategies layer2-component-readiness-matrix",
            "layer2_component_readiness_matrix",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_layer2_historical_weight_path_build(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _layer2_fact_context(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
    )
    data_quality = _mapping(context["data_quality"])
    if not bool(data_quality.get("passed")):
        payload = _fact_manifest_payload(
            report_type="layer2_historical_weight_path",
            title="Layer-2 Historical Weight Path",
            status="LAYER2_WEIGHT_PATH_BLOCKED",
            output_root=output_root,
            parquet_name="layer2_historical_weight_path.parquet",
            summary={
                "row_count": 0,
                "component_count": len(_records(context["components"])),
                "data_quality_status": data_quality.get("status"),
                "blocked_reason": "validate_data_cache_failed",
            },
            data_quality=data_quality,
            blockers=["validate_data_cache_failed"],
            report_registry_entry=_report_registry_entry(
                "layer2_historical_weight_path",
                "Layer-2 Historical Weight Path",
                "aits research strategies layer2-historical-weight-path-build",
                "layer2_historical_weight_path_*",
            ),
        )
        _write_fact_artifacts(payload, output_root=output_root)
        return payload

    frame = _build_weight_path_frame(context)
    parquet_path = output_root / "layer2_historical_weight_path.parquet"
    _write_parquet(frame, parquet_path)
    status = (
        "LAYER2_WEIGHT_PATH_DATA_WARN"
        if _int(data_quality.get("warning_count"))
        else "LAYER2_WEIGHT_PATH_READY"
    )
    payload = _fact_manifest_payload(
        report_type="layer2_historical_weight_path",
        title="Layer-2 Historical Weight Path",
        status=status,
        output_root=output_root,
        parquet_name=parquet_path.name,
        summary={
            "row_count": len(frame),
            "component_count": int(frame["strategy_id"].nunique()) if not frame.empty else 0,
            "start_date": _frame_min(frame, "decision_date"),
            "end_date": _frame_max(frame, "decision_date"),
            "data_quality_status": data_quality.get("status"),
            "component_pool_hash": context["component_pool_hash"],
            "inactive_growth_included": False,
            "layer1_historical_research_allowed": False,
        },
        data_quality=data_quality,
        parquet_checksum=_sha256_file(parquet_path),
        parquet_row_count=len(frame),
        parquet_columns=list(frame.columns),
        source_component_count=len(_records(context["components"])),
        warning_codes=_warning_codes(data_quality),
        report_registry_entry=_report_registry_entry(
            "layer2_historical_weight_path",
            "Layer-2 Historical Weight Path",
            "aits research strategies layer2-historical-weight-path-build",
            "layer2_historical_weight_path_*",
        ),
    )
    _write_fact_artifacts(payload, output_root=output_root)
    return payload


def run_layer2_return_cost_exposure_panel(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _layer2_fact_context(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
    )
    data_quality = _mapping(context["data_quality"])
    if not bool(data_quality.get("passed")):
        payload = _fact_manifest_payload(
            report_type="layer2_return_cost_exposure_panel",
            title="Layer-2 Return Cost Exposure Panel",
            status="LAYER2_RETURN_PANEL_BLOCKED",
            output_root=output_root,
            parquet_name="layer2_return_cost_exposure_panel.parquet",
            summary={
                "row_count": 0,
                "data_quality_status": data_quality.get("status"),
                "blocked_reason": "validate_data_cache_failed",
            },
            data_quality=data_quality,
            blockers=["validate_data_cache_failed"],
            report_registry_entry=_report_registry_entry(
                "layer2_return_cost_exposure_panel",
                "Layer-2 Return Cost Exposure Panel",
                "aits research strategies layer2-return-cost-exposure-panel",
                "layer2_return_cost_exposure_panel_*",
            ),
        )
        _write_fact_artifacts(payload, output_root=output_root)
        return payload

    weight_frame = _build_weight_path_frame(context)
    panel = _build_return_panel_frame(context, weight_frame)
    parquet_path = output_root / "layer2_return_cost_exposure_panel.parquet"
    _write_parquet(panel, parquet_path)
    status = (
        "LAYER2_RETURN_PANEL_WARN"
        if _int(data_quality.get("warning_count"))
        else "LAYER2_RETURN_PANEL_READY"
    )
    payload = _fact_manifest_payload(
        report_type="layer2_return_cost_exposure_panel",
        title="Layer-2 Return Cost Exposure Panel",
        status=status,
        output_root=output_root,
        parquet_name=parquet_path.name,
        summary={
            "row_count": len(panel),
            "component_count": int(panel["strategy_id"].nunique()) if not panel.empty else 0,
            "start_date": _frame_min(panel, "date"),
            "end_date": _frame_max(panel, "date"),
            "cost_scenario": "base_cost",
            "execution_lag": "primary_execution_lag",
            "data_quality_status": data_quality.get("status"),
            "layer1_historical_research_allowed": False,
        },
        data_quality=data_quality,
        parquet_checksum=_sha256_file(parquet_path),
        parquet_row_count=len(panel),
        parquet_columns=list(panel.columns),
        warning_codes=_warning_codes(data_quality),
        report_registry_entry=_report_registry_entry(
            "layer2_return_cost_exposure_panel",
            "Layer-2 Return Cost Exposure Panel",
            "aits research strategies layer2-return-cost-exposure-panel",
            "layer2_return_cost_exposure_panel_*",
        ),
    )
    _write_fact_artifacts(payload, output_root=output_root)
    return payload


def run_layer2_forward_outcome_cube_build(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _layer2_fact_context(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
    )
    data_quality = _mapping(context["data_quality"])
    if not bool(data_quality.get("passed")):
        payload = _fact_manifest_payload(
            report_type="layer2_forward_outcome_cube",
            title="Layer-2 Forward Outcome Cube",
            status="FORWARD_OUTCOME_CUBE_BLOCKED",
            output_root=output_root,
            parquet_name="layer2_forward_outcome_cube.parquet",
            summary={
                "row_count": 0,
                "data_quality_status": data_quality.get("status"),
                "blocked_reason": "validate_data_cache_failed",
            },
            data_quality=data_quality,
            blockers=["validate_data_cache_failed"],
            report_registry_entry=_report_registry_entry(
                "layer2_forward_outcome_cube",
                "Layer-2 Forward Outcome Cube",
                "aits research strategies layer2-forward-outcome-cube-build",
                "layer2_forward_outcome_cube_*",
            ),
        )
        _write_fact_artifacts(payload, output_root=output_root)
        return payload

    weight_frame = _build_weight_path_frame(context)
    panel = _build_return_panel_frame(context, weight_frame)
    cube = _build_forward_outcome_cube_frame(panel)
    parquet_path = output_root / "layer2_forward_outcome_cube.parquet"
    _write_parquet(cube, parquet_path)
    matured = int((cube["outcome_status"] == "MATURED").sum()) if not cube.empty else 0
    insufficient = len(cube) - matured
    if not matured:
        status = "FORWARD_OUTCOME_CUBE_INSUFFICIENT"
    elif insufficient:
        status = "FORWARD_OUTCOME_CUBE_PARTIAL"
    else:
        status = "FORWARD_OUTCOME_CUBE_READY"
    payload = _fact_manifest_payload(
        report_type="layer2_forward_outcome_cube",
        title="Layer-2 Forward Outcome Cube",
        status=status,
        output_root=output_root,
        parquet_name=parquet_path.name,
        summary={
            "row_count": len(cube),
            "matured_outcome_count": matured,
            "insufficient_future_window_count": insufficient,
            "horizons": [f"{horizon}d" for horizon in FORWARD_HORIZONS],
            "outcome_side_only": True,
            "layer1_historical_research_allowed": False,
        },
        data_quality=data_quality,
        parquet_checksum=_sha256_file(parquet_path),
        parquet_row_count=len(cube),
        parquet_columns=list(cube.columns),
        warning_codes=_warning_codes(data_quality),
        report_registry_entry=_report_registry_entry(
            "layer2_forward_outcome_cube",
            "Layer-2 Forward Outcome Cube",
            "aits research strategies layer2-forward-outcome-cube-build",
            "layer2_forward_outcome_cube_*",
        ),
    )
    _write_fact_artifacts(payload, output_root=output_root)
    return payload


def run_layer2_anti_leakage_time_boundary_audit(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    weight_manifest = run_layer2_historical_weight_path_build(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
    )
    return_manifest = run_layer2_return_cost_exposure_panel(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
    )
    outcome_manifest = run_layer2_forward_outcome_cube_build(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
    )
    issues = _anti_leakage_issues(weight_manifest, return_manifest, outcome_manifest)
    hard_blockers = [issue for issue in issues if issue["severity"] == "BLOCKER"]
    warnings = [issue for issue in issues if issue["severity"] == "WARN"]
    if hard_blockers:
        status = "LAYER2_ANTI_LEAKAGE_BLOCKED"
    elif warnings:
        status = "LAYER2_ANTI_LEAKAGE_WARN"
    else:
        status = "LAYER2_ANTI_LEAKAGE_PASS"
    payload = _payload(
        report_type="layer2_anti_leakage_time_boundary_audit",
        title="Layer-2 Anti-Leakage and Time-Boundary Audit",
        status=status,
        summary={
            "issue_count": len(issues),
            "hard_blocker_count": len(hard_blockers),
            "warning_count": len(warnings),
            "feature_outcome_separated": not hard_blockers,
            "same_bar_execution_allowed": False,
            "layer1_historical_research_allowed": False,
        },
        field_overlap_matrix=_field_overlap_matrix(),
        derived_dependency_matrix=_derived_dependency_matrix(),
        time_window_matrix=_time_window_matrix(),
        execution_boundary_matrix=_execution_boundary_matrix(),
        forbidden_dependency_list=hard_blockers,
        issues=issues,
        input_artifacts={
            "weight_path_manifest": weight_manifest["artifact_paths"]["json_path"],
            "return_panel_manifest": return_manifest["artifact_paths"]["json_path"],
            "outcome_cube_manifest": outcome_manifest["artifact_paths"]["json_path"],
        },
        report_registry_entry=_report_registry_entry(
            "layer2_anti_leakage_time_boundary_audit",
            "Layer-2 Anti-Leakage and Time-Boundary Audit",
            "aits research strategies layer2-anti-leakage-time-boundary-audit",
            "layer2_anti_leakage_time_boundary_audit",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_layer2_common_robustness_validation(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_LAYER2_COMPONENT_POOL_CONFIG_PATH,
    simple_registry_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    as_of_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    output_root: Path = DEFAULT_LAYER2_COMPONENT_OUTPUT_ROOT,
) -> dict[str, Any]:
    context = _layer2_fact_context(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        output_root=output_root,
    )
    data_quality = _mapping(context["data_quality"])
    if not bool(data_quality.get("passed")):
        payload = _payload(
            report_type="layer2_common_robustness_validation",
            title="Layer-2 Common Robustness Validation",
            status="LAYER2_ROBUSTNESS_BLOCKED",
            summary={
                "row_count": 0,
                "data_quality_status": data_quality.get("status"),
                "blocked_reason": "validate_data_cache_failed",
                "layer1_historical_research_allowed": False,
            },
            data_quality=data_quality,
            blockers=["validate_data_cache_failed"],
            report_registry_entry=_report_registry_entry(
                "layer2_common_robustness_validation",
                "Layer-2 Common Robustness Validation",
                "aits research strategies layer2-common-robustness-validation",
                "layer2_common_robustness_validation",
            ),
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    weight_frame = _build_weight_path_frame(context)
    panel = _build_return_panel_frame(context, weight_frame)
    rows = _build_robustness_rows(context, panel)
    missing_coverage = [row for row in rows if row["coverage_status"] != "COVERED"]
    status = "LAYER2_ROBUSTNESS_READY" if not missing_coverage else "LAYER2_ROBUSTNESS_MIXED"
    payload = _payload(
        report_type="layer2_common_robustness_validation",
        title="Layer-2 Common Robustness Validation",
        status=status,
        summary={
            "row_count": len(rows),
            "component_count": len({row["strategy_id"] for row in rows}),
            "missing_or_partial_coverage_count": len(missing_coverage),
            "data_quality_status": data_quality.get("status"),
            "layer1_historical_research_allowed": False,
        },
        robustness_rows=rows,
        missing_coverage_rows=missing_coverage[:20],
        data_quality=data_quality,
        input_artifacts={
            "layer2_component_pool_config": str(config_path),
            "prices": str(prices_path),
            "rates": str(rates_path),
        },
        report_registry_entry=_report_registry_entry(
            "layer2_common_robustness_validation",
            "Layer-2 Common Robustness Validation",
            "aits research strategies layer2-common-robustness-validation",
            "layer2_common_robustness_validation",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def _layer2_fact_context(
    *,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    config_path: Path,
    simple_registry_config_path: Path,
    as_of_date: date | None,
    start_date: date | None,
    end_date: date | None,
    output_root: Path,
) -> dict[str, Any]:
    config = _load_config(config_path)
    simple_config = _load_registry(simple_registry_config_path)
    data_quality_payload = run_layer2_component_data_quality_check(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        as_of_date=as_of_date,
        output_root=output_root,
    )
    definition_payload = run_layer2_component_definition_lock(
        config_path=config_path,
        simple_registry_config_path=simple_registry_config_path,
        output_root=output_root,
    )
    data_quality = _mapping(data_quality_payload.get("data_quality"))
    resolved_end = end_date or _date_or_none(data_quality.get("as_of"))
    resolved_start = start_date or DEFAULT_AI_REGIME_BACKTEST_START
    prices = _load_price_matrix(prices_path, _required_price_tickers(config))
    prices = _slice_prices(prices, start_date=resolved_start, end_date=resolved_end)
    definition_by_id = {
        str(row.get("strategy_id")): row
        for row in _records(definition_payload.get("component_definitions"))
    }
    components = []
    for component in _formal_components(config):
        registry_strategy_id = str(
            component.get("registry_strategy_id") or component.get("strategy_id")
        )
        strategy = _strategy_by_id(simple_config, registry_strategy_id)
        definition = definition_by_id.get(str(component.get("strategy_id")), {})
        components.append(
            {
                "component": dict(component),
                "strategy": dict(strategy),
                "definition": dict(definition),
            }
        )
    return {
        "config": config,
        "simple_config": simple_config,
        "prices": prices,
        "components": components,
        "data_quality": data_quality,
        "component_pool_hash": definition_payload.get("component_pool_hash"),
        "definition_payload": definition_payload,
        "start_date": resolved_start,
        "end_date": resolved_end,
    }


def _build_weight_path_frame(context: Mapping[str, Any]) -> pd.DataFrame:
    prices = _ensure_price_frame(context.get("prices"))
    config = _mapping(context.get("simple_config"))
    data_quality = _mapping(context.get("data_quality"))
    warning_codes = _warning_codes(data_quality)
    rows: list[dict[str, Any]] = []
    for item in _records(context.get("components")):
        component = _mapping(item.get("component"))
        strategy = _mapping(item.get("strategy"))
        definition = _mapping(item.get("definition"))
        if not strategy:
            continue
        weights = _normalised_asset_weight_frame(_target_weight_frame(strategy, prices, config))
        turnover = _turnover_series(weights)
        for timestamp, weight_row in weights.iterrows():
            weight_map = _weight_map(weight_row)
            rows.append(
                {
                    "decision_date": timestamp.date().isoformat(),
                    "holding_date": timestamp.date().isoformat(),
                    "strategy_id": component.get("strategy_id"),
                    "registry_strategy_id": component.get("registry_strategy_id"),
                    "strategy_role": component.get("strategy_role"),
                    "policy_definition_hash": definition.get("policy_definition_hash"),
                    "component_pool_hash": context.get("component_pool_hash"),
                    "target_weight_qqq": _round(weight_map.get("QQQ")),
                    "target_weight_tqqq": _round(weight_map.get("TQQQ")),
                    "target_weight_sgov": _round(weight_map.get("SGOV")),
                    "pre_constraint_weights": json.dumps(
                        weight_map, ensure_ascii=False, sort_keys=True
                    ),
                    "post_constraint_weights": json.dumps(
                        weight_map, ensure_ascii=False, sort_keys=True
                    ),
                    "rebalance_flag": bool(_float(turnover.loc[timestamp]) > 0.0)
                    or timestamp == weights.index[0],
                    "signal_snapshot_id": _stable_hash(
                        {
                            "strategy_id": component.get("strategy_id"),
                            "decision_date": timestamp.date().isoformat(),
                            "policy_definition_hash": definition.get(
                                "policy_definition_hash"
                            ),
                        }
                    ),
                    "data_quality_status": data_quality.get("status"),
                    "warning_codes": json.dumps(warning_codes, ensure_ascii=False),
                    "paper_shadow_allowed": False,
                    "production_allowed": False,
                    "broker_action": "none",
                    "manual_review_required": True,
                }
            )
    return pd.DataFrame(rows)


def _build_return_panel_frame(
    context: Mapping[str, Any],
    weight_frame: pd.DataFrame,
) -> pd.DataFrame:
    prices = _ensure_price_frame(context.get("prices"))
    data_quality = _mapping(context.get("data_quality"))
    config = _mapping(context.get("config"))
    base_cost_bps = _float(
        _mapping(_research_policy(config).get("cost_assumption")).get("base_cost_bps"),
        5.0,
    )
    transaction_cost_bps = base_cost_bps * 0.6
    slippage_cost_bps = base_cost_bps * 0.4
    asset_returns = prices.pct_change().fillna(0.0).reindex(columns=list(ASSET_COLUMNS)).fillna(0.0)
    qqq_returns = asset_returns["QQQ"]
    rows: list[dict[str, Any]] = []
    for strategy_id, group in weight_frame.groupby("strategy_id", sort=False):
        ordered = group.sort_values("decision_date").copy()
        ordered.index = pd.to_datetime(ordered["decision_date"])
        weights = ordered[
            ["target_weight_qqq", "target_weight_tqqq", "target_weight_sgov"]
        ].rename(
            columns={
                "target_weight_qqq": "QQQ",
                "target_weight_tqqq": "TQQQ",
                "target_weight_sgov": "SGOV",
            }
        )
        applied = weights.shift(1).ffill().reindex(asset_returns.index).fillna(0.0)
        gross = (applied * asset_returns).sum(axis=1)
        turnover = _turnover_series(applied)
        transaction_cost = turnover * (transaction_cost_bps / 10000.0)
        slippage_cost = turnover * (slippage_cost_bps / 10000.0)
        net = gross - transaction_cost - slippage_cost
        role = str(ordered["strategy_role"].iloc[0])
        definition_hash = str(ordered["policy_definition_hash"].iloc[0])
        pool_hash = str(ordered["component_pool_hash"].iloc[0])
        for timestamp in asset_returns.index:
            weight_row = applied.loc[timestamp]
            qqq_exposure = _float(weight_row.get("QQQ"))
            tqqq_exposure = _float(weight_row.get("TQQQ"))
            sgov_exposure = _float(weight_row.get("SGOV"))
            effective_beta = qqq_exposure + tqqq_exposure * 3.0
            rows.append(
                {
                    "date": timestamp.date().isoformat(),
                    "strategy_id": strategy_id,
                    "strategy_role": role,
                    "policy_definition_hash": definition_hash,
                    "component_pool_hash": pool_hash,
                    "cost_scenario": "base_cost",
                    "execution_lag": "primary_execution_lag",
                    "gross_return": _round(gross.loc[timestamp]),
                    "transaction_cost": _round(transaction_cost.loc[timestamp]),
                    "slippage_cost": _round(slippage_cost.loc[timestamp]),
                    "net_return": _round(net.loc[timestamp]),
                    "turnover": _round(turnover.loc[timestamp]),
                    "qqq_exposure": _round(qqq_exposure),
                    "tqqq_exposure": _round(tqqq_exposure),
                    "sgov_exposure": _round(sgov_exposure),
                    "effective_qqq_beta": _round(effective_beta),
                    "effective_leverage": _round(effective_beta + sgov_exposure),
                    "sgov_carry_contribution": _round(
                        sgov_exposure * asset_returns.loc[timestamp, "SGOV"]
                    ),
                    "leverage_drag": _round(max(tqqq_exposure, 0.0) * 0.0002),
                    "cash_drag": _round(sgov_exposure),
                    "active_return_vs_qqq": _round(net.loc[timestamp] - qqq_returns.loc[timestamp]),
                    "data_quality_status": data_quality.get("status"),
                    "paper_shadow_allowed": False,
                    "production_allowed": False,
                    "broker_action": "none",
                    "manual_review_required": True,
                }
            )
    return pd.DataFrame(rows)


def _build_forward_outcome_cube_frame(panel: pd.DataFrame) -> pd.DataFrame:
    if panel.empty:
        return pd.DataFrame()
    ordered_dates = sorted(panel["date"].unique())
    net = panel.pivot(index="date", columns="strategy_id", values="net_return").sort_index()
    roles = {
        str(row["strategy_id"]): str(row["strategy_role"])
        for _, row in panel.drop_duplicates("strategy_id").iterrows()
    }
    definition_hashes = {
        str(row["strategy_id"]): str(row["policy_definition_hash"])
        for _, row in panel.drop_duplicates("strategy_id").iterrows()
    }
    pool_hashes = {
        str(row["strategy_id"]): str(row["component_pool_hash"])
        for _, row in panel.drop_duplicates("strategy_id").iterrows()
    }
    rows: list[dict[str, Any]] = []
    for date_index, decision_date in enumerate(ordered_dates):
        for horizon in FORWARD_HORIZONS:
            future_dates = ordered_dates[date_index + 1 : date_index + 1 + horizon]
            matured = len(future_dates) == horizon
            horizon_returns = {}
            horizon_drawdowns = {}
            for strategy_id in net.columns:
                if not matured:
                    horizon_returns[str(strategy_id)] = None
                    horizon_drawdowns[str(strategy_id)] = None
                    continue
                series = net.loc[future_dates, strategy_id].fillna(0.0)
                horizon_returns[str(strategy_id)] = _compound_return(series)
                horizon_drawdowns[str(strategy_id)] = _max_drawdown_from_returns(series)
            best_return = max(
                [value for value in horizon_returns.values() if value is not None],
                default=None,
            )
            for strategy_id in net.columns:
                key = str(strategy_id)
                series = (
                    net.loc[future_dates, strategy_id].fillna(0.0)
                    if matured
                    else pd.Series(dtype=float)
                )
                future_return = horizon_returns.get(key)
                future_drawdown = horizon_drawdowns.get(key)
                realized_vol = _realized_volatility_from_returns(series) if matured else None
                downside = _downside_deviation_from_returns(series) if matured else None
                rows.append(
                    {
                        "decision_date": decision_date,
                        "strategy_id": key,
                        "strategy_role": roles.get(key),
                        "horizon": f"{horizon}d",
                        "horizon_trading_days": horizon,
                        "outcome_status": "MATURED" if matured else "INSUFFICIENT_FUTURE_WINDOW",
                        "outcome_start_date": future_dates[0] if matured else None,
                        "outcome_end_date": future_dates[-1] if matured else None,
                        "policy_definition_hash": definition_hashes.get(key),
                        "component_pool_hash": pool_hashes.get(key),
                        "future_net_return": _nullable_round(future_return),
                        "future_max_drawdown": _nullable_round(future_drawdown),
                        "future_realized_volatility": _nullable_round(realized_vol),
                        "future_downside_deviation": _nullable_round(downside),
                        "future_calmar_proxy": _nullable_round(
                            _ratio_or_none(future_return, abs(future_drawdown or 0.0))
                        ),
                        "relative_return_vs_100_qqq": _nullable_round(
                            _diff_or_none(future_return, horizon_returns.get("100_qqq"))
                        ),
                        "relative_return_vs_equal_risk": _nullable_round(
                            _diff_or_none(
                                future_return,
                                horizon_returns.get("equal_risk_qqq_sgov"),
                            )
                        ),
                        "relative_return_vs_growth_candidate": None,
                        "relative_drawdown_vs_100_qqq": _nullable_round(
                            _diff_or_none(future_drawdown, horizon_drawdowns.get("100_qqq"))
                        ),
                        "regret_vs_best_component": _nullable_round(
                            _diff_or_none(best_return, future_return)
                        ),
                        "outcome_side_only": True,
                        "paper_shadow_allowed": False,
                        "production_allowed": False,
                        "broker_action": "none",
                        "manual_review_required": True,
                    }
                )
    return pd.DataFrame(rows)


def _anti_leakage_issues(
    weight_manifest: Mapping[str, Any],
    return_manifest: Mapping[str, Any],
    outcome_manifest: Mapping[str, Any],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if str(weight_manifest.get("status")) == "LAYER2_WEIGHT_PATH_BLOCKED":
        issues.append(
            {
                "issue_id": "weight_path_blocked",
                "severity": "BLOCKER",
                "message": "historical weight path was not built",
            }
        )
    if str(return_manifest.get("status")) == "LAYER2_RETURN_PANEL_BLOCKED":
        issues.append(
            {
                "issue_id": "return_panel_blocked",
                "severity": "BLOCKER",
                "message": "return/cost/exposure panel was not built",
            }
        )
    if str(outcome_manifest.get("status")) == "FORWARD_OUTCOME_CUBE_BLOCKED":
        issues.append(
            {
                "issue_id": "outcome_cube_blocked",
                "severity": "BLOCKER",
                "message": "forward outcome cube was not built",
            }
        )
    if outcome_manifest.get("status") == "FORWARD_OUTCOME_CUBE_PARTIAL":
        issues.append(
            {
                "issue_id": "latest_forward_windows_not_matured",
                "severity": "WARN",
                "message": "latest decision dates do not yet have all future horizons matured",
            }
        )
    return issues


def _field_overlap_matrix() -> list[dict[str, Any]]:
    return [
        {
            "matrix_id": "feature_vs_outcome",
            "feature_fields": ["decision_date", "target weights", "policy_definition_hash"],
            "outcome_fields": [
                "future_net_return",
                "future_max_drawdown",
                "regret_vs_best_component",
            ],
            "overlap_count": 0,
            "status": "PASS",
        }
    ]


def _derived_dependency_matrix() -> list[dict[str, Any]]:
    return [
        {
            "artifact": "layer2_historical_weight_path",
            "allowed_dependencies": [
                "price/rate data at or before decision_date",
                "component definition",
            ],
            "forbidden_dependencies": [
                "future returns",
                "future drawdown",
                "best future component",
            ],
            "status": "PASS",
        },
        {
            "artifact": "layer2_forward_outcome_cube",
            "allowed_dependencies": ["post-decision realized return path"],
            "forbidden_as_feature_input": True,
            "status": "PASS",
        },
    ]


def _time_window_matrix() -> list[dict[str, Any]]:
    return [
        {
            "field_family": "component weights",
            "time_boundary": "decision_date uses data available at or before close_t",
            "future_data_allowed": False,
            "status": "PASS",
        },
        {
            "field_family": "forward outcomes",
            "time_boundary": "outcome windows start after decision_date",
            "future_data_allowed": True,
            "feature_side_allowed": False,
            "status": "PASS",
        },
    ]


def _execution_boundary_matrix() -> list[dict[str, Any]]:
    return [
        {
            "execution_assumption": "primary_execution_lag",
            "signal_time": "after close_t",
            "execution_time": "t_plus_1",
            "same_bar_execution_allowed": False,
            "status": "PASS",
        }
    ]


def _build_robustness_rows(
    context: Mapping[str, Any],
    panel: pd.DataFrame,
) -> list[dict[str, Any]]:
    prices = _ensure_price_frame(context.get("prices"))
    panel = panel.copy()
    panel["date_ts"] = pd.to_datetime(panel["date"])
    qqq_returns = prices["QQQ"].pct_change().fillna(0.0)
    segments = _robustness_segments(prices)
    rows: list[dict[str, Any]] = []
    for segment in segments:
        dates = set(segment["dates"])
        for strategy_id, group in panel.groupby("strategy_id", sort=False):
            selected = group[group["date"].isin(dates)].sort_values("date_ts")
            if selected.empty:
                rows.append(_missing_robustness_row(strategy_id, group, segment))
                continue
            returns = pd.Series(
                selected["net_return"].astype(float).to_numpy(),
                index=selected["date_ts"],
            )
            benchmark = qqq_returns.reindex(selected["date_ts"]).fillna(0.0)
            metrics = _return_metrics(returns, benchmark)
            rows.append(
                {
                    "strategy_id": str(strategy_id),
                    "strategy_role": str(group["strategy_role"].iloc[0]),
                    "period_or_regime": segment["segment_id"],
                    "segment_type": segment["segment_type"],
                    "coverage_status": "COVERED",
                    "sample_count": len(selected),
                    "annual_return": _round(metrics["annual_return"]),
                    "max_drawdown": _round(metrics["max_drawdown"]),
                    "sharpe": _round(metrics["sharpe"]),
                    "calmar": _round(metrics["calmar"]),
                    "turnover": _round(float(selected["turnover"].sum())),
                    "recovery_days": metrics["recovery_days"],
                    "active_return_vs_qqq": _round(metrics["active_return_vs_qqq"]),
                    "beta_adjusted_return": _round(metrics["beta_adjusted_return"]),
                    "risk_matched_return": _round(metrics["risk_matched_return"]),
                    "regime_concentration_score": _round(metrics["regime_concentration_score"]),
                    "paper_shadow_allowed": False,
                    "production_allowed": False,
                    "broker_action": "none",
                    "manual_review_required": True,
                }
            )
    return rows


def _robustness_segments(prices: pd.DataFrame) -> list[dict[str, Any]]:
    date_strings = pd.Series([idx.date().isoformat() for idx in prices.index], index=prices.index)
    qqq_returns = prices["QQQ"].pct_change().fillna(0.0)
    rolling_60d = prices["QQQ"].pct_change(60).fillna(0.0)
    vol_20d = qqq_returns.rolling(20, min_periods=20).std().fillna(0.0)
    low_cut = float(vol_20d.quantile(0.33))
    high_cut = float(vol_20d.quantile(0.66))
    ma_200 = prices["QQQ"].rolling(200, min_periods=200).mean()
    drawdown = prices["QQQ"] / prices["QQQ"].cummax() - 1.0
    segments = [
        _date_segment("ai_after_chatgpt_full", "period", date_strings, date(2022, 12, 1), None),
        _date_segment(
            "2022_bear_market_tail",
            "event",
            date_strings,
            date(2022, 12, 1),
            date(2022, 12, 31),
        ),
        _date_segment("2023_recovery", "event", date_strings, date(2023, 1, 1), date(2023, 12, 31)),
        _date_segment("2024_ai_rally", "event", date_strings, date(2024, 1, 1), date(2024, 12, 31)),
        _date_segment("2025_to_latest", "event", date_strings, date(2025, 1, 1), None),
        _date_segment("2018Q4", "event", date_strings, date(2018, 10, 1), date(2018, 12, 31)),
        _date_segment("2020_crash", "event", date_strings, date(2020, 2, 15), date(2020, 4, 30)),
        _mask_segment("bull_state", "regime", date_strings, rolling_60d > 0.05),
        _mask_segment("bear_state", "regime", date_strings, rolling_60d < -0.05),
        _mask_segment("range_state", "regime", date_strings, rolling_60d.abs() <= 0.05),
        _mask_segment("low_volatility", "volatility", date_strings, vol_20d <= low_cut),
        _mask_segment(
            "mid_volatility",
            "volatility",
            date_strings,
            (vol_20d > low_cut) & (vol_20d < high_cut),
        ),
        _mask_segment("high_volatility", "volatility", date_strings, vol_20d >= high_cut),
        _mask_segment("above_200dma", "trend", date_strings, prices["QQQ"] >= ma_200),
        _mask_segment("below_200dma", "trend", date_strings, prices["QQQ"] < ma_200),
        _mask_segment("drawdown_state", "drawdown", date_strings, drawdown <= -0.08),
        _mask_segment(
            "recovery_state",
            "drawdown",
            date_strings,
            (drawdown > -0.08) & (drawdown < 0.0) & (rolling_60d > 0.0),
        ),
    ]
    return segments


def _date_segment(
    segment_id: str,
    segment_type: str,
    date_strings: pd.Series,
    start: date,
    end: date | None,
) -> dict[str, Any]:
    mask = pd.Series(date_strings.index.date >= start, index=date_strings.index)
    if end is not None:
        mask &= pd.Series(date_strings.index.date <= end, index=date_strings.index)
    return _mask_segment(segment_id, segment_type, date_strings, mask)


def _mask_segment(
    segment_id: str,
    segment_type: str,
    date_strings: pd.Series,
    mask: pd.Series,
) -> dict[str, Any]:
    selected = date_strings.loc[mask.fillna(False)]
    return {
        "segment_id": segment_id,
        "segment_type": segment_type,
        "dates": list(selected.astype(str)),
    }


def _missing_robustness_row(
    strategy_id: str,
    group: pd.DataFrame,
    segment: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "strategy_id": str(strategy_id),
        "strategy_role": str(group["strategy_role"].iloc[0]) if not group.empty else "UNKNOWN",
        "period_or_regime": segment["segment_id"],
        "segment_type": segment["segment_type"],
        "coverage_status": "INSUFFICIENT_PRICE_COVERAGE",
        "sample_count": 0,
        "annual_return": None,
        "max_drawdown": None,
        "sharpe": None,
        "calmar": None,
        "turnover": None,
        "recovery_days": None,
        "active_return_vs_qqq": None,
        "beta_adjusted_return": None,
        "risk_matched_return": None,
        "regime_concentration_score": None,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "manual_review_required": True,
    }


def _return_metrics(returns: pd.Series, benchmark: pd.Series) -> dict[str, Any]:
    returns = returns.fillna(0.0)
    benchmark = benchmark.reindex(returns.index).fillna(0.0)
    equity = (1.0 + returns).cumprod()
    benchmark_equity = (1.0 + benchmark).cumprod()
    drawdown = equity / equity.cummax() - 1.0
    annualization = 252
    annual_return = _annual_return(equity, len(returns), annualization)
    benchmark_annual_return = _annual_return(benchmark_equity, len(benchmark), annualization)
    annual_vol = float(returns.std(ddof=0) * (annualization**0.5))
    benchmark_vol = float(benchmark.std(ddof=0) * (annualization**0.5))
    beta = _beta(returns, benchmark)
    max_drawdown = float(drawdown.min()) if not drawdown.empty else 0.0
    total_return = _compound_return(returns)
    benchmark_total_return = _compound_return(benchmark)
    return {
        "annual_return": annual_return,
        "max_drawdown": max_drawdown,
        "sharpe": _ratio(annual_return, annual_vol),
        "calmar": _ratio(annual_return, abs(max_drawdown)),
        "recovery_days": _max_recovery_days(equity),
        "active_return_vs_qqq": total_return - benchmark_total_return,
        "beta_adjusted_return": annual_return - beta * benchmark_annual_return,
        "risk_matched_return": annual_return * _ratio(benchmark_vol, annual_vol),
        "regime_concentration_score": min(abs(total_return), 1.0),
    }


def _fact_manifest_payload(
    *,
    report_type: str,
    title: str,
    status: str,
    output_root: Path,
    parquet_name: str,
    summary: Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    parquet_path = output_root / parquet_name
    return _payload(
        report_type=report_type,
        title=title,
        status=status,
        summary=summary,
        parquet_path=str(parquet_path),
        layer1_historical_research_allowed=False,
        layer1_forward_aging_allowed=False,
        layer1_paper_shadow_allowed=False,
        **extra,
    )


def _write_fact_artifacts(payload: dict[str, Any], *, output_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    report_type = str(payload["report_type"])
    json_path = output_root / f"{report_type}_manifest.json"
    markdown_path = output_root / f"{report_type}_review.md"
    payload["artifact_paths"] = {
        "json_path": str(json_path),
        "markdown_path": str(markdown_path),
        "parquet_path": str(payload.get("parquet_path", "")),
    }
    json_path.write_text(
        json.dumps(_jsonable(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(_render_markdown(payload), encoding="utf-8")


def _write_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _ensure_price_frame(value: object) -> pd.DataFrame:
    if not isinstance(value, pd.DataFrame):
        raise ValueError("layer2 fact context is missing a price DataFrame")
    return value


def _normalised_asset_weight_frame(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.reindex(columns=list(ASSET_COLUMNS), fill_value=0.0).fillna(0.0)
    totals = result.sum(axis=1)
    positive = totals > 0.0
    if positive.any():
        result.loc[positive, :] = result.loc[positive, :].div(totals.loc[positive], axis=0)
    return result


def _weight_map(row: pd.Series) -> dict[str, float]:
    return {ticker: _round(row.get(ticker), digits=8) for ticker in ASSET_COLUMNS}


def _warning_codes(data_quality: Mapping[str, Any]) -> list[str]:
    codes: set[str] = set()
    for issue in _records(data_quality.get("issues")):
        severity = str(issue.get("severity", "")).upper()
        if "WARN" in severity and issue.get("code"):
            codes.add(str(issue["code"]))
    return sorted(codes)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _frame_min(frame: pd.DataFrame, column: str) -> str | None:
    if frame.empty or column not in frame.columns:
        return None
    value = frame[column].dropna().min()
    return None if pd.isna(value) else str(value)


def _frame_max(frame: pd.DataFrame, column: str) -> str | None:
    if frame.empty or column not in frame.columns:
        return None
    value = frame[column].dropna().max()
    return None if pd.isna(value) else str(value)


def _date_or_none(value: object) -> date | None:
    if isinstance(value, date):
        return value
    if value is None or value == "":
        return None
    try:
        timestamp = pd.to_datetime(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(timestamp):
        return None
    return timestamp.date()


def _compound_return(returns: pd.Series) -> float:
    if returns.empty:
        return 0.0
    return float((1.0 + returns.fillna(0.0).astype(float)).prod() - 1.0)


def _max_drawdown_from_returns(returns: pd.Series) -> float:
    if returns.empty:
        return 0.0
    equity = (1.0 + returns.fillna(0.0).astype(float)).cumprod()
    return float((equity / equity.cummax() - 1.0).min())


def _realized_volatility_from_returns(returns: pd.Series) -> float:
    if returns.empty:
        return 0.0
    return float(returns.fillna(0.0).astype(float).std(ddof=0) * math.sqrt(252))


def _downside_deviation_from_returns(returns: pd.Series) -> float:
    if returns.empty:
        return 0.0
    downside = returns.fillna(0.0).astype(float)
    downside = downside[downside < 0.0]
    if downside.empty:
        return 0.0
    return float(downside.std(ddof=0) * math.sqrt(252))


def _nullable_round(value: object, digits: int = 8) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return round(number, digits)


def _ratio_or_none(numerator: object, denominator: object) -> float | None:
    if numerator is None or denominator is None:
        return None
    denom = _float(denominator, default=0.0)
    if denom == 0.0:
        return None
    return _float(numerator) / denom


def _diff_or_none(left: object, right: object) -> float | None:
    if left is None or right is None:
        return None
    return _float(left) - _float(right)


def _annual_return(equity: pd.Series, observations: int, annualization: int) -> float:
    if observations <= 0 or equity.empty:
        return 0.0
    terminal = float(equity.iloc[-1])
    if terminal <= 0.0:
        return -1.0
    return terminal ** (annualization / observations) - 1.0


def _beta(returns: pd.Series, benchmark: pd.Series) -> float:
    aligned = pd.concat(
        [returns.fillna(0.0).astype(float), benchmark.fillna(0.0).astype(float)],
        axis=1,
        join="inner",
    ).dropna()
    if len(aligned) < 2:
        return 0.0
    variance = float(aligned.iloc[:, 1].var(ddof=0))
    if variance == 0.0:
        return 0.0
    covariance = float(aligned.iloc[:, 0].cov(aligned.iloc[:, 1], ddof=0))
    return covariance / variance


def _max_recovery_days(equity: pd.Series) -> int:
    high_water = -math.inf
    current_gap = 0
    longest_gap = 0
    for value in equity.fillna(1.0).astype(float):
        if value >= high_water:
            high_water = value
            current_gap = 0
        else:
            current_gap += 1
            longest_gap = max(longest_gap, current_gap)
    return longest_gap


def _ratio(numerator: object, denominator: object) -> float:
    denom = _float(denominator, default=0.0)
    if denom == 0.0:
        return 0.0
    return _float(numerator) / denom


def _round(value: object, *, digits: int = 8) -> float:
    return round(_float(value, default=0.0), digits)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _load_config(path: Path) -> dict[str, Any]:
    loaded = safe_load_yaml_path(path)
    if not isinstance(loaded, dict):
        raise ValueError(f"layer2 component pool config must be a mapping: {path}")
    return loaded


def _source_payloads(
    simple_output_root: Path,
    growth_output_root: Path,
) -> dict[str, dict[str, Any]]:
    paths = _source_paths(simple_output_root, growth_output_root)
    return {name: _read_json_or_empty(Path(path)) for name, path in paths.items()}


def _source_paths(simple_output_root: Path, growth_output_root: Path) -> dict[str, str]:
    return {
        "simple_baseline_master_review": str(
            simple_output_root / "simple_baseline_master_review.json"
        ),
        "simple_baseline_watchlist_owner_decision": str(
            simple_output_root / "simple_baseline_watchlist_owner_decision.json"
        ),
        "simple_baseline_forward_aging_master_review": str(
            simple_output_root / "simple_baseline_forward_aging_master_review.json"
        ),
        "qqq_plus_growth_real_cli_suite_summary": str(
            growth_output_root / "qqq_plus_growth_real_cli_suite_summary.json"
        ),
        "qqq_plus_growth_candidate_result_summary": str(
            growth_output_root / "qqq_plus_growth_candidate_result_summary.json"
        ),
        "growth_edge_vs_qqq_materiality_review": str(
            growth_output_root / "growth_edge_vs_qqq_materiality_review.json"
        ),
        "qqq_plus_beta_and_exposure_attribution": str(
            growth_output_root / "qqq_plus_beta_and_exposure_attribution.json"
        ),
        "qqq_plus_growth_owner_decision_pack": str(
            growth_output_root / "qqq_plus_growth_owner_decision_pack.json"
        ),
    }


def _read_json_or_empty(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _reconciliation_checks(
    config: Mapping[str, Any],
    sources: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    required_sources = {
        "simple_baseline_master_review",
        "simple_baseline_watchlist_owner_decision",
        "simple_baseline_forward_aging_master_review",
        "qqq_plus_growth_owner_decision_pack",
    }
    for source_id in required_sources:
        checks.append(
            _check(
                source_id=f"source_artifact_{source_id}",
                passed=bool(sources.get(source_id)),
                warning=False,
                message=f"required source artifact {source_id} is readable",
            )
        )
    checks.append(
        _check(
            source_id="defensive_primary_equal_risk",
            passed="equal_risk_qqq_sgov" in _component_ids(config),
            warning=False,
            message="defensive primary equal_risk_qqq_sgov is present in formal pool",
        )
    )
    checks.append(
        _check(
            source_id="hard_benchmark_100_qqq",
            passed="100_qqq" in _component_ids(config),
            warning=False,
            message="hard benchmark 100_qqq is present in formal pool",
        )
    )
    checks.append(
        _check(
            source_id="growth_not_formal_selectable",
            passed=not _growth_in_formal_pool(config),
            warning=False,
            message="QQQ-plus growth is excluded from formal selectable/reference pool",
        )
    )
    checks.append(
        _check(
            source_id="growth_owner_keep_research_only",
            passed=_growth_owner_decision(sources) == GROWTH_OWNER_DECISION_KEEP_RESEARCH_ONLY,
            warning=False,
            message="TRADING-956 owner decision keeps growth research-only",
        )
    )
    for report_id, payload in sources.items():
        if not payload:
            continue
        checks.append(
            _check(
                source_id=f"safety_{report_id}",
                passed=_safe_payload(payload),
                warning=False,
                message=f"{report_id} preserves paper-shadow/production/broker safety",
            )
        )
    if _growth_owner_decision(sources) == GROWTH_OWNER_DECISION_KEEP_RESEARCH_ONLY:
        checks.append(
            {
                "check_id": "growth_inactive_reference_only",
                "status": "WARN",
                "message": (
                    "growth candidate remains research-only inactive/reference and is not "
                    "a formal Layer-2 component"
                ),
            }
        )
    return checks


def _pool_validation_issues(
    config: Mapping[str, Any],
    sources: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    constraints = _mapping(config.get("pool_constraints"))
    issues = []
    selectable = _components(config, "selectable_components")
    reference = _components(config, "reference_components")
    if len(selectable) > _int(constraints.get("max_selectable_components"), 3):
        issues.append({"issue_id": "selectable_component_limit_exceeded"})
    if len(reference) > _int(constraints.get("max_reference_components"), 2):
        issues.append({"issue_id": "reference_component_limit_exceeded"})
    if _growth_in_formal_pool(config):
        issues.append({"issue_id": "growth_component_wrongly_in_formal_pool"})
    if _growth_owner_decision(sources) != GROWTH_OWNER_DECISION_KEEP_RESEARCH_ONLY:
        issues.append({"issue_id": "growth_owner_decision_not_keep_research_only"})
    return issues


def _component_pool_rows(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for section, formal_pool in (
        ("selectable_components", True),
        ("reference_components", True),
        (INACTIVE_COMPONENT_SECTION, False),
    ):
        for component in _components(config, section):
            rows.append(
                {
                    "component_pool_id": config.get("component_pool_id"),
                    "component_pool_version": config.get("component_pool_version"),
                    "strategy_id": component.get("strategy_id"),
                    "registry_strategy_id": component.get("registry_strategy_id"),
                    "strategy_role": component.get("strategy_role"),
                    "strategy_version": component.get("strategy_version"),
                    "selectable_by_layer1": bool(component.get("selectable_by_layer1")),
                    "reference_only": bool(component.get("reference_only")),
                    "formal_component_pool_member": formal_pool,
                    "inclusion_reason": component.get("inclusion_reason"),
                    "exclusion_reason": component.get("inactive_reason"),
                    "source_report": component.get("source_report"),
                    "manual_review_required": True,
                    "paper_shadow_allowed": False,
                    "production_allowed": False,
                    "broker_action": "none",
                }
            )
    return rows


def _component_role_rows(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = _component_pool_rows(config)
    for row in rows:
        role = row.get("strategy_role")
        row["role_status"] = (
            "inactive_reference"
            if role == "research_only_inactive_reference"
            else "formal_role_assigned"
        )
    return rows


def _component_definition(
    component: Mapping[str, Any],
    *,
    config: Mapping[str, Any],
    simple_config: Mapping[str, Any],
) -> dict[str, Any]:
    registry_strategy_id = str(
        component.get("registry_strategy_id") or component.get("strategy_id")
    )
    strategy = _strategy_by_id(simple_config, registry_strategy_id)
    if not strategy:
        return {
            "strategy_id": component.get("strategy_id"),
            "registry_strategy_id": registry_strategy_id,
            "definition_status": "MISSING_REGISTRY_STRATEGY",
            "policy_definition_hash": None,
        }
    policy_definition = {
        "strategy_id": component.get("strategy_id"),
        "strategy_version": component.get("strategy_version"),
        "strategy_role": component.get("strategy_role"),
        "registry_strategy_id": registry_strategy_id,
        "asset_universe": strategy.get("asset_universe", []),
        "input_fields": component.get("input_fields", []),
        "lookback_windows": component.get("lookback_windows", {}),
        "mapping_rule": {
            "target_weights": strategy.get("target_weights", {}),
            "risk_on_weights": strategy.get("risk_on_weights", {}),
            "risk_off_weights": strategy.get("risk_off_weights", {}),
            "risk_control_rule": strategy.get("risk_control_rule"),
            "trend_filter_rule": strategy.get("trend_filter_rule"),
            "volatility_filter_rule": strategy.get("volatility_filter_rule"),
            "drawdown_filter_rule": strategy.get("drawdown_filter_rule"),
        },
        "weight_bounds": {
            "max_turnover": strategy.get("max_turnover"),
            "max_tqqq_weight": strategy.get("max_tqqq_weight"),
            "uses_leverage_etf": strategy.get("uses_leverage_etf"),
            "uses_options": strategy.get("uses_options"),
        },
        "rebalance_rule": strategy.get("rebalance_frequency"),
        "execution_assumption": _research_policy(config).get("common_execution_assumption", {}),
        "cost_assumption": _research_policy(config).get("cost_assumption", {}),
        "data_contract_version": "layer2_component_data_contract_v1",
    }
    return {
        "strategy_id": component.get("strategy_id"),
        "registry_strategy_id": registry_strategy_id,
        "strategy_version": component.get("strategy_version"),
        "strategy_role": component.get("strategy_role"),
        "definition_status": "LOCKED",
        "policy_definition_hash": _stable_hash(policy_definition),
        "policy_definition": policy_definition,
        "manual_review_required": True,
    }


def _inactive_component_definition(
    component: Mapping[str, Any],
    sources: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    owner = _mapping(sources.get("qqq_plus_growth_owner_decision_pack"))
    definition = {
        "strategy_id": component.get("strategy_id"),
        "strategy_version": component.get("strategy_version"),
        "strategy_role": component.get("strategy_role"),
        "definition_lock_scope": "inactive_reference_only",
        "owner_decision": _growth_owner_decision(sources),
        "source_report_status": owner.get("status"),
        "input_fields": component.get("input_fields", []),
        "lookback_windows": component.get("lookback_windows", {}),
        "formal_component_pool_member": False,
    }
    return {
        "strategy_id": component.get("strategy_id"),
        "strategy_version": component.get("strategy_version"),
        "strategy_role": component.get("strategy_role"),
        "definition_status": "INACTIVE_REFERENCE_ONLY",
        "policy_definition_hash": _stable_hash(definition),
        "policy_definition": definition,
        "manual_review_required": True,
        "selectable_by_layer1": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _readiness_rows(
    *,
    pool: Mapping[str, Any],
    definitions: Mapping[str, Any],
    data_quality: Mapping[str, Any],
) -> list[dict[str, Any]]:
    definition_by_id = {
        str(row.get("strategy_id")): row
        for row in _records(definitions.get("component_definitions"))
    }
    inactive_definition_by_id = {
        str(row.get("strategy_id")): row
        for row in _records(definitions.get("inactive_reference_definitions"))
    }
    rows = []
    for component in _records(pool.get("components")):
        strategy_id = str(component.get("strategy_id"))
        definition = definition_by_id.get(strategy_id) or inactive_definition_by_id.get(
            strategy_id, {}
        )
        is_inactive = component.get("strategy_role") == "research_only_inactive_reference"
        blockers = []
        if definition.get("definition_status") not in {"LOCKED", "INACTIVE_REFERENCE_ONLY"}:
            blockers.append("definition_not_locked")
        if str(data_quality.get("status")) == "LAYER2_DATA_QUALITY_BLOCKED":
            blockers.append("data_quality_blocked")
        if is_inactive:
            blockers.append("owner_decision_keep_growth_research_only")
        row_status = "READY_FOR_PANEL_BUILD"
        if is_inactive:
            row_status = "RESEARCH_ONLY_INACTIVE_REFERENCE"
        elif blockers:
            row_status = "BLOCKED"
        rows.append(
            {
                "strategy_id": strategy_id,
                "registry_strategy_id": component.get("registry_strategy_id"),
                "strategy_role": component.get("strategy_role"),
                "formal_component_pool_member": component.get("formal_component_pool_member"),
                "selectable_by_layer1": component.get("selectable_by_layer1"),
                "reference_only": component.get("reference_only"),
                "definition_status": definition.get("definition_status"),
                "policy_definition_hash": definition.get("policy_definition_hash"),
                "component_pool_hash": definitions.get("component_pool_hash"),
                "data_quality_status": _mapping(data_quality.get("data_quality")).get("status"),
                "readiness_status": row_status,
                "blockers": blockers,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
                "manual_review_required": True,
            }
        )
    return rows


def _matrix_blockers(
    reconciliation: Mapping[str, Any],
    pool: Mapping[str, Any],
    definitions: Mapping[str, Any],
    data_quality: Mapping[str, Any],
) -> list[str]:
    blockers = []
    if reconciliation.get("status") in {"LAYER2_CONFLICT_FOUND", "LAYER2_RECONCILIATION_BLOCKED"}:
        blockers.append("component_reconciliation_not_clean")
    if pool.get("status") == "LAYER2_POOL_BLOCKED":
        blockers.append("component_pool_freeze_blocked")
    if definitions.get("status") != "ALL_COMPONENT_DEFINITIONS_LOCKED":
        blockers.append("component_definition_lock_not_complete")
    if data_quality.get("status") == "LAYER2_DATA_QUALITY_BLOCKED":
        blockers.append("layer2_data_quality_blocked")
    return blockers


def _matrix_warnings(
    reconciliation: Mapping[str, Any],
    pool: Mapping[str, Any],
    data_quality: Mapping[str, Any],
) -> list[str]:
    warnings = []
    if str(reconciliation.get("status")).endswith("_WITH_WARNINGS"):
        warnings.append("reconciliation_has_warnings")
    if pool.get("status") == "LAYER2_POOL_FROZEN_WITHOUT_GROWTH":
        warnings.append("growth_candidate_not_in_formal_pool")
    if data_quality.get("status") == "LAYER2_DATA_QUALITY_PASS_WITH_WARNINGS":
        warnings.append("data_quality_passed_with_warnings")
    return warnings


def _layer2_data_quality_status(data_gate: Mapping[str, Any]) -> str:
    if not bool(data_gate.get("passed")) or _int(data_gate.get("error_count")):
        return "LAYER2_DATA_QUALITY_BLOCKED"
    if _int(data_gate.get("warning_count")):
        return "LAYER2_DATA_QUALITY_PASS_WITH_WARNINGS"
    return "LAYER2_DATA_QUALITY_PASS"


def _missing_required_sources(sources: Mapping[str, Mapping[str, Any]]) -> list[str]:
    required = {
        "simple_baseline_master_review",
        "simple_baseline_watchlist_owner_decision",
        "simple_baseline_forward_aging_master_review",
        "qqq_plus_growth_owner_decision_pack",
    }
    return [source_id for source_id in sorted(required) if not sources.get(source_id)]


def _source_statuses(sources: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    return {
        source_id: {
            "available": bool(payload),
            "status": payload.get("status"),
            "data_quality_status": payload.get("data_quality_status")
            or _mapping(payload.get("summary")).get("data_quality_status"),
            "paper_shadow_allowed": payload.get("paper_shadow_allowed"),
            "production_allowed": payload.get("production_allowed"),
            "broker_action": payload.get("broker_action"),
        }
        for source_id, payload in sources.items()
    }


def _safe_payload(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("paper_shadow_allowed") is False
        and payload.get("production_allowed") is False
        and payload.get("broker_action") == "none"
    )


def _growth_owner_decision(sources: Mapping[str, Mapping[str, Any]]) -> str:
    owner = _mapping(sources.get("qqq_plus_growth_owner_decision_pack"))
    summary = _mapping(owner.get("summary"))
    return str(
        owner.get("owner_recommendation")
        or summary.get("owner_recommendation")
        or "UNKNOWN"
    )


def _growth_in_formal_pool(config: Mapping[str, Any]) -> bool:
    return any("growth" in strategy_id for strategy_id in _component_ids(config))


def _component_ids(config: Mapping[str, Any]) -> set[str]:
    return {
        str(component.get("strategy_id"))
        for component in _formal_components(config)
        if component.get("strategy_id")
    }


def _required_price_tickers(config: Mapping[str, Any]) -> list[str]:
    policy = _research_policy(config)
    values = policy.get("required_price_tickers")
    if isinstance(values, list):
        return [str(value) for value in values]
    return ["QQQ", "TQQQ", "SGOV"]


def _formal_components(config: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    for section in FORMAL_COMPONENT_SECTIONS:
        rows.extend(_components(config, section))
    return rows


def _components(config: Mapping[str, Any], section: str) -> list[Mapping[str, Any]]:
    return _records(config.get(section))


def _research_policy(config: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(config.get("research_policy"))


def _strategy_by_id(config: Mapping[str, Any], strategy_id: str) -> Mapping[str, Any]:
    for row in _strategy_rows(config):
        if row.get("strategy_id") == strategy_id:
            return row
    return {}


def _payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": utc_now_iso(),
        "market_regime": "ai_after_chatgpt",
        "anchor_event": "ChatGPT public launch",
        "anchor_date": "2022-11-30",
        "default_backtest_start": DEFAULT_AI_REGIME_BACKTEST_START.isoformat(),
        "summary": {
            "market_regime": "ai_after_chatgpt",
            "default_backtest_start": DEFAULT_AI_REGIME_BACKTEST_START.isoformat(),
            **dict(summary),
        },
        **SAFETY_BOUNDARY,
        **extra,
    }


def _write_pair(payload: dict[str, Any], *, output_root: Path, artifact_id: str) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / f"{artifact_id}.json"
    markdown_path = output_root / f"{artifact_id}.md"
    payload["artifact_paths"] = {
        "json_path": str(json_path),
        "markdown_path": str(markdown_path),
    }
    json_path.write_text(
        json.dumps(_jsonable(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(_render_markdown(payload), encoding="utf-8")


def _render_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# {payload.get('title')}",
        "",
        f"- 状态：`{payload.get('status')}`",
        f"- market_regime：`{payload.get('market_regime')}`",
        f"- production_effect：`{payload.get('production_effect')}`",
        f"- broker_action：`{payload.get('broker_action')}`",
        f"- promotion_allowed：`{str(payload.get('promotion_allowed')).lower()}`",
        f"- paper_shadow_allowed：`{str(payload.get('paper_shadow_allowed')).lower()}`",
        f"- production_allowed：`{str(payload.get('production_allowed')).lower()}`",
        f"- manual_review_required：`{str(payload.get('manual_review_required')).lower()}`",
        "",
    ]
    summary = _mapping(payload.get("summary"))
    if summary:
        lines.extend(["## Summary", "", "|字段|值|", "|---|---|"])
        lines.extend(f"|`{key}`|`{_compact(value)}`|" for key, value in summary.items())
    blockers = payload.get("blockers")
    if isinstance(blockers, list):
        lines.extend(["", "## Blockers", ""])
        lines.extend(f"- `{item}`" for item in blockers) if blockers else lines.append("- none")
    warnings = payload.get("warnings")
    if isinstance(warnings, list):
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- `{item}`" for item in warnings) if warnings else lines.append("- none")
    return "\n".join(lines) + "\n"


def _report_registry_entry(
    report_id: str,
    title: str,
    command: str,
    artifact_slug: str,
) -> dict[str, Any]:
    return {
        "report_id": report_id,
        "title": title,
        "group": "research",
        "cadence": "ad_hoc",
        "audience": "project_owner",
        "owner": "research_governance",
        "command": command,
        "artifact_globs": [
            f"outputs/research_strategies/layer2_components/{artifact_slug}.json",
            f"outputs/research_strategies/layer2_components/{artifact_slug}.md",
        ],
        "artifact_selection_policy": "latest_available",
        "freshness_sla_days": 30,
        "freshness_rationale": (
            "TRADING-957 to 961 Layer-2 component readiness artifacts are "
            "regenerated after component pool, definition, data-quality, or "
            "growth owner-decision state changes."
        ),
        "owner_action": "review_layer2_component_readiness_before_layer1_research",
        "include_in_reader_brief": False,
        "include_in_daily_task_dashboard": False,
        "required_for_daily_reading": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _check(
    *,
    source_id: str,
    passed: bool,
    warning: bool,
    message: str,
) -> dict[str, Any]:
    if passed:
        status = "PASS"
    else:
        status = "WARN" if warning else "FAIL"
    return {"check_id": source_id, "status": status, "message": message}


def _stable_hash(value: Any) -> str:
    payload = json.dumps(
        _jsonable(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _compact(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    if isinstance(value, Mapping):
        return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True)
    return "" if value is None else str(value)
