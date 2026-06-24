from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

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
    _load_registry,
    _records,
    _required_rate_series,
    _strategy_rows,
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
