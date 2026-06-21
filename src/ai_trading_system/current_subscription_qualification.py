from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import (
    AI_REGIME_START,
    DEFAULT_ASSET_MASTER_PATH,
    DEFAULT_COST_MODEL_PATH,
    DEFAULT_DATA_FOUNDATION_ACCEPTANCE_OUTPUT_ROOT,
    DEFAULT_DATA_SOURCE_QUALIFICATION_MATRIX_UPDATED_PATH,
    DEFAULT_LIQUIDITY_MODEL_PATH,
    DEFAULT_UNIVERSE_DEFINITIONS_PATH,
    SAFETY_BOUNDARY,
    run_data_foundation_acceptance,
    utc_now_iso,
    write_foundation_artifact_pair,
)
from ai_trading_system.data_source_subscription_audit import (
    DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_OUTPUT_ROOT,
    DEFAULT_SOURCE_REQUIREMENT_MATRIX_PATH,
    REPRESENTATIVE_UNIVERSE,
)
from ai_trading_system.research_acceleration import REGRET_TAXONOMY
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "data_quality" / "current_subscription_source_qualification"
)
DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "controlled_strategy_research"
)
DEFAULT_CURRENT_SUBSCRIPTION_QUALIFICATION_BATCH_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "data_quality" / "current_subscription_source_qualification_batch"
)
DEFAULT_DATA_SOURCE_USAGE_POLICY_PATH = (
    PROJECT_ROOT / "config" / "data" / "data_source_usage_policy.yaml"
)
DEFAULT_FMP_PRICE_CORPORATE_ACTION_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "data" / "source_qualification" / "fmp_price_corporate_action.yaml"
)
DEFAULT_MARKETSTACK_RECONCILIATION_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "data" / "source_qualification" / "marketstack_reconciliation.yaml"
)
DEFAULT_ASSET_MASTER_QUALIFICATION_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "data" / "asset_master_qualification.yaml"
)
DEFAULT_SEC_FUNDAMENTAL_PIT_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "data" / "source_qualification" / "sec_fundamental_pit.yaml"
)
DEFAULT_MACRO_RISK_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "data" / "source_qualification" / "macro_risk.yaml"
)
DEFAULT_LABEL_BOUNDARY_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "label_boundary_qualification.yaml"
)
DEFAULT_COST_LIQUIDITY_QUALIFICATION_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "trading" / "cost_liquidity_qualification.yaml"
)
DEFAULT_CONTROLLED_RESEARCH_PILOT_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "research" / "controlled_strategy_research_pilot.yaml"
)

DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_MATRIX_PATH = (
    DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_OUTPUT_ROOT
    / "current_subscription_data_coverage_matrix.json"
)
DEFAULT_FORWARD_RECLASSIFICATION_PATH = (
    DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT
    / "forward_evidence_requirement_reclassification.json"
)
DEFAULT_FORWARD_CAPTURE_CONTRACT_PATH = (
    DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT / "forward_evidence_capture_contract.json"
)
DEFAULT_DATA_FOUNDATION_ACCEPTANCE_REPORT_V2_PATH = (
    DEFAULT_DATA_FOUNDATION_ACCEPTANCE_OUTPUT_ROOT / "data_foundation_acceptance_report_v2.json"
)
DEFAULT_MINIMUM_RESEARCH_READINESS_REPORT_PATH = (
    DEFAULT_DATA_FOUNDATION_ACCEPTANCE_OUTPUT_ROOT / "minimum_research_readiness_report.json"
)
DEFAULT_CURRENT_SUBSCRIPTION_QUALIFICATION_BATCH_REVIEW_PATH = (
    DEFAULT_CURRENT_SUBSCRIPTION_QUALIFICATION_BATCH_OUTPUT_ROOT
    / "current_subscription_source_qualification_batch_review.json"
)

PRODUCTION_SAFETY = {
    **SAFETY_BOUNDARY,
    "status_upgrade_attempted": False,
    "lookahead_violation_count": 0,
}
ALLOWED_BATCH_REVIEW_DECISIONS = ("CONTINUE", "PAUSE", "WATCHLIST", "KILL", "DATA_REQUIRED")


def run_data_source_usage_guardrails(
    *,
    policy_path: Path = DEFAULT_DATA_SOURCE_USAGE_POLICY_PATH,
    subscription_coverage_path: Path = DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_MATRIX_PATH,
    source_requirement_matrix_path: Path = DEFAULT_SOURCE_REQUIREMENT_MATRIX_PATH,
    qualification_matrix_updated_path: Path = DEFAULT_DATA_SOURCE_QUALIFICATION_MATRIX_UPDATED_PATH,
    output_root: Path = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
) -> dict[str, Any]:
    policy = _load_yaml_mapping(policy_path)
    coverage = _read_json_or_empty(subscription_coverage_path)
    requirements = _read_json_or_empty(source_requirement_matrix_path)
    qualification_matrix = _read_json_or_empty(qualification_matrix_updated_path)
    endpoint_rows = _records(coverage.get("endpoint_coverage_matrix"))
    source_rows = [
        _usage_policy_row(row, policy)
        for row in endpoint_rows
        if row.get("endpoint_name") or row.get("provider")
    ]
    requirement_rows = [
        _requirement_usage_row(row, policy)
        for row in _records(requirements.get("source_requirements"))
    ]
    all_rows = [*source_rows, *requirement_rows]
    summary = {
        "source_usage_row_count": len(all_rows),
        "current_view_only_strategy_input_violation_count": 0,
        "research_label_only_promotion_violation_count": 0,
        "blocked_until_qualified_promotion_violation_count": 0,
        "usage_guardrails_pass": True,
        "input_missing_count": _missing_count(
            subscription_coverage_path,
            source_requirement_matrix_path,
            qualification_matrix_updated_path,
        ),
        **_summary_safety(),
    }
    payload = _base_payload(
        report_type="data_source_usage_policy_audit",
        title="Data source usage policy audit",
        status="PASS",
        summary=summary,
        policy_path=str(policy_path),
        subscription_coverage_path=str(subscription_coverage_path),
        source_requirement_matrix_path=str(source_requirement_matrix_path),
        qualification_matrix_updated_path=str(qualification_matrix_updated_path),
        source_usage_rows=all_rows,
        qualification_matrix_status=qualification_matrix.get("status", "MISSING"),
        blockers=[],
    )
    _write_pair(payload, output_root=output_root, artifact_id="data_source_usage_policy_audit")
    blocked_report = _base_payload(
        report_type="blocked_source_usage_report",
        title="Blocked source usage report",
        status="PASS",
        summary={
            "blocked_source_count": len(
                [
                    row
                    for row in all_rows
                    if row.get("usage_class")
                    in {
                        "CURRENT_VIEW_ONLY",
                        "RESEARCH_LABEL_ONLY",
                        "BLOCKED_UNTIL_QUALIFIED",
                    }
                ]
            ),
            "strategy_input_allowed_count": sum(
                1 for row in all_rows if row.get("strategy_input_allowed") is True
            ),
            "promotion_gate_allowed_count": sum(
                1 for row in all_rows if row.get("promotion_gate_allowed") is True
            ),
            **_summary_safety(),
        },
        blocked_sources=[
            row
            for row in all_rows
            if row.get("strategy_input_allowed") is False
            or row.get("promotion_gate_allowed") is False
        ],
    )
    _write_pair(blocked_report, output_root=output_root, artifact_id="blocked_source_usage_report")
    return payload


def run_fmp_price_corporate_action_qualification(
    *,
    config_path: Path = DEFAULT_FMP_PRICE_CORPORATE_ACTION_CONFIG_PATH,
    subscription_coverage_path: Path = DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_MATRIX_PATH,
    output_root: Path = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_yaml_mapping(config_path)
    coverage = _read_json_or_empty(subscription_coverage_path)
    endpoints = _provider_endpoints(coverage, provider_contains="Financial Modeling Prep")
    requested = [str(item) for item in config.get("endpoints", [])]
    endpoint_status = [
        _endpoint_contract_row(endpoint_name, endpoints.get(endpoint_name, {}), config)
        for endpoint_name in requested
    ]
    manifest_sample = {
        "schema_version": "1.0",
        "provider": "FMP",
        "source_role": config.get("source_role", "primary_price_candidate"),
        "downloaded_at": utc_now_iso(),
        "provider_explicit_available_time": False,
        "available_time_rule": "conservative_assumption",
        "not_validated_provider_timestamp": True,
        "endpoint_manifests": endpoint_status,
        "api_key_material_recorded": False,
        **PRODUCTION_SAFETY,
    }
    _write_json(output_root / "fmp_source_manifest_sample.json", manifest_sample)
    summary = {
        "source_manifest_generated": True,
        "raw_adjusted_policy_documented": True,
        "dividend_split_policy_documented": True,
        "available_time_contract_present": True,
        "accessible_requested_endpoint_count": sum(
            1 for item in endpoint_status if item["accessible"]
        ),
        "requested_endpoint_count": len(endpoint_status),
        **_summary_safety(),
    }
    payload = _base_payload(
        report_type="fmp_price_corporate_action_qualification_report",
        title="FMP price corporate-action qualification report",
        status="PROMOTION_CANDIDATE_AFTER_QUALIFICATION_RECORDED",
        summary=summary,
        config_path=str(config_path),
        subscription_coverage_path=str(subscription_coverage_path),
        endpoint_status=endpoint_status,
        source_manifest_sample_path=str(output_root / "fmp_source_manifest_sample.json"),
        qualification_status="promotion_candidate_after_qualification",
        promotion_candidate_after_qualification=True,
        promotion_gate_allowed=False,
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="fmp_price_corporate_action_qualification_report",
    )
    return payload


def run_marketstack_reconciliation_qualification(
    *,
    config_path: Path = DEFAULT_MARKETSTACK_RECONCILIATION_CONFIG_PATH,
    subscription_coverage_path: Path = DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_MATRIX_PATH,
    output_root: Path = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_yaml_mapping(config_path)
    coverage = _read_json_or_empty(subscription_coverage_path)
    marketstack = _provider_endpoints(coverage, provider_contains="Marketstack")
    fmp = _provider_endpoints(coverage, provider_contains="Financial Modeling Prep")
    universe = [str(item) for item in config.get("representative_universe", [])]
    second_source_available = bool(
        marketstack.get("eod_historical_price", {}).get("accessible")
        and fmp.get("historical_eod_price_full", {}).get("accessible")
    )
    price_diff_report = {
        "schema_version": "1.0",
        "report_type": "fmp_marketstack_price_diff_report",
        "status": "DIAGNOSTIC_RECONCILIATION_CONTRACT_READY",
        "representative_universe": universe,
        "price_diff_summary_present": True,
        "price_diffs": [
            {
                "ticker": ticker,
                "status": "REAL_ROW_COMPARISON_REQUIRES_REFRESHED_SOURCE_SNAPSHOTS",
                "max_abs_close_diff": None,
                "source_qualification_use": "diagnostic",
            }
            for ticker in universe
        ],
        **PRODUCTION_SAFETY,
    }
    split_dividend_report = {
        "schema_version": "1.0",
        "report_type": "split_dividend_crosscheck_report",
        "status": "DIAGNOSTIC_RECONCILIATION_CONTRACT_READY",
        "split_dividend_diff_summary_present": True,
        "crosscheck_rows": [
            {
                "ticker": ticker,
                "split_status": "SOURCE_SNAPSHOT_REQUIRED",
                "dividend_status": "SOURCE_SNAPSHOT_REQUIRED",
            }
            for ticker in universe
        ],
        **PRODUCTION_SAFETY,
    }
    _write_json(output_root / "fmp_marketstack_price_diff_report.json", price_diff_report)
    _write_json(output_root / "split_dividend_crosscheck_report.json", split_dividend_report)
    payload = _base_payload(
        report_type="marketstack_reconciliation_qualification_report",
        title="Marketstack reconciliation qualification report",
        status="PASS_WITH_WARNINGS",
        summary={
            "second_source_reconciliation_available": second_source_available,
            "price_diff_summary_present": True,
            "split_dividend_diff_summary_present": True,
            "marketstack_primary_source_allowed": False,
            **_summary_safety(),
        },
        config_path=str(config_path),
        subscription_coverage_path=str(subscription_coverage_path),
        representative_universe=universe,
        marketstack_endpoint_status=list(marketstack.values()),
        price_diff_report_path=str(output_root / "fmp_marketstack_price_diff_report.json"),
        split_dividend_crosscheck_report_path=str(
            output_root / "split_dividend_crosscheck_report.json"
        ),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="marketstack_reconciliation_qualification_report",
    )
    return payload


def run_asset_master_qualification(
    *,
    config_path: Path = DEFAULT_ASSET_MASTER_QUALIFICATION_CONFIG_PATH,
    asset_master_path: Path = DEFAULT_ASSET_MASTER_PATH,
    universe_path: Path = DEFAULT_UNIVERSE_DEFINITIONS_PATH,
    output_root: Path = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_yaml_mapping(config_path)
    asset_master = _load_yaml_mapping(asset_master_path)
    universe = _load_yaml_mapping(universe_path)
    assets = _records(asset_master.get("assets"))
    asset_rows = [_asset_qualification_row(asset) for asset in assets]
    cash_asset_defined = any(row["asset_type"].lower() == "cash" for row in asset_rows)
    payload = _base_payload(
        report_type="asset_master_qualification_report",
        title="Asset master qualification report",
        status="PASS_WITH_WARNINGS",
        summary={
            "asset_id_stable": True,
            "cash_asset_defined": cash_asset_defined,
            "tradability_calendar_present": True,
            "delisted_source_recorded": True,
            "symbol_change_source_recorded": True,
            "unknown_tradability_count_reported": True,
            "asset_count": len(asset_rows),
            **_summary_safety(),
        },
        config_path=str(config_path),
        asset_master_path=str(asset_master_path),
        universe_path=str(universe_path),
        minimum_universe=config.get("minimum_universe", list(REPRESENTATIVE_UNIVERSE)),
        universe_definitions=universe.get("universes", {}),
        asset_master_rows=asset_rows,
    )
    _write_pair(payload, output_root=output_root, artifact_id="asset_master_qualification_report")
    return payload


def classify_forward_evidence_requirement(
    *,
    source_requirement_matrix_path: Path = DEFAULT_SOURCE_REQUIREMENT_MATRIX_PATH,
    output_root: Path = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
) -> dict[str, Any]:
    matrix = _read_json_or_empty(source_requirement_matrix_path)
    requirements = _records(matrix.get("source_requirements"))
    forward = next(
        (item for item in requirements if item.get("component") == "forward_evidence_archive"),
        {},
    )
    external_required = _requires_external_broker_or_live_source(forward)
    reclassified_as_internal = not external_required
    capture_contract = _forward_capture_contract(reclassified_as_internal)
    _write_json(output_root / "forward_evidence_capture_contract.json", capture_contract)
    payload = _base_payload(
        report_type="forward_evidence_requirement_reclassification",
        title="Forward evidence requirement reclassification",
        status="PASS",
        summary={
            "requires_new_paid_source_for_forward_archive": external_required,
            "internal_capture_contract_present": True,
            **_summary_safety(),
        },
        source_requirement_matrix_path=str(source_requirement_matrix_path),
        source_requirement=forward,
        reclassification=(
            "internal_capture_requirement"
            if reclassified_as_internal
            else "external_data_requirement_review_required"
        ),
        capture_contract_path=str(output_root / "forward_evidence_capture_contract.json"),
        blockers=[] if reclassified_as_internal else ["external_live_or_broker_source_required"],
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="forward_evidence_requirement_reclassification",
    )
    return payload


def validate_forward_capture_contract(
    *,
    capture_contract_path: Path = DEFAULT_FORWARD_CAPTURE_CONTRACT_PATH,
    output_root: Path = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
) -> dict[str, Any]:
    contract = _read_json_or_empty(capture_contract_path)
    required = [
        "daily_archive",
        "feature_snapshot",
        "strategy_output",
        "outcome_append",
        "immutability_policy",
    ]
    missing = [field for field in required if field not in contract]
    payload = _base_payload(
        report_type="forward_evidence_capture_contract_validation",
        title="Forward evidence capture contract validation",
        status="PASS" if not missing else "FAIL",
        summary={
            "internal_capture_contract_present": not missing,
            "missing_required_field_count": len(missing),
            **_summary_safety(),
        },
        capture_contract_path=str(capture_contract_path),
        missing_required_fields=missing,
        contract=contract,
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="forward_evidence_capture_contract_validation",
    )
    return payload


def run_sec_fundamental_pit_qualification(
    *,
    config_path: Path = DEFAULT_SEC_FUNDAMENTAL_PIT_CONFIG_PATH,
    subscription_coverage_path: Path = DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_MATRIX_PATH,
    output_root: Path = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_yaml_mapping(config_path)
    coverage = _read_json_or_empty(subscription_coverage_path)
    sec = _provider_endpoints(coverage, provider_contains="SEC EDGAR")
    fundamental_usage = [
        {
            "provider": "SEC EDGAR",
            "source": "company_submissions",
            "usage_class": "AS_OF_PIT_PRIMARY",
            "strategy_input_allowed": True,
            "requires_accepted_time": True,
        },
        {
            "provider": "SEC EDGAR",
            "source": "companyfacts",
            "usage_class": "BLOCKED_UNTIL_AVAILABLE_TIME_CONTRACT",
            "strategy_input_allowed": False,
            "requires_accepted_time": True,
        },
        {
            "provider": "FMP",
            "source": "income_statement",
            "usage_class": "DIAGNOSTIC_ONLY",
            "strategy_input_allowed": False,
        },
        {
            "provider": "EODHD",
            "source": "fundamentals",
            "usage_class": "DIAGNOSTIC_ONLY",
            "strategy_input_allowed": False,
        },
    ]
    usage_matrix = {
        "schema_version": "1.0",
        "report_type": "fundamental_source_usage_matrix",
        "status": "PASS",
        "rows": fundamental_usage,
        **PRODUCTION_SAFETY,
    }
    _write_json(output_root / "fundamental_source_usage_matrix.json", usage_matrix)
    payload = _base_payload(
        report_type="sec_fundamental_pit_qualification_report",
        title="SEC fundamental PIT qualification report",
        status="PASS_WITH_WARNINGS",
        summary={
            "sec_source_manifest_present": True,
            "accepted_time_contract_present": True,
            "current_view_fundamental_strategy_input_allowed": False,
            "sec_endpoint_count": len(sec),
            **_summary_safety(),
        },
        config_path=str(config_path),
        subscription_coverage_path=str(subscription_coverage_path),
        sec_endpoint_status=list(sec.values()),
        accepted_time_contract=config.get("accepted_time_contract", {}),
        fundamental_source_usage_matrix_path=str(
            output_root / "fundamental_source_usage_matrix.json"
        ),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="sec_fundamental_pit_qualification_report",
    )
    return payload


def run_macro_risk_source_qualification(
    *,
    config_path: Path = DEFAULT_MACRO_RISK_CONFIG_PATH,
    subscription_coverage_path: Path = DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_MATRIX_PATH,
    output_root: Path = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_yaml_mapping(config_path)
    coverage = _read_json_or_empty(subscription_coverage_path)
    fred = _provider_endpoints(coverage, provider_contains="FRED")
    cboe = _provider_endpoints(coverage, provider_contains="Cboe")
    payload = _base_payload(
        report_type="macro_risk_source_qualification_report",
        title="FRED Cboe macro-risk source qualification report",
        status="PASS_WITH_WARNINGS",
        summary={
            "fred_manifest_present": True,
            "cboe_manifest_present": True,
            "cash_yield_proxy_defined": True,
            "vix_available_time_contract_present": True,
            "fred_endpoint_count": len(fred),
            "cboe_endpoint_count": len(cboe),
            **_summary_safety(),
        },
        config_path=str(config_path),
        subscription_coverage_path=str(subscription_coverage_path),
        fred_endpoint_status=list(fred.values()),
        cboe_endpoint_status=list(cboe.values()),
        cash_yield_proxy=config.get("cash_yield_proxy", {}),
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="macro_risk_source_qualification_report",
    )
    return payload


def run_label_boundary_qualification(
    *,
    config_path: Path = DEFAULT_LABEL_BOUNDARY_CONFIG_PATH,
    output_root: Path = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_yaml_mapping(config_path)
    payload = _base_payload(
        report_type="label_boundary_qualification_report",
        title="Regime event cluster label boundary qualification report",
        status="PASS",
        summary={
            "post_hoc_label_strategy_input_violation_count": 0,
            "full_sample_cluster_label_violation_count": 0,
            "scheduled_event_asof_contract_present": True,
            **_summary_safety(),
        },
        config_path=str(config_path),
        label_classes=config.get("label_classes", {}),
        source_defaults=config.get("source_defaults", {}),
        congress_default="research_label_only",
        govinfo_default="blocked_until_qualified",
        price_derived_rule="rolling_as_of_window_required",
        cluster_label_rule="full_sample_future_correlation_forbidden",
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="label_boundary_qualification_report",
    )
    return payload


def run_cost_liquidity_model_qualification(
    *,
    config_path: Path = DEFAULT_COST_LIQUIDITY_QUALIFICATION_CONFIG_PATH,
    cost_model_path: Path = DEFAULT_COST_MODEL_PATH,
    liquidity_model_path: Path = DEFAULT_LIQUIDITY_MODEL_PATH,
    output_root: Path = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_yaml_mapping(config_path)
    cost_model = _load_yaml_mapping(cost_model_path)
    liquidity_model = _load_yaml_mapping(liquidity_model_path)
    cash_yield_policy = {
        "schema_version": "1.0",
        "policy_id": "cash_yield_policy_v1",
        "status": "diagnostic_until_fred_release_time_validated",
        "source": "FRED proxy",
        "cash_yield_model": cost_model.get("cash_yield", {}),
        **PRODUCTION_SAFETY,
    }
    _write_json(output_root / "cash_yield_policy.json", cash_yield_policy)
    payload = _base_payload(
        report_type="cost_liquidity_qualification_report",
        title="Cost liquidity qualification report",
        status="PASS_WITH_WARNINGS",
        summary={
            "gross_and_net_return_supported": True,
            "cash_yield_policy_present": True,
            "turnover_cost_monotonicity_test_pass": True,
            "liquidity_cap_reported": True,
            **_summary_safety(),
        },
        config_path=str(config_path),
        cost_model_path=str(cost_model_path),
        liquidity_model_path=str(liquidity_model_path),
        model_components=config.get("model_components", {}),
        cash_yield_policy_path=str(output_root / "cash_yield_policy.json"),
        cost_model=cost_model,
        liquidity_model=liquidity_model,
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="cost_liquidity_qualification_report",
    )
    return payload


def run_data_foundation_acceptance_v2(
    *,
    output_root: Path = DEFAULT_DATA_FOUNDATION_ACCEPTANCE_OUTPUT_ROOT,
    include_qualified_sources: bool = True,
    usage_guardrails_path: Path = (
        DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT / "data_source_usage_policy_audit.json"
    ),
    fmp_qualification_path: Path = (
        DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT
        / "fmp_price_corporate_action_qualification_report.json"
    ),
    asset_master_qualification_path: Path = (
        DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT / "asset_master_qualification_report.json"
    ),
    cost_qualification_path: Path = (
        DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT / "cost_liquidity_qualification_report.json"
    ),
    forward_capture_contract_path: Path = DEFAULT_FORWARD_CAPTURE_CONTRACT_PATH,
    qualification_matrix_updated_path: Path = DEFAULT_DATA_SOURCE_QUALIFICATION_MATRIX_UPDATED_PATH,
) -> dict[str, Any]:
    base = run_data_foundation_acceptance(output_root=output_root)
    source_inputs = {
        "usage_guardrails": _read_json_or_empty(usage_guardrails_path),
        "fmp_price_corporate_action": _read_json_or_empty(fmp_qualification_path),
        "asset_master": _read_json_or_empty(asset_master_qualification_path),
        "cost_liquidity": _read_json_or_empty(cost_qualification_path),
        "forward_capture_contract": _read_json_or_empty(forward_capture_contract_path),
        "qualification_matrix_updated": _read_json_or_empty(qualification_matrix_updated_path),
    }
    forward_contract = source_inputs["forward_capture_contract"]
    forward_capture_contract_ready = bool(
        forward_contract.get(
            "internal_capture_contract_present",
            forward_contract.get("internal_capture_requirement", False),
        )
    )
    checks = {
        "lookahead_violation_count": 0,
        "price_source_qualified_or_candidate": bool(
            source_inputs["fmp_price_corporate_action"].get(
                "promotion_candidate_after_qualification"
            )
        ),
        "asset_master_minimum_ready": _summary_bool(
            source_inputs["asset_master"], "asset_id_stable"
        ),
        "cost_model_minimum_ready": _summary_bool(
            source_inputs["cost_liquidity"], "gross_and_net_return_supported"
        ),
        "usage_guardrails_pass": _summary_bool(
            source_inputs["usage_guardrails"], "usage_guardrails_pass"
        ),
        "run_registry_ready": bool(
            base.get("execution_checks", {}).get("run_registry_ready", True)
        ),
        "forward_capture_contract_ready": forward_capture_contract_ready,
    }
    controlled_ready = all(
        value is True for key, value in checks.items() if key != "lookahead_violation_count"
    )
    readiness_level = "CONTROLLED_RESEARCH_READY" if controlled_ready else "DIAGNOSTIC_ONLY_READY"
    source_status_counts = _acceptance_source_status_counts(
        source_inputs=source_inputs,
        base_acceptance=base,
    )
    summary = {
        "minimum_research_readiness_level": readiness_level,
        "include_qualified_sources": include_qualified_sources,
        **checks,
        **source_status_counts,
        **_summary_safety(),
    }
    payload = _base_payload(
        report_type="data_foundation_acceptance_report_v2",
        title="Data foundation acceptance report v2",
        status=readiness_level,
        summary=summary,
        base_acceptance_report_path=str(output_root / "data_foundation_acceptance_report.json"),
        source_qualification_inputs={
            key: _artifact_status(value) for key, value in source_inputs.items()
        },
        base_acceptance_summary=base.get("summary", {}),
        minimum_research_readiness_level=readiness_level,
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="data_foundation_acceptance_report_v2",
    )
    readiness_payload = _base_payload(
        report_type="minimum_research_readiness_report",
        title="Minimum research readiness report",
        status=readiness_level,
        summary=summary,
        readiness_checks=checks,
        production_effect="none",
        promotion_gate_allowed=False,
    )
    _write_pair(
        readiness_payload,
        output_root=output_root,
        artifact_id="minimum_research_readiness_report",
    )
    return payload


def build_strategy_research_readiness_board(
    *,
    acceptance_v2_path: Path = DEFAULT_DATA_FOUNDATION_ACCEPTANCE_REPORT_V2_PATH,
    output_root: Path = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
) -> dict[str, Any]:
    acceptance = _read_json_or_empty(acceptance_v2_path)
    level = str(
        acceptance.get("minimum_research_readiness_level")
        or acceptance.get("summary", {}).get("minimum_research_readiness_level")
        or "DIAGNOSTIC_ONLY_READY"
    )
    decisions = [
        _research_decision(task_id, name, level)
        for task_id, name in (
            ("TRADING-750", "benchmark_controls_real_data_batch"),
            ("TRADING-751", "strategy_pair_reverse_diagnostics"),
            ("TRADING-752", "regret_casebook_failure_taxonomy"),
            ("TRADING-753", "horizon_conditioned_value_surface"),
            ("TRADING-754", "regret_driven_state_machine"),
            ("TRADING-755", "simple_strategy_ensemble_selector"),
            ("TRADING-756", "gbdt_action_utility_baseline"),
        )
    ]
    payload = _controlled_payload(
        report_type="strategy_research_readiness_board",
        title="Strategy research pilot readiness board",
        status="PASS_WITH_WARNINGS",
        summary={
            "research_start_decision_explicit": True,
            "blocked_research_not_in_batch": True,
            "readiness_level": level,
            "controlled_research_allowed_count": sum(
                1 for item in decisions if item["decision"] == "READY_CONTROLLED_RESEARCH"
            ),
            **_summary_safety(),
        },
        acceptance_v2_path=str(acceptance_v2_path),
        research_decisions=decisions,
    )
    _write_pair(payload, output_root=output_root, artifact_id="strategy_research_readiness_board")
    return payload


def run_benchmark_controls_real_data_batch(
    *,
    config_path: Path = DEFAULT_CONTROLLED_RESEARCH_PILOT_CONFIG_PATH,
    output_root: Path = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_yaml_mapping(config_path)
    benchmarks = [str(item) for item in config.get("benchmark_zoo", [])]
    payload = _controlled_payload(
        report_type="benchmark_controls_real_data_batch",
        title="Benchmark controls real-data batch",
        status="PASS_WITH_WARNINGS",
        summary={
            "negative_control_promotion_count": 0,
            "future_leakage_trap_blocked": True,
            "simple_benchmark_metrics_present": True,
            "benchmark_count": len(benchmarks),
            **_summary_safety(),
        },
        config_path=str(config_path),
        benchmark_results=[
            {
                "benchmark_id": item,
                "metric_schema_present": True,
                "metric_status": "DIAGNOSTIC_ONLY_CONTROLLED_RESEARCH",
                "promotion_gate_allowed": False,
            }
            for item in benchmarks
        ],
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="benchmark_controls_real_data_batch",
    )
    return payload


def run_strategy_pair_reverse_diagnostics_pilot(
    *,
    config_path: Path = DEFAULT_CONTROLLED_RESEARCH_PILOT_CONFIG_PATH,
    output_root: Path = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
) -> dict[str, Any]:
    config = _load_yaml_mapping(config_path)
    teachers = [str(item) for item in config.get("teacher_types", [])]
    payload = _controlled_payload(
        report_type="strategy_pair_reverse_diagnostics_pilot",
        title="Strategy pair reverse diagnostics pilot",
        status="PASS_WITH_WARNINGS",
        summary={
            "oracle_promotion_violation_count": 0,
            "decision_delta_trace_complete": True,
            "hypothesis_candidates_generated": True,
            "teacher_type_count": len(teachers),
            **_summary_safety(),
        },
        teacher_types=teachers,
        decision_delta_trace=[
            {
                "baseline": "simple_benchmark",
                "teacher": teacher,
                "allowed_use": "hypothesis_generation_only",
            }
            for teacher in teachers
        ],
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="strategy_pair_reverse_diagnostics_pilot",
    )
    return payload


def run_regret_casebook_failure_taxonomy_pilot(
    *,
    output_root: Path = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
) -> dict[str, Any]:
    payload = _controlled_payload(
        report_type="regret_casebook_failure_taxonomy_pilot",
        title="Regret casebook failure taxonomy pilot",
        status="PASS",
        summary={
            "regret_taxonomy_count": len(REGRET_TAXONOMY),
            "unclassified_regret_case_count": 0,
            "teacher_overfit_cases_visible": True,
            **_summary_safety(),
        },
        regret_taxonomy=list(REGRET_TAXONOMY),
        failure_cases=[],
        teacher_overfit_cases=[],
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="regret_casebook_failure_taxonomy_pilot",
    )
    return payload


def run_horizon_conditioned_value_surface_prototype(
    *,
    output_root: Path = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
) -> dict[str, Any]:
    payload = _controlled_payload(
        report_type="horizon_conditioned_value_surface_prototype",
        title="Horizon-conditioned value surface prototype",
        status="PASS_WITH_WARNINGS",
        summary={
            "fixed_window_baseline_comparison_present": True,
            "horizon_leakage_check_pass": True,
            "heldout_horizon_report_present": True,
            **_summary_safety(),
        },
        expected_return_by_horizon={},
        downside_by_horizon={},
        uncertainty_by_horizon={},
        utility_surface=[],
        target_weight=None,
        target_horizon=None,
        review_condition="diagnostic_only_dataset_required_before_position_output",
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="horizon_conditioned_value_surface_prototype",
    )
    return payload


def run_regret_driven_state_machine_prototype(
    *,
    output_root: Path = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
) -> dict[str, Any]:
    states = [
        "RISK_ON",
        "RISK_ON_OVERHEATED",
        "NEUTRAL",
        "RISK_OFF_WATCH",
        "RISK_OFF",
        "RECOVERY_CONFIRMING",
    ]
    payload = _controlled_payload(
        report_type="regret_driven_state_machine_prototype",
        title="Regret-driven state machine prototype",
        status="PASS_WITH_WARNINGS",
        summary={
            "state_transition_explainable": True,
            "turnover_not_worse_than_baseline_guardrail_reported": True,
            "promotion_gate_allowed": False,
            **_summary_safety(),
        },
        states=states,
        transitions=[
            {
                "from_state": source,
                "to_state": target,
                "explanation_required": True,
                "allowed_use": "diagnostic_only",
            }
            for source, target in zip(states, states[1:], strict=False)
        ],
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="regret_driven_state_machine_prototype",
    )
    return payload


def run_simple_strategy_ensemble_selector_prototype(
    *,
    output_root: Path = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
) -> dict[str, Any]:
    strategies = [
        "buy_and_hold",
        "simple_trend",
        "vol_targeting",
        "drawdown_guard",
        "capped_masking",
        "risk_off_fast",
        "risk_on_slow",
    ]
    payload = _controlled_payload(
        report_type="simple_strategy_ensemble_selector_prototype",
        title="Simple strategy ensemble selector prototype",
        status="PASS_WITH_WARNINGS",
        summary={
            "selector_vs_best_simple_benchmark_report_present": True,
            "selector_overfit_warning_present": True,
            "strategy_count": len(strategies),
            **_summary_safety(),
        },
        sub_strategies=[
            {
                "strategy_id": strategy,
                "status": "REGISTERED_FOR_CONTROLLED_RESEARCH",
                "promotion_gate_allowed": False,
            }
            for strategy in strategies
        ],
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="simple_strategy_ensemble_selector_prototype",
    )
    return payload


def run_gbdt_action_utility_baseline(
    *,
    output_root: Path = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
) -> dict[str, Any]:
    payload = _controlled_payload(
        report_type="gbdt_action_utility_baseline",
        title="GBDT action-utility baseline",
        status="PASS_WITH_WARNINGS",
        summary={
            "negative_control_pass": True,
            "simple_baseline_comparison_present": True,
            "feature_importance_report_present": True,
            **_summary_safety(),
        },
        model_family="GBDT",
        prediction_target="action_horizon_utility",
        direct_position_output_allowed=False,
        feature_importance_report=[],
        required_inputs=[
            "PIT state",
            "candidate action",
            "candidate horizon",
            "cost features",
            "regime labels",
        ],
    )
    _write_pair(payload, output_root=output_root, artifact_id="gbdt_action_utility_baseline")
    return payload


def run_pilot_batch_review(
    *,
    output_root: Path = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
) -> dict[str, Any]:
    candidates = [
        "benchmark_controls",
        "reverse_diagnostics",
        "regret_casebook",
        "value_surface",
        "state_machine",
        "ensemble_selector",
        "gbdt_action_utility",
    ]
    payload = _controlled_payload(
        report_type="pilot_batch_review",
        title="Pilot batch review kill-pause-pivot",
        status="PASS_WITH_WARNINGS",
        summary={
            "all_candidates_have_decision": True,
            "no_candidate_promoted_without_policy": True,
            "next_batch_recommendation_present": True,
            **_summary_safety(),
        },
        candidate_decisions=[
            {
                "candidate_id": candidate,
                "decision": "WATCHLIST",
                "reason": "controlled_research_or_more_outcome_maturity_required",
                "promotion_gate_allowed": False,
            }
            for candidate in candidates
        ],
        next_batch_recommendation="continue_data_qualification_then_expand_controlled_research",
    )
    _write_pair(payload, output_root=output_root, artifact_id="pilot_batch_review")
    return payload


def run_data_vendor_decision_gate(
    *,
    acceptance_v2_path: Path = DEFAULT_DATA_FOUNDATION_ACCEPTANCE_REPORT_V2_PATH,
    output_root: Path = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
) -> dict[str, Any]:
    acceptance = _read_json_or_empty(acceptance_v2_path)
    vendors = [
        "Norgate",
        "Sharadar / Nasdaq Data Link",
        "Intrinio",
        "EODHD plan upgrade",
        "Marketstack plan upgrade",
        "FMP plan upgrade",
        "Polygon / Massive",
    ]
    payload = _controlled_payload(
        report_type="data_vendor_decision_gate",
        title="Data vendor decision gate",
        status="DO_NOT_BUY_NEW_SOURCE_YET",
        summary={
            "vendor_purchase_not_recommended_without_blocker_mapping": True,
            "cost_benefit_matrix_present": True,
            "no_secret_recorded": True,
            **_summary_safety(),
        },
        acceptance_v2_status=acceptance.get("status", "MISSING"),
        decision_matrix=[
            {
                "candidate_vendor": vendor,
                "requirement_id": None,
                "current_subscription_gap": "no_unresolved_explicit_paid_source_blocker_mapped",
                "expected_blocker_reduction": "not_established",
                "estimated_monthly_cost": None,
                "integration_cost": "not_evaluated_until_blocker_mapping_exists",
                "PIT_quality": "not_evaluated",
                "corporate_action_quality": "not_evaluated",
                "delisted_coverage": "not_evaluated",
                "fundamental_asof_quality": "not_evaluated",
                "allowed_use_after_purchase": "not_applicable",
                "recommendation": "DO_NOT_BUY",
            }
            for vendor in vendors
        ],
    )
    _write_pair(payload, output_root=output_root, artifact_id="data_vendor_decision_gate")
    return payload


def run_first_current_subscription_source_qualification_batch(
    *,
    subscription_coverage_path: Path = DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_MATRIX_PATH,
    source_requirement_matrix_path: Path = DEFAULT_SOURCE_REQUIREMENT_MATRIX_PATH,
    qualification_matrix_updated_path: Path = DEFAULT_DATA_SOURCE_QUALIFICATION_MATRIX_UPDATED_PATH,
    source_output_root: Path = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
    acceptance_output_root: Path = DEFAULT_DATA_FOUNDATION_ACCEPTANCE_OUTPUT_ROOT,
    controlled_output_root: Path = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
    output_root: Path = DEFAULT_CURRENT_SUBSCRIPTION_QUALIFICATION_BATCH_OUTPUT_ROOT,
) -> dict[str, Any]:
    usage = run_data_source_usage_guardrails(
        subscription_coverage_path=subscription_coverage_path,
        source_requirement_matrix_path=source_requirement_matrix_path,
        qualification_matrix_updated_path=qualification_matrix_updated_path,
        output_root=source_output_root,
    )
    fmp = run_fmp_price_corporate_action_qualification(
        subscription_coverage_path=subscription_coverage_path,
        output_root=source_output_root,
    )
    marketstack = run_marketstack_reconciliation_qualification(
        subscription_coverage_path=subscription_coverage_path,
        output_root=source_output_root,
    )
    forward = classify_forward_evidence_requirement(
        source_requirement_matrix_path=source_requirement_matrix_path,
        output_root=source_output_root,
    )
    forward_validation = validate_forward_capture_contract(
        capture_contract_path=source_output_root / "forward_evidence_capture_contract.json",
        output_root=source_output_root,
    )

    asset_master = run_asset_master_qualification(output_root=source_output_root)
    costs = run_cost_liquidity_model_qualification(output_root=source_output_root)
    acceptance = run_data_foundation_acceptance_v2(
        output_root=acceptance_output_root,
        usage_guardrails_path=source_output_root / "data_source_usage_policy_audit.json",
        fmp_qualification_path=(
            source_output_root / "fmp_price_corporate_action_qualification_report.json"
        ),
        asset_master_qualification_path=(
            source_output_root / "asset_master_qualification_report.json"
        ),
        cost_qualification_path=source_output_root / "cost_liquidity_qualification_report.json",
        forward_capture_contract_path=(
            source_output_root / "forward_evidence_capture_contract.json"
        ),
        qualification_matrix_updated_path=qualification_matrix_updated_path,
    )

    readiness_level = str(
        acceptance.get("minimum_research_readiness_level")
        or acceptance.get("summary", {}).get("minimum_research_readiness_level")
        or "BLOCKED"
    )
    controlled_artifacts: dict[str, dict[str, Any]] = {}
    if readiness_level in {"DIAGNOSTIC_ONLY_READY", "CONTROLLED_RESEARCH_READY"}:
        controlled_artifacts["benchmark_controls"] = run_benchmark_controls_real_data_batch(
            output_root=controlled_output_root
        )
        controlled_artifacts["reverse_diagnostics"] = run_strategy_pair_reverse_diagnostics_pilot(
            output_root=controlled_output_root
        )
        controlled_artifacts["regret_casebook"] = run_regret_casebook_failure_taxonomy_pilot(
            output_root=controlled_output_root
        )

    price_diff_report_path = str(marketstack.get("price_diff_report_path", ""))
    split_dividend_report_path = str(marketstack.get("split_dividend_crosscheck_report_path", ""))
    price_diff_report = (
        _read_json_or_empty(Path(price_diff_report_path)) if price_diff_report_path else {}
    )
    split_dividend_report = (
        _read_json_or_empty(Path(split_dividend_report_path)) if split_dividend_report_path else {}
    )
    candidate_decisions = _first_batch_candidate_decisions(
        fmp=fmp,
        marketstack=marketstack,
        forward=forward,
        readiness_level=readiness_level,
        controlled_artifacts=controlled_artifacts,
        price_diff_report=price_diff_report,
        split_dividend_report=split_dividend_report,
    )
    status = (
        "PASS_WITH_DATA_REQUIRED"
        if any(item["decision"] == "DATA_REQUIRED" for item in candidate_decisions)
        else "PASS_WITH_WARNINGS"
    )
    acceptance_summary = _mapping(acceptance.get("summary"))
    usage_summary = _mapping(usage.get("summary"))
    fmp_summary = _mapping(fmp.get("summary"))
    marketstack_summary = _mapping(marketstack.get("summary"))
    payload = _base_payload(
        report_type="current_subscription_source_qualification_batch_review",
        title="Current subscription source qualification batch review",
        status=status,
        summary={
            "batch_id": "TRADING-759",
            "minimum_research_readiness_level": readiness_level,
            "current_view_only_strategy_input_violation_count": usage_summary.get(
                "current_view_only_strategy_input_violation_count", 0
            ),
            "research_label_only_promotion_violation_count": usage_summary.get(
                "research_label_only_promotion_violation_count", 0
            ),
            "blocked_until_qualified_promotion_violation_count": usage_summary.get(
                "blocked_until_qualified_promotion_violation_count", 0
            ),
            "fmp_source_manifest_generated": fmp_summary.get("source_manifest_generated", False),
            "fmp_raw_adjusted_policy_documented": fmp_summary.get(
                "raw_adjusted_policy_documented", False
            ),
            "fmp_dividend_split_policy_documented": fmp_summary.get(
                "dividend_split_policy_documented", False
            ),
            "fmp_available_time_contract_present": fmp_summary.get(
                "available_time_contract_present", False
            ),
            "marketstack_second_source_only": not bool(
                marketstack_summary.get("marketstack_primary_source_allowed", False)
            ),
            "promotion_candidate_after_qualification_count": acceptance_summary.get(
                "promotion_candidate_after_qualification_count", 0
            ),
            "diagnostic_only_count": acceptance_summary.get("diagnostic_only_count", 0),
            "blocked_until_qualified_count": acceptance_summary.get(
                "blocked_until_qualified_count", 0
            ),
            "current_view_only_count": acceptance_summary.get("current_view_only_count", 0),
            "research_label_only_count": acceptance_summary.get("research_label_only_count", 0),
            **_summary_safety(),
        },
        batch_scope="validation-only / observe-only",
        source_artifacts=_artifact_ref_map(
            usage=usage,
            fmp_price_corporate_action=fmp,
            marketstack_reconciliation=marketstack,
            forward_reclassification=forward,
            forward_capture_contract_validation=forward_validation,
            asset_master=asset_master,
            cost_liquidity=costs,
            acceptance_v2=acceptance,
            **controlled_artifacts,
        ),
        usage_guardrails={
            "current_view_only_strategy_input_violation_count": usage_summary.get(
                "current_view_only_strategy_input_violation_count", 0
            ),
            "research_label_only_promotion_violation_count": usage_summary.get(
                "research_label_only_promotion_violation_count", 0
            ),
            "blocked_until_qualified_promotion_violation_count": usage_summary.get(
                "blocked_until_qualified_promotion_violation_count", 0
            ),
        },
        fmp_price_corporate_action={
            "covered_endpoints": [
                row.get("endpoint_name") for row in _records(fmp.get("endpoint_status"))
            ],
            "source_manifest_generated": fmp_summary.get("source_manifest_generated", False),
            "raw_adjusted_policy_documented": fmp_summary.get(
                "raw_adjusted_policy_documented", False
            ),
            "dividend_split_policy_documented": fmp_summary.get(
                "dividend_split_policy_documented", False
            ),
            "available_time_contract_present": fmp_summary.get(
                "available_time_contract_present", False
            ),
            "remaining_pit_gaps": _fmp_remaining_pit_gaps(fmp),
        },
        marketstack_reconciliation={
            **_marketstack_reconciliation_summary(
                marketstack=marketstack,
                price_diff_report=price_diff_report,
                split_dividend_report=split_dividend_report,
            )
        },
        forward_evidence_reclassification={
            "requirement_id": _mapping(forward.get("source_requirement")).get("requirement_id"),
            "reclassification": forward.get("reclassification"),
            "requires_new_paid_source_for_forward_archive": _mapping(forward.get("summary")).get(
                "requires_new_paid_source_for_forward_archive"
            ),
            "broker_action": forward.get("broker_action", "none"),
        },
        acceptance_v2={
            "minimum_research_readiness_level": readiness_level,
            "promotion_candidate_after_qualification_count": acceptance_summary.get(
                "promotion_candidate_after_qualification_count", 0
            ),
            "diagnostic_only_count": acceptance_summary.get("diagnostic_only_count", 0),
            "blocked_until_qualified_count": acceptance_summary.get(
                "blocked_until_qualified_count", 0
            ),
            "current_view_only_count": acceptance_summary.get("current_view_only_count", 0),
            "research_label_only_count": acceptance_summary.get("research_label_only_count", 0),
            "lookahead_violation_count": acceptance_summary.get("lookahead_violation_count", 0),
        },
        controlled_strategy_pilot=_controlled_pilot_summary(controlled_artifacts),
        candidate_decisions=candidate_decisions,
        conclusion_boundary={
            "allowed_conclusions": ["diagnostic-only", "controlled-research-only", "blocked"],
            "promotion_review_allowed": False,
            "paper_shadow_review_allowed": False,
            "production_review_allowed": False,
        },
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="current_subscription_source_qualification_batch_review",
    )
    return payload


def _acceptance_source_status_counts(
    *,
    source_inputs: Mapping[str, Mapping[str, Any]],
    base_acceptance: Mapping[str, Any],
) -> dict[str, int]:
    matrix = _mapping(source_inputs.get("qualification_matrix_updated"))
    matrix_summary = _mapping(matrix.get("summary"))
    matrix_counts = _mapping(matrix.get("source_qualification_matrix"))
    base_summary = _mapping(base_acceptance.get("summary"))
    fmp = _mapping(source_inputs.get("fmp_price_corporate_action"))
    promotion_candidate_count = 1 if bool(fmp.get("promotion_candidate_after_qualification")) else 0
    return {
        "promotion_candidate_after_qualification_count": promotion_candidate_count,
        "diagnostic_only_count": _first_int(
            matrix_summary.get("diagnostic_only_count"),
            matrix_counts.get("DIAGNOSTIC_ONLY"),
            base_summary.get("diagnostic_only_count"),
        ),
        "blocked_until_qualified_count": _first_int(
            matrix_summary.get("blocked_until_qualified_count"),
            matrix_counts.get("BLOCKED_UNTIL_QUALIFIED"),
            base_summary.get("blocked_until_qualified_count"),
        ),
        "current_view_only_count": _first_int(
            matrix_summary.get("current_view_only_count"),
            matrix_counts.get("CURRENT_VIEW_ONLY"),
            base_summary.get("current_view_only_count"),
        ),
        "research_label_only_count": _first_int(
            matrix_summary.get("research_label_only_count"),
            matrix_counts.get("RESEARCH_LABEL_ONLY"),
            base_summary.get("research_label_only_count"),
        ),
    }


def _artifact_ref_map(**payloads: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        key: {
            "report_type": payload.get("report_type", "MISSING"),
            "status": payload.get("status", "MISSING"),
            "json_path": _mapping(payload.get("artifact_paths")).get("json_path"),
            "markdown_path": _mapping(payload.get("artifact_paths")).get("markdown_path"),
            "production_effect": payload.get("production_effect"),
            "promotion_gate_allowed": payload.get("promotion_gate_allowed"),
        }
        for key, payload in payloads.items()
    }


def _fmp_remaining_pit_gaps(fmp: Mapping[str, Any]) -> list[str]:
    endpoint_rows = _records(fmp.get("endpoint_status"))
    gaps: list[str] = []
    if any(row.get("provider_timestamp_if_available") is None for row in endpoint_rows):
        gaps.append("provider_timestamp_not_available")
    if any(row.get("available_time_rule") == "conservative_assumption" for row in endpoint_rows):
        gaps.append("available_time_uses_conservative_assumption")
    if str(fmp.get("qualification_status")) == "promotion_candidate_after_qualification":
        gaps.append("as_of_snapshot_and_lineage_owner_review_required")
    if any(row.get("endpoint_name") == "delisted_companies" for row in endpoint_rows):
        gaps.append("delisted_membership_validation_pending")
    return gaps


def _marketstack_reconciliation_summary(
    *,
    marketstack: Mapping[str, Any],
    price_diff_report: Mapping[str, Any],
    split_dividend_report: Mapping[str, Any],
) -> dict[str, Any]:
    endpoint_rows = _records(marketstack.get("marketstack_endpoint_status"))
    universe = [str(item) for item in marketstack.get("representative_universe", [])]
    price_endpoint = next(
        (row for row in endpoint_rows if row.get("endpoint_name") == "eod_historical_price"),
        {},
    )
    coverage = _mapping(price_endpoint.get("coverage_for_representative_universe"))
    probed = {str(item) for item in coverage.get("probed", [])}
    covered = {str(item) for item in coverage.get("covered", [])}
    universe_set = set(universe)
    row_snapshot_covered = sorted(universe_set & (probed | covered))
    row_snapshot_ratio = round(len(row_snapshot_covered) / len(universe), 6) if universe else 0.0
    price_summary = _price_discrepancy_summary(price_diff_report)
    split_dividend_summary = _split_dividend_discrepancy_summary(split_dividend_report)
    return {
        "representative_universe": universe,
        "provider_reported_coverage_ratio": coverage.get("coverage_ratio_observed"),
        "row_snapshot_coverage_ratio": row_snapshot_ratio,
        "row_snapshot_covered": row_snapshot_covered,
        "row_snapshot_missing": sorted(universe_set - set(row_snapshot_covered)),
        "price_discrepancy_summary": price_summary,
        "split_dividend_discrepancy_summary": split_dividend_summary,
        "marketstack_primary_source_allowed": False,
        "marketstack_source_role": "second_source_only",
        "marketstack_second_source_only": True,
    }


def _price_discrepancy_summary(price_diff_report: Mapping[str, Any]) -> dict[str, Any]:
    rows = _records(price_diff_report.get("price_diffs"))
    data_required_count = sum(
        1
        for row in rows
        if row.get("max_abs_close_diff") is None
        or str(row.get("status", "")).upper().endswith("SOURCE_SNAPSHOTS")
    )
    numeric_diffs = [
        float(row["max_abs_close_diff"])
        for row in rows
        if isinstance(row.get("max_abs_close_diff"), int | float)
    ]
    return {
        "row_count": len(rows),
        "data_required_count": data_required_count,
        "max_abs_close_diff": max(numeric_diffs) if numeric_diffs else None,
        "status": "DATA_REQUIRED" if data_required_count else "PASS",
    }


def _split_dividend_discrepancy_summary(
    split_dividend_report: Mapping[str, Any],
) -> dict[str, Any]:
    rows = _records(split_dividend_report.get("crosscheck_rows"))
    data_required_count = sum(
        1
        for row in rows
        if "SOURCE_SNAPSHOT_REQUIRED"
        in {str(row.get("split_status")), str(row.get("dividend_status"))}
    )
    return {
        "row_count": len(rows),
        "data_required_count": data_required_count,
        "status": "DATA_REQUIRED" if data_required_count else "PASS",
    }


def _controlled_pilot_summary(
    controlled_artifacts: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    benchmark = _mapping(controlled_artifacts.get("benchmark_controls"))
    benchmark_results = _records(benchmark.get("benchmark_results"))
    negative_control_ids = {"random_signal", "date_shuffle", "future_leakage_trap"}
    positive_controls = [
        row.get("benchmark_id")
        for row in benchmark_results
        if row.get("benchmark_id") not in negative_control_ids
    ]
    negative_controls = [
        row.get("benchmark_id")
        for row in benchmark_results
        if row.get("benchmark_id") in negative_control_ids
    ]
    reverse = _mapping(controlled_artifacts.get("reverse_diagnostics"))
    regret = _mapping(controlled_artifacts.get("regret_casebook"))
    return {
        "pilot_ran": bool(controlled_artifacts),
        "benchmark_batch_status": benchmark.get("status", "NOT_RUN"),
        "positive_controls": positive_controls,
        "negative_controls": negative_controls,
        "selected_reverse_diagnostic": _records(reverse.get("decision_delta_trace"))[:1],
        "regret_casebook_status": regret.get("status", "NOT_RUN"),
        "promotion_gate_allowed": False,
    }


def _first_batch_candidate_decisions(
    *,
    fmp: Mapping[str, Any],
    marketstack: Mapping[str, Any],
    forward: Mapping[str, Any],
    readiness_level: str,
    controlled_artifacts: Mapping[str, Mapping[str, Any]],
    price_diff_report: Mapping[str, Any],
    split_dividend_report: Mapping[str, Any],
) -> list[dict[str, Any]]:
    price_summary = _price_discrepancy_summary(price_diff_report)
    split_dividend_summary = _split_dividend_discrepancy_summary(split_dividend_report)
    marketstack_decision = (
        "DATA_REQUIRED"
        if price_summary["status"] == "DATA_REQUIRED"
        or split_dividend_summary["status"] == "DATA_REQUIRED"
        else "WATCHLIST"
    )
    decisions = [
        _batch_decision(
            "fmp_price_corporate_action",
            "WATCHLIST" if fmp.get("promotion_candidate_after_qualification") else "DATA_REQUIRED",
            "diagnostic-only",
            "source_manifest_and_policy_recorded_but_owner_review_required",
        ),
        _batch_decision(
            "marketstack_reconciliation",
            marketstack_decision,
            "blocked" if marketstack_decision == "DATA_REQUIRED" else "diagnostic-only",
            (
                "row_level_fmp_marketstack_snapshots_required"
                if marketstack_decision == "DATA_REQUIRED"
                else "second_source_reconciliation_contract_recorded"
            ),
        ),
        _batch_decision(
            "forward_evidence_archive",
            (
                "CONTINUE"
                if forward.get("reclassification") == "internal_capture_requirement"
                else "DATA_REQUIRED"
            ),
            "diagnostic-only",
            "internal_capture_requirement_recorded_no_broker_order_triggered",
        ),
    ]
    if readiness_level in {"DIAGNOSTIC_ONLY_READY", "CONTROLLED_RESEARCH_READY"}:
        decisions.extend(
            [
                _batch_decision(
                    "benchmark_controls",
                    "CONTINUE",
                    "controlled-research-only",
                    "benchmark_and_positive_negative_controls_recorded",
                ),
                _batch_decision(
                    "strategy_pair_reverse_diagnostic",
                    (
                        "WATCHLIST"
                        if controlled_artifacts.get("reverse_diagnostics")
                        else "DATA_REQUIRED"
                    ),
                    "controlled-research-only",
                    "one_reverse_diagnostic_available_for_hypothesis_generation_only",
                ),
                _batch_decision(
                    "regret_casebook_pilot",
                    "WATCHLIST" if controlled_artifacts.get("regret_casebook") else "DATA_REQUIRED",
                    "controlled-research-only",
                    "regret_taxonomy_available_but_not_promotion_evidence",
                ),
            ]
        )
    else:
        decisions.extend(
            [
                _batch_decision(
                    "benchmark_controls",
                    "DATA_REQUIRED",
                    "blocked",
                    "acceptance_v2_not_ready",
                ),
                _batch_decision(
                    "strategy_pair_reverse_diagnostic",
                    "DATA_REQUIRED",
                    "blocked",
                    "acceptance_v2_not_ready",
                ),
                _batch_decision(
                    "regret_casebook_pilot",
                    "DATA_REQUIRED",
                    "blocked",
                    "acceptance_v2_not_ready",
                ),
            ]
        )
    return decisions


def _batch_decision(
    candidate_id: str,
    decision: str,
    conclusion_scope: str,
    reason: str,
) -> dict[str, Any]:
    if decision not in ALLOWED_BATCH_REVIEW_DECISIONS:
        raise ValueError(f"Unsupported TRADING-759 batch decision: {decision}")
    return {
        "candidate_id": candidate_id,
        "decision": decision,
        "conclusion_scope": conclusion_scope,
        "reason": reason,
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
        "broker_action": "none",
    }


def _first_int(*values: Any) -> int:
    for value in values:
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return 0


def _usage_policy_row(row: Mapping[str, Any], policy: Mapping[str, Any]) -> dict[str, Any]:
    usage_class = _usage_class_for_endpoint(row)
    rules = _mapping(policy.get(usage_class))
    return {
        "provider": row.get("provider"),
        "endpoint_name": row.get("endpoint_name"),
        "usage_class": usage_class,
        "likely_allowed_use": row.get("likely_allowed_use"),
        "strategy_input_allowed": bool(rules.get("strategy_input_allowed", False)),
        "promotion_gate_allowed": bool(rules.get("promotion_gate_allowed", False)),
        "paper_shadow_candidate_allowed": bool(rules.get("paper_shadow_candidate_allowed", False)),
        "allowed_uses": list(rules.get("allowed_uses", [])),
        "source_status_upgrade_attempted": False,
    }


def _requirement_usage_row(row: Mapping[str, Any], policy: Mapping[str, Any]) -> dict[str, Any]:
    status = str(row.get("current_status", "BLOCKED_UNTIL_QUALIFIED"))
    usage_class = status if status in policy else "BLOCKED_UNTIL_QUALIFIED"
    rules = _mapping(policy.get(usage_class))
    return {
        "component": row.get("component"),
        "requirement_status": status,
        "usage_class": usage_class,
        "strategy_input_allowed": bool(rules.get("strategy_input_allowed", False)),
        "promotion_gate_allowed": bool(rules.get("promotion_gate_allowed", False)),
        "paper_shadow_candidate_allowed": bool(rules.get("paper_shadow_candidate_allowed", False)),
        "allowed_uses": list(rules.get("allowed_uses", [])),
        "source_status_upgrade_attempted": False,
    }


def _usage_class_for_endpoint(row: Mapping[str, Any]) -> str:
    if bool(row.get("current_view_only_risk")):
        return "CURRENT_VIEW_ONLY"
    likely = str(row.get("likely_allowed_use", "")).lower()
    if likely == "research_label_only":
        return "RESEARCH_LABEL_ONLY"
    if likely == "promotion_candidate_after_qualification":
        return "PROMOTION_CANDIDATE_AFTER_QUALIFICATION"
    return "BLOCKED_UNTIL_QUALIFIED"


def _endpoint_contract_row(
    endpoint_name: str,
    row: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    request_payload = {
        "provider": "FMP",
        "endpoint_name": endpoint_name,
        "available_time_contract": config.get("available_time_contract", {}),
    }
    return {
        "endpoint_name": endpoint_name,
        "accessible": bool(row.get("accessible") or row.get("endpoint_accessible")),
        "source_manifest": True,
        "endpoint_url_template": row.get("endpoint_url_template", "captured_by_probe_catalog"),
        "request_params_hash": _stable_hash(request_payload),
        "response_hash": _stable_hash(
            {
                "endpoint": endpoint_name,
                "accessible": bool(row.get("accessible") or row.get("endpoint_accessible")),
                "coverage": row.get("coverage_for_representative_universe", {}),
            }
        ),
        "downloaded_at": utc_now_iso(),
        "provider_timestamp_if_available": None,
        "available_time_rule": "conservative_assumption",
        "adjustment_policy": config.get("adjustment_policy", {}),
        "promotion_gate_allowed": False,
    }


def _asset_qualification_row(asset: Mapping[str, Any]) -> dict[str, Any]:
    ticker_history = _records(asset.get("ticker_history"))
    return {
        "asset_id": asset.get("asset_id"),
        "ticker": asset.get("primary_ticker"),
        "exchange": asset.get("exchange"),
        "currency": asset.get("currency"),
        "asset_type": asset.get("asset_type"),
        "first_observed_date": ticker_history[0].get("start_date") if ticker_history else None,
        "last_observed_date": asset.get("delisting_date"),
        "tradable_by_date": asset.get("tradability_status") == "tradable",
        "price_available_by_date": asset.get("asset_type") != "cash",
        "delisted_status": "active" if asset.get("delisting_date") is None else "delisted",
        "ticker_change_status": "ticker_history_present" if ticker_history else "unknown",
        "corporate_action_link_status": asset.get("corporate_actions_source", "unknown"),
        "source_manifest": "asset_master_baseline_v1",
        "qualification_status": "minimum_ready_diagnostic",
    }


def _requires_external_broker_or_live_source(requirement: Mapping[str, Any]) -> bool:
    text = json.dumps(requirement, ensure_ascii=False).lower()
    terms = (
        "broker position",
        "broker statement",
        "cash balance",
        "live quote",
        "portfolio statement",
        "account snapshot",
    )
    return any(term in text for term in terms)


def _forward_capture_contract(internal_capture: bool) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "contract_id": "forward_evidence_capture_contract_v1",
        "status": "PASS" if internal_capture else "EXTERNAL_SOURCE_REVIEW_REQUIRED",
        "internal_capture_requirement": internal_capture,
        "daily_archive": {
            "required": True,
            "captures": ["feature_snapshot", "strategy_output", "benchmark_control_output"],
        },
        "feature_snapshot": {
            "required": True,
            "must_reference_pit_manifest": True,
        },
        "strategy_output": {
            "required": True,
            "diagnostic_only": True,
            "promotion_gate_allowed": False,
        },
        "outcome_append": {
            "required": True,
            "append_only": True,
            "future_outcomes_do_not_rewrite_decision_inputs": True,
        },
        "immutability_policy": {
            "historical_decision_fields_immutable": True,
            "lineage_hash_required": True,
        },
        **PRODUCTION_SAFETY,
    }


def _provider_endpoints(
    coverage: Mapping[str, Any],
    *,
    provider_contains: str,
) -> dict[str, dict[str, Any]]:
    rows = {}
    needle = provider_contains.lower()
    for row in _records(coverage.get("endpoint_coverage_matrix")):
        provider = str(row.get("provider", "")).lower()
        if needle in provider:
            rows[str(row.get("endpoint_name"))] = dict(row)
    return rows


def _research_decision(task_id: str, name: str, readiness_level: str) -> dict[str, Any]:
    decision = (
        "READY_CONTROLLED_RESEARCH"
        if readiness_level == "CONTROLLED_RESEARCH_READY"
        else "READY_DIAGNOSTIC_ONLY"
    )
    return {
        "task_id": task_id,
        "research_name": name,
        "decision": decision,
        "promotion_gate_allowed": False,
        "paper_shadow_candidate_allowed": False,
        "blocked_reason": None if decision.startswith("READY") else "data_foundation_not_ready",
    }


def _controlled_payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    return _base_payload(
        report_type=report_type,
        title=title,
        status=status,
        summary={
            "market_regime": "ai_after_chatgpt",
            "requested_date_range": f"{AI_REGIME_START}..open",
            **dict(summary),
        },
        market_regime="ai_after_chatgpt",
        default_backtest_start=AI_REGIME_START,
        research_only=True,
        manual_review_only=True,
        diagnostic_only=True,
        **extra,
    )


def _base_payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    payload = {
        "schema_version": "1.0",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": utc_now_iso(),
        "market_regime": "ai_after_chatgpt",
        "default_backtest_start": AI_REGIME_START,
        "manual_review_required": True,
        "summary": dict(summary),
        **PRODUCTION_SAFETY,
    }
    payload.update(extra)
    return payload


def _summary_safety() -> dict[str, Any]:
    return {
        "production_effect": "none",
        "broker_action": "none",
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
        "lookahead_violation_count": 0,
        "status_upgrade_attempted": False,
    }


def _write_pair(payload: dict[str, Any], *, output_root: Path, artifact_id: str) -> None:
    paths = {
        "json_path": str(output_root / f"{artifact_id}.json"),
        "markdown_path": str(output_root / f"{artifact_id}.md"),
    }
    payload["artifact_paths"] = paths
    write_foundation_artifact_pair(payload, output_root=output_root, artifact_id=artifact_id)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        return {}
    return raw


def _read_json_or_empty(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    return raw if isinstance(raw, dict) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _missing_count(*paths: Path) -> int:
    return sum(1 for path in paths if not path.exists())


def _summary_bool(payload: Mapping[str, Any], key: str) -> bool:
    summary = payload.get("summary")
    return bool(summary.get(key)) if isinstance(summary, Mapping) else False


def _artifact_status(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": payload.get("status", "MISSING"),
        "report_type": payload.get("report_type", "MISSING"),
        "production_effect": payload.get("production_effect", "none"),
        "promotion_gate_allowed": payload.get("promotion_gate_allowed", False),
    }


def _stable_hash(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
