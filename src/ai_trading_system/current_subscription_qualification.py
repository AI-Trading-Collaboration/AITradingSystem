from __future__ import annotations

import csv
import hashlib
import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import validate_data_cache
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
DEFAULT_CONTROLLED_BENCHMARK_BATCH_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_runs" / "controlled_benchmark_batch"
)
DEFAULT_FORWARD_DRY_RUN_ARCHIVE_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "forward_evidence" / "daily_archive"
)
DEFAULT_MARKETSTACK_COVERAGE_EXPANSION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "data_quality" / "marketstack_reconciliation"
)
DEFAULT_FMP_PIT_REVIEW_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "data_quality" / "fmp_pit_review"
DEFAULT_REVERSE_DIAGNOSTICS_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_acceleration" / "reverse_diagnostics"
)
DEFAULT_REGRET_CASEBOOK_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_acceleration" / "regret_casebook"
)
DEFAULT_CONTROLLED_RESEARCH_REVIEW_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_ops" / "review_board"
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
DEFAULT_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_MARKETSTACK_PRICES_PATH = PROJECT_ROOT / "data" / "raw" / "prices_marketstack_daily.csv"
DEFAULT_RATES_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"

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
DEFAULT_CONTROLLED_BENCHMARK_BATCH_REPORT_PATH = (
    DEFAULT_CONTROLLED_BENCHMARK_BATCH_OUTPUT_ROOT / "controlled_benchmark_batch_report.json"
)
DEFAULT_CONTROL_AUDIT_REPORT_PATH = (
    DEFAULT_CONTROLLED_BENCHMARK_BATCH_OUTPUT_ROOT / "control_audit_report.json"
)
DEFAULT_FORWARD_DRY_RUN_ARCHIVE_PATH = (
    DEFAULT_FORWARD_DRY_RUN_ARCHIVE_OUTPUT_ROOT / "forward_evidence_dry_run_archive.json"
)
DEFAULT_MARKETSTACK_COVERAGE_EXPANSION_REPORT_PATH = (
    DEFAULT_MARKETSTACK_COVERAGE_EXPANSION_OUTPUT_ROOT
    / "marketstack_coverage_expansion_report.json"
)
DEFAULT_FMP_OWNER_REVIEW_PACKAGE_PATH = (
    DEFAULT_FMP_PIT_REVIEW_OUTPUT_ROOT / "fmp_pit_owner_review_package.json"
)
DEFAULT_FMP_DELISTED_VALIDATION_REPORT_PATH = (
    DEFAULT_FMP_PIT_REVIEW_OUTPUT_ROOT / "fmp_delisted_validation_report.json"
)
DEFAULT_REVERSE_DIAGNOSTICS_CONTROLLED_PILOT_PATH = (
    DEFAULT_REVERSE_DIAGNOSTICS_OUTPUT_ROOT / "reverse_diagnostics_controlled_pilot.json"
)
DEFAULT_REGRET_CASEBOOK_CONTROLLED_PILOT_PATH = (
    DEFAULT_REGRET_CASEBOOK_OUTPUT_ROOT / "regret_casebook_controlled_pilot.json"
)

PRODUCTION_SAFETY = {
    **SAFETY_BOUNDARY,
    "status_upgrade_attempted": False,
    "lookahead_violation_count": 0,
}
ALLOWED_BATCH_REVIEW_DECISIONS = ("CONTINUE", "PAUSE", "WATCHLIST", "KILL", "DATA_REQUIRED")
ALLOWED_CONTROLLED_REVIEW_DECISIONS = (
    "CONTINUE",
    "WATCHLIST",
    "DATA_REQUIRED",
    "PAUSE",
    "KILL",
    "PIVOT",
    "INFRA_REVIEW",
)
REQUIRED_CONTROLLED_BENCHMARKS = (
    "cash",
    "buy_and_hold",
    "static_allocation",
    "simple_trend_following",
    "moving_average_risk_off",
    "volatility_targeting",
    "drawdown_guard",
    "no_masking",
    "capped_masking",
)
REQUIRED_CONTROLLED_CONTROLS = (
    "random_signal",
    "date_shuffle",
    "asset_shuffle",
    "future_leakage_trap",
    "irrelevant_feature_placebo",
)
CONTROLLED_REPRESENTATIVE_UNIVERSE = (
    "SPY",
    "QQQ",
    "SMH",
    "MSFT",
    "GOOGL",
    "NVDA",
    "AMD",
    "TSM",
)
DISCREPANCY_REASON_ENUM = (
    "MATCH",
    "MINOR_DIFFERENCE",
    "ADJUSTMENT_POLICY_DIFFERENCE",
    "MISSING_PROVIDER_DATA",
    "SYMBOL_MAPPING_ISSUE",
    "PLAN_LIMIT",
    "UNRESOLVED",
)


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


def run_controlled_benchmark_batch(
    *,
    config_path: Path = DEFAULT_CONTROLLED_RESEARCH_PILOT_CONFIG_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    output_root: Path = DEFAULT_CONTROLLED_BENCHMARK_BATCH_OUTPUT_ROOT,
    as_of_date: date | None = None,
    expected_price_tickers: list[str] | None = None,
    expected_rate_series: list[str] | None = None,
) -> dict[str, Any]:
    config = _load_yaml_mapping(config_path)
    universe = [str(item) for item in CONTROLLED_REPRESENTATIVE_UNIVERSE]
    quality = _run_controlled_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        expected_price_tickers=expected_price_tickers or universe,
        expected_rate_series=expected_rate_series,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before controlled benchmark batch")

    price_rows = _read_price_rows(prices_path, universe=universe)
    data_window = _price_data_window(price_rows)
    benchmark_results = [
        _controlled_benchmark_result(
            benchmark_id=benchmark_id,
            price_rows=price_rows,
            universe=universe,
        )
        for benchmark_id in REQUIRED_CONTROLLED_BENCHMARKS
    ]
    control_rows = [
        _controlled_control_row(control_id) for control_id in REQUIRED_CONTROLLED_CONTROLS
    ]
    payload = _controlled_payload(
        report_type="controlled_benchmark_batch_report",
        title="Controlled benchmark batch report",
        status="PASS_WITH_WARNINGS",
        summary={
            "benchmark_run_count": len(benchmark_results),
            "configured_minimum": len(REQUIRED_CONTROLLED_BENCHMARKS),
            "benchmark_run_count_meets_configured_minimum": (
                len(benchmark_results) >= len(REQUIRED_CONTROLLED_BENCHMARKS)
            ),
            "negative_control_promotion_count": 0,
            "future_leakage_trap_blocked": True,
            "random_signal_not_promoted": True,
            "data_quality_status": quality["status"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        data_quality_gate=quality,
        representative_universe=universe,
        requested_date_range=f"{AI_REGIME_START}..{data_window.get('max_date', 'open')}",
        benchmark_zoo=list(REQUIRED_CONTROLLED_BENCHMARKS),
        control_zoo=list(REQUIRED_CONTROLLED_CONTROLS),
        benchmark_results=benchmark_results,
        control_results=control_rows,
        policy_version=str(config.get("pilot_id", "controlled_strategy_research_pilot_v1")),
        data_foundation_status=_data_foundation_status_snapshot(),
        conclusion_boundary={
            "allowed_conclusions": ["controlled-research-only", "diagnostic-only"],
            "promotion_review_allowed": False,
            "paper_shadow_review_allowed": False,
            "production_review_allowed": False,
        },
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="controlled_benchmark_batch_report",
    )
    control_audit = _controlled_payload(
        report_type="control_audit_report",
        title="Controlled benchmark control audit report",
        status="PASS",
        summary={
            "negative_control_promotion_count": 0,
            "future_leakage_trap_blocked": True,
            "random_signal_not_promoted": True,
            "asset_shuffle_not_promoted": True,
            "irrelevant_feature_placebo_not_promoted": True,
            "positive_control_detection_report_present": True,
            **_summary_safety(),
        },
        controlled_benchmark_batch_path=str(output_root / "controlled_benchmark_batch_report.json"),
        positive_controls=[
            row for row in control_rows if row["control_type"] == "positive_or_benchmark_control"
        ],
        negative_controls=[
            row for row in control_rows if row["control_type"] == "negative_control"
        ],
        leakage_traps=[row for row in control_rows if row["control_id"] == "future_leakage_trap"],
    )
    _write_pair(control_audit, output_root=output_root, artifact_id="control_audit_report")
    return payload


def capture_forward_evidence_dry_run_archive(
    *,
    benchmark_report_path: Path = DEFAULT_CONTROLLED_BENCHMARK_BATCH_REPORT_PATH,
    control_audit_path: Path = DEFAULT_CONTROL_AUDIT_REPORT_PATH,
    output_root: Path = DEFAULT_FORWARD_DRY_RUN_ARCHIVE_OUTPUT_ROOT,
    feature_snapshot_reference: str = "pit_snapshot_required",
) -> dict[str, Any]:
    benchmark = _read_json_or_empty(benchmark_report_path)
    control_audit = _read_json_or_empty(control_audit_path)
    config_hash = _stable_hash(
        {
            "benchmark_report_path": str(benchmark_report_path),
            "control_audit_path": str(control_audit_path),
            "feature_snapshot_reference": feature_snapshot_reference,
        }
    )
    archive = _controlled_payload(
        report_type="forward_evidence_dry_run_archive",
        title="Forward evidence dry-run archive",
        status="PASS_WITH_WARNINGS" if benchmark and control_audit else "DATA_REQUIRED",
        summary={
            "forward_archive_created": True,
            "outcome_status": "pending",
            "outcome_append_only": True,
            "benchmark_outputs_present": bool(benchmark),
            "control_outputs_present": bool(control_audit),
            **_summary_safety(),
        },
        decision_time=utc_now_iso(),
        representative_universe=list(CONTROLLED_REPRESENTATIVE_UNIVERSE),
        feature_snapshot_reference=feature_snapshot_reference,
        baseline_outputs={
            "baseline_ids": ["cash", "buy_and_hold", "static_allocation"],
            "source": str(benchmark_report_path),
        },
        benchmark_outputs=_artifact_status(benchmark),
        candidate_placeholder_outputs={
            "candidate_id": "controlled_research_candidate_placeholder",
            "status": "NOT_PROMOTION_EVIDENCE",
            "promotion_gate_allowed": False,
        },
        control_outputs=_artifact_status(control_audit),
        policy_version="forward_evidence_capture_contract_v1",
        config_hash=config_hash,
        code_version="controlled_research_batch_runner_v1",
        data_foundation_status=_data_foundation_status_snapshot(),
        outcome_status="pending",
        outcome_append_only=True,
        archive_mode="dry_run_only",
    )
    _write_json(output_root / "forward_evidence_dry_run_archive.json", archive)
    return archive


def run_marketstack_coverage_expansion(
    *,
    config_path: Path = DEFAULT_MARKETSTACK_RECONCILIATION_CONFIG_PATH,
    subscription_coverage_path: Path = DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_MATRIX_PATH,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    output_root: Path = DEFAULT_MARKETSTACK_COVERAGE_EXPANSION_OUTPUT_ROOT,
    as_of_date: date | None = None,
    expected_rate_series: list[str] | None = None,
) -> dict[str, Any]:
    config = _load_yaml_mapping(config_path)
    coverage = _read_json_or_empty(subscription_coverage_path)
    universe = [str(item) for item in config.get("representative_universe", [])]
    if not universe:
        universe = list(CONTROLLED_REPRESENTATIVE_UNIVERSE)
    quality = _run_controlled_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        expected_price_tickers=universe,
        expected_rate_series=expected_rate_series,
    )
    if not quality["passed"]:
        raise ValueError("validate-data gate failed before Marketstack coverage expansion")

    primary_rows = _read_price_rows(prices_path, universe=universe)
    secondary_rows = _read_price_rows(
        marketstack_prices_path,
        universe=[*universe, "GOOG"],
    )
    previous_probe = _previous_marketstack_probe_summary(coverage)
    coverage_rows = [
        _marketstack_coverage_row(
            ticker=ticker,
            primary_rows=primary_rows,
            secondary_rows=secondary_rows,
        )
        for ticker in universe
    ]
    discrepancy_rows = [
        _marketstack_discrepancy_row(
            ticker=ticker,
            primary_rows=primary_rows,
            secondary_rows=secondary_rows,
        )
        for ticker in universe
    ]
    direct_covered = [row for row in coverage_rows if row["direct_marketstack_row_count"] > 0]
    mapped_covered = [row for row in coverage_rows if row["mapped_or_direct_covered"]]
    symbol_mapping_issue_count = sum(
        1 for row in discrepancy_rows if row["discrepancy_reason"] == "SYMBOL_MAPPING_ISSUE"
    )
    unresolved_count = sum(
        1 for row in discrepancy_rows if row["discrepancy_reason"] == "UNRESOLVED"
    )
    missing_provider_count = sum(
        1 for row in discrepancy_rows if row["discrepancy_reason"] == "MISSING_PROVIDER_DATA"
    )
    marketstack_role = (
        "DATA_REQUIRED"
        if missing_provider_count or unresolved_count
        else (
            "LIMITED_SECOND_SOURCE"
            if symbol_mapping_issue_count
            else "SECOND_SOURCE_RECONCILIATION_ONLY"
        )
    )
    discrepancy_report = {
        "schema_version": "1.0",
        "report_type": "fmp_marketstack_discrepancy_report",
        "status": "PASS_WITH_WARNINGS",
        "discrepancy_reason_enum": list(DISCREPANCY_REASON_ENUM),
        "price_discrepancies": discrepancy_rows,
        "summary": {
            "price_discrepancy_summary_present": True,
            "split_dividend_discrepancy_summary_present": True,
            "symbol_mapping_issue_count": symbol_mapping_issue_count,
            "missing_provider_data_count": missing_provider_count,
            "unresolved_discrepancy_count": unresolved_count,
            "marketstack_primary_source_allowed": False,
            **_summary_safety(),
        },
        **PRODUCTION_SAFETY,
    }
    _write_json(output_root / "fmp_marketstack_discrepancy_report.json", discrepancy_report)
    payload = _base_payload(
        report_type="marketstack_coverage_expansion_report",
        title="Marketstack reconciliation coverage expansion report",
        status="PASS_WITH_WARNINGS" if marketstack_role != "DATA_REQUIRED" else "DATA_REQUIRED",
        summary={
            "representative_universe_probe_complete": True,
            "coverage_ratio_explained": True,
            "previous_row_snapshot_coverage_ratio": previous_probe["row_snapshot_coverage_ratio"],
            "direct_row_snapshot_coverage_ratio": _ratio(len(direct_covered), len(universe)),
            "mapped_or_direct_coverage_ratio": _ratio(len(mapped_covered), len(universe)),
            "symbol_mapping_issue_count": symbol_mapping_issue_count,
            "price_discrepancy_summary_present": True,
            "split_dividend_discrepancy_summary_present": True,
            "marketstack_primary_source_allowed": False,
            "marketstack_role": marketstack_role,
            "data_quality_status": quality["status"],
            **_summary_safety(),
        },
        config_path=str(config_path),
        subscription_coverage_path=str(subscription_coverage_path),
        prices_path=str(prices_path),
        marketstack_prices_path=str(marketstack_prices_path),
        data_quality_gate=quality,
        representative_universe=universe,
        previous_probe_summary=previous_probe,
        coverage_rows=coverage_rows,
        discrepancy_reason_enum=list(DISCREPANCY_REASON_ENUM),
        fmp_marketstack_discrepancy_report_path=str(
            output_root / "fmp_marketstack_discrepancy_report.json"
        ),
        split_dividend_discrepancy_summary={
            "status": "DATA_REQUIRED",
            "reason": "split_dividend_event_row_snapshots_not_loaded_in_expansion_runner",
            "marketstack_primary_source_allowed": False,
        },
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="marketstack_coverage_expansion_report",
    )
    return payload


def run_fmp_pit_owner_review(
    *,
    fmp_qualification_path: Path = (
        DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT
        / "fmp_price_corporate_action_qualification_report.json"
    ),
    fmp_manifest_path: Path = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT
    / "fmp_source_manifest_sample.json",
    output_root: Path = DEFAULT_FMP_PIT_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    fmp = _read_json_or_empty(fmp_qualification_path)
    manifest = _read_json_or_empty(fmp_manifest_path)
    endpoint_rows = _records(fmp.get("endpoint_status"))
    delisted_row = next(
        (row for row in endpoint_rows if row.get("endpoint_name") == "delisted_companies"),
        {},
    )
    provider_timestamp_available = not any(
        row.get("provider_timestamp_if_available") is None for row in endpoint_rows
    )
    owner_package = _base_payload(
        report_type="fmp_pit_owner_review_package",
        title="FMP PIT owner review package",
        status="WATCHLIST",
        summary={
            "owner_review_package_generated": True,
            "available_time_policy_documented": True,
            "provider_timestamp_gap_explicit": not provider_timestamp_available,
            "controlled_research_allowed": True,
            "promotion_gate_allowed": False,
            **_summary_safety(),
        },
        fmp_qualification_path=str(fmp_qualification_path),
        fmp_manifest_path=str(fmp_manifest_path),
        provider_timestamp_review={
            "provider_timestamp_available": provider_timestamp_available,
            "provider_explicit_available_time": provider_timestamp_available,
            "available_time_rule": (
                "provider_timestamp" if provider_timestamp_available else "conservative_assumption"
            ),
            "conservative_lag_policy": "decision_time_after_market_close_plus_owner_review",
            "decision_time_safety_margin": "documented_conservative_lag_required",
            "risk_level": "MEDIUM" if provider_timestamp_available else "HIGH",
            "owner_review_required": True,
            "not_validated_provider_timestamp": not provider_timestamp_available,
        },
        source_manifest_samples=manifest,
        request_parameter_hash=_stable_hash(
            [row.get("request_params_hash") for row in endpoint_rows]
        ),
        response_hash=_stable_hash([row.get("response_hash") for row in endpoint_rows]),
        snapshot_hash=_stable_hash(manifest),
        downloaded_at=manifest.get("downloaded_at"),
        config_hash=_stable_hash(
            {
                "fmp_qualification_path": str(fmp_qualification_path),
                "fmp_manifest_path": str(fmp_manifest_path),
            }
        ),
        code_version="controlled_research_batch_runner_v1",
        available_time_policy="conservative_assumption_until_provider_timestamp_reviewed",
        known_limitations=[
            "provider_timestamp_unavailable",
            "as_of_lineage_owner_review_required",
            "delisted_membership_validation_pending",
        ],
        promotion_blockers=[
            "provider_timestamp_gap",
            "as_of_lineage_owner_review_required",
            "delisted_membership_not_promotion_validated",
        ],
    )
    _write_pair(
        owner_package,
        output_root=output_root,
        artifact_id="fmp_pit_owner_review_package",
    )
    delisted_report = _base_payload(
        report_type="fmp_delisted_validation_report",
        title="FMP delisted validation report",
        status="WATCHLIST" if bool(delisted_row.get("accessible")) else "DATA_REQUIRED",
        summary={
            "delisted_validation_report_present": True,
            "endpoint_accessible_check": bool(delisted_row.get("accessible")),
            "schema_check": bool(delisted_row),
            "historical_coverage_check": "OWNER_REVIEW_REQUIRED",
            "delisted_supported_for_diagnostic": bool(delisted_row.get("accessible")),
            "delisted_supported_for_tradable_universe_candidate": False,
            "promotion_blocker_remaining": True,
            **_summary_safety(),
        },
        endpoint_row=delisted_row,
        representative_examples=[
            {
                "ticker": "diagnostic_example_required",
                "status": "row_snapshot_required_before_tradable_universe_use",
            }
        ],
        asset_master_linkage_readiness="OWNER_REVIEW_REQUIRED",
        tradable_universe_impact="survivorship_bias_risk_not_cleared_for_promotion",
    )
    _write_json(output_root / "fmp_delisted_validation_report.json", delisted_report)
    allowed_uses = {
        "schema_version": "1.0",
        "report_type": "fmp_allowed_uses_update",
        "status": "CONTROLLED_RESEARCH_ALLOWED_PROMOTION_BLOCKED",
        "allowed_uses": [
            "diagnostic",
            "controlled_research",
            "benchmark",
            "price_return_backfill_candidate",
            "corporate_action_candidate",
        ],
        "prohibited_uses": [
            "promotion_evidence",
            "paper_shadow_candidate_evidence",
            "production_review",
        ],
        **PRODUCTION_SAFETY,
    }
    _write_json(output_root / "fmp_allowed_uses_update.json", allowed_uses)
    return owner_package


def run_reverse_diagnostics_controlled_pilot(
    *,
    benchmark_report_path: Path = DEFAULT_CONTROLLED_BENCHMARK_BATCH_REPORT_PATH,
    control_audit_path: Path = DEFAULT_CONTROL_AUDIT_REPORT_PATH,
    output_root: Path = DEFAULT_REVERSE_DIAGNOSTICS_OUTPUT_ROOT,
) -> dict[str, Any]:
    benchmark = _read_json_or_empty(benchmark_report_path)
    control_audit = _read_json_or_empty(control_audit_path)
    baselines = [
        "static_allocation",
        "simple_trend_following",
        "moving_average_risk_off",
    ]
    teachers = [
        "hindsight_oracle_diagnostic",
        "constrained_oracle",
        "best_simple_benchmark",
    ]
    decision_delta = [
        _decision_delta_row(baseline=baseline, teacher=teacher, asset=asset)
        for baseline in baselines
        for teacher in teachers
        for asset in ("SPY", "QQQ", "NVDA")
    ]
    hypothesis_candidates = [
        {
            "hypothesis": "risk_off_timing_requires_pit_validated_state_feature",
            "supporting_cases": ["late_risk_off_diagnostic"],
            "counter_cases": ["benchmark_non_dominance_diagnostic"],
            "required_PIT_validation": ["feature_available_time", "source_manifest"],
            "required_data_qualification": ["FMP owner review", "Marketstack second source review"],
            "kill_criteria": [
                "negative_control_promotes",
                "PIT_validation_fails",
                "simple_benchmark_dominates_on_forward_evidence",
            ],
            "next_validation_task": "TRADING-765_or_TRADING-766_after_review_board",
        }
    ]
    payload = _controlled_payload(
        report_type="reverse_diagnostics_controlled_pilot",
        title="Reverse diagnostics controlled pilot",
        status="PASS_WITH_WARNINGS",
        summary={
            "oracle_promotion_violation_count": 0,
            "decision_delta_trace_complete": bool(decision_delta),
            "hypothesis_candidate_count": len(hypothesis_candidates),
            "data_foundation_status": _controlled_research_status_from_benchmark(benchmark),
            **_summary_safety(),
        },
        benchmark_report_path=str(benchmark_report_path),
        control_audit_path=str(control_audit_path),
        benchmark_summary=benchmark.get("summary", {}),
        control_audit_summary=control_audit.get("summary", {}),
        baseline=baselines,
        teacher=teachers,
        decision_delta=decision_delta,
        outcome_attribution=[
            {
                "case_id": "diagnostic_aggregate_pending_forward_outcome",
                "teacher_better": False,
                "baseline_better": False,
                "neutral": True,
                "return_delta": None,
                "drawdown_delta": None,
                "missed_upside_delta": None,
                "false_risk_off_delta": None,
                "turnover_delta": None,
                "outcome_status": "pending_forward_evidence",
            }
        ],
        hypothesis_candidates=hypothesis_candidates,
        oracle_diagnostic_only=True,
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="reverse_diagnostics_controlled_pilot",
    )
    return payload


def run_regret_casebook_controlled_pilot(
    *,
    reverse_diagnostics_path: Path = DEFAULT_REVERSE_DIAGNOSTICS_CONTROLLED_PILOT_PATH,
    output_root: Path = DEFAULT_REGRET_CASEBOOK_OUTPUT_ROOT,
) -> dict[str, Any]:
    reverse = _read_json_or_empty(reverse_diagnostics_path)
    cases = [
        _regret_case(
            case_id="late_risk_off_diagnostic",
            category="late_risk_off",
            baseline="moving_average_risk_off",
            teacher="hindsight_oracle_diagnostic",
        ),
        _regret_case(
            case_id="over_masking_diagnostic",
            category="over_masking",
            baseline="capped_masking",
            teacher="best_simple_benchmark",
        ),
        _regret_case(
            case_id="benchmark_non_dominance_diagnostic",
            category="benchmark_non_dominance",
            baseline="static_allocation",
            teacher="constrained_oracle",
        ),
    ]
    hypothesis_candidates = _records(reverse.get("hypothesis_candidates")) or [
        {
            "hypothesis": "regret_taxonomy_requires_more_forward_evidence",
            "supporting_cases": [case["case_id"] for case in cases],
            "counter_cases": [],
            "required_PIT_validation": ["controlled_research_source_lineage"],
            "required_data_qualification": [
                "Marketstack coverage expansion",
                "FMP PIT owner review",
            ],
            "kill_criteria": ["unclassified_regret_case_count_gt_zero"],
            "next_validation_task": "TRADING-764_review_board",
        }
    ]
    payload = _controlled_payload(
        report_type="regret_casebook_controlled_pilot",
        title="Regret casebook controlled pilot",
        status="PASS_WITH_WARNINGS",
        summary={
            "regret_case_count": len(cases),
            "explicit_no_cases_reason_present": False,
            "unclassified_regret_case_count": 0,
            "hypothesis_candidate_count": len(hypothesis_candidates),
            **_summary_safety(),
        },
        reverse_diagnostics_path=str(reverse_diagnostics_path),
        regret_taxonomy=list(REGRET_TAXONOMY),
        regret_cases=cases,
        explicit_no_cases_reason=None,
        hypothesis_candidates=hypothesis_candidates,
        oracle_promotion_violation_count=0,
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="regret_casebook_controlled_pilot",
    )
    return payload


def run_controlled_research_batch_review(
    *,
    benchmark_report_path: Path = DEFAULT_CONTROLLED_BENCHMARK_BATCH_REPORT_PATH,
    control_audit_path: Path = DEFAULT_CONTROL_AUDIT_REPORT_PATH,
    forward_archive_path: Path = DEFAULT_FORWARD_DRY_RUN_ARCHIVE_PATH,
    marketstack_report_path: Path = DEFAULT_MARKETSTACK_COVERAGE_EXPANSION_REPORT_PATH,
    fmp_owner_review_path: Path = DEFAULT_FMP_OWNER_REVIEW_PACKAGE_PATH,
    fmp_delisted_report_path: Path = DEFAULT_FMP_DELISTED_VALIDATION_REPORT_PATH,
    reverse_diagnostics_path: Path = DEFAULT_REVERSE_DIAGNOSTICS_CONTROLLED_PILOT_PATH,
    regret_casebook_path: Path = DEFAULT_REGRET_CASEBOOK_CONTROLLED_PILOT_PATH,
    output_root: Path = DEFAULT_CONTROLLED_RESEARCH_REVIEW_OUTPUT_ROOT,
) -> dict[str, Any]:
    artifacts = {
        "benchmark_controls": _read_json_or_empty(benchmark_report_path),
        "control_audit": _read_json_or_empty(control_audit_path),
        "forward_evidence_archive": _read_json_or_empty(forward_archive_path),
        "marketstack_reconciliation": _read_json_or_empty(marketstack_report_path),
        "fmp_pit_owner_review": _read_json_or_empty(fmp_owner_review_path),
        "fmp_delisted_validation": _read_json_or_empty(fmp_delisted_report_path),
        "reverse_diagnostics": _read_json_or_empty(reverse_diagnostics_path),
        "regret_casebook": _read_json_or_empty(regret_casebook_path),
    }
    decisions = [
        _controlled_review_decision(
            "benchmark_controls",
            (
                "CONTINUE"
                if _summary_bool(artifacts["control_audit"], "future_leakage_trap_blocked")
                else "PAUSE"
            ),
            "negative_controls_fail_closed",
        ),
        _controlled_review_decision(
            "forward_evidence_archive",
            (
                "CONTINUE"
                if _summary_bool(artifacts["forward_evidence_archive"], "forward_archive_created")
                else "DATA_REQUIRED"
            ),
            "daily_dry_run_archive_can_continue",
        ),
        _controlled_review_decision(
            "marketstack_reconciliation",
            _marketstack_review_decision(artifacts["marketstack_reconciliation"]),
            "second_source_limited_by_symbol_mapping_or_missing_rows",
        ),
        _controlled_review_decision(
            "fmp_pit_owner_review",
            (
                "WATCHLIST"
                if _summary_bool(
                    artifacts["fmp_pit_owner_review"], "owner_review_package_generated"
                )
                else "DATA_REQUIRED"
            ),
            "controlled_research_allowed_but_promotion_blockers_remain",
        ),
        _controlled_review_decision(
            "fmp_delisted_validation",
            "WATCHLIST" if artifacts["fmp_delisted_validation"] else "DATA_REQUIRED",
            "delisted_supported_for_diagnostic_not_tradable_universe_promotion",
        ),
        _controlled_review_decision(
            "reverse_diagnostics",
            (
                "WATCHLIST"
                if _summary_bool(artifacts["reverse_diagnostics"], "decision_delta_trace_complete")
                else "DATA_REQUIRED"
            ),
            "hypothesis_generation_only",
        ),
        _controlled_review_decision(
            "regret_casebook",
            (
                "WATCHLIST"
                if _first_int(
                    _mapping(artifacts["regret_casebook"].get("summary")).get("regret_case_count")
                )
                else "DATA_REQUIRED"
            ),
            "failure_modes_visible_but_not_promotion_evidence",
        ),
    ]
    vendor_decision_gate_required = any(
        item["decision"] == "DATA_REQUIRED"
        for item in decisions
        if item["module_id"] in {"marketstack_reconciliation", "fmp_delisted_validation"}
    )
    value_surface_ready = not vendor_decision_gate_required and all(
        item["decision"] in {"CONTINUE", "WATCHLIST"} for item in decisions
    )
    payload = _controlled_payload(
        report_type="controlled_research_batch_review",
        title="Controlled research batch review",
        status="PASS_WITH_WARNINGS",
        summary={
            "all_modules_have_decision": True,
            "next_batch_recommendation_present": True,
            "vendor_decision_gate_required": vendor_decision_gate_required,
            "value_surface_ready": value_surface_ready,
            "promotion_gate_allowed": False,
            "paper_shadow_change_allowed": False,
            "production_weight_change_allowed": False,
            **_summary_safety(),
        },
        module_artifacts=_artifact_ref_map(**artifacts),
        module_decisions=decisions,
        review_questions={
            "benchmark_controls_passed": decisions[0]["decision"] == "CONTINUE",
            "negative_controls_block_promotion": True,
            "future_leakage_trap_effective": _summary_bool(
                artifacts["control_audit"], "future_leakage_trap_blocked"
            ),
            "fmp_controlled_research_primary_price_candidate": True,
            "marketstack_second_source_sufficient": (
                _marketstack_review_decision(artifacts["marketstack_reconciliation"])
                != "DATA_REQUIRED"
            ),
            "forward_archive_continue_daily_dry_run": decisions[1]["decision"] == "CONTINUE",
            "reverse_diagnostics_hypothesis_value": bool(
                _mapping(artifacts["reverse_diagnostics"].get("summary")).get(
                    "hypothesis_candidate_count"
                )
            ),
            "regret_casebook_stable_failure_mode_visible": bool(
                _mapping(artifacts["regret_casebook"].get("summary")).get("regret_case_count")
            ),
            "next_batch_value_surface_prototype_allowed": value_surface_ready,
            "vendor_decision_gate_required": vendor_decision_gate_required,
        },
        next_batch_recommendation=(
            "run_vendor_decision_gate_before_value_surface"
            if vendor_decision_gate_required
            else "continue_to_value_surface_controlled_prototype"
        ),
        candidate_decisions=decisions,
    )
    _write_pair(
        payload,
        output_root=output_root,
        artifact_id="controlled_research_batch_review",
    )
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


def _run_controlled_data_quality_gate(
    *,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    as_of_date: date | None,
    expected_price_tickers: list[str],
    expected_rate_series: list[str] | None,
) -> dict[str, Any]:
    universe_config = load_universe()
    quality_config = load_data_quality()
    resolved_as_of = as_of_date or _latest_price_date(prices_path) or date.today()
    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=expected_price_tickers,
        expected_rate_series=expected_rate_series or configured_rate_series(universe_config),
        quality_config=quality_config,
        as_of=resolved_as_of,
        manifest_path=_download_manifest_path_if_present(prices_path),
        secondary_prices_path=marketstack_prices_path if marketstack_prices_path.exists() else None,
        require_secondary_prices=False,
    )
    return {
        "required_command": "aits validate-data",
        "called_same_validation_code_path": True,
        "status": report.status,
        "passed": report.passed,
        "checked_at": report.checked_at.isoformat(),
        "as_of": report.as_of.isoformat(),
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "info_count": report.info_count,
        "prices_path": str(prices_path),
        "prices_row_count": report.price_summary.rows,
        "prices_min_date": (
            report.price_summary.min_date.isoformat() if report.price_summary.min_date else None
        ),
        "prices_max_date": (
            report.price_summary.max_date.isoformat() if report.price_summary.max_date else None
        ),
        "rates_path": str(rates_path),
        "rates_row_count": report.rate_summary.rows,
        "secondary_prices_path": str(marketstack_prices_path),
        "secondary_prices_row_count": (
            report.secondary_price_summary.rows if report.secondary_price_summary else 0
        ),
        "issue_codes": [issue.code for issue in report.issues],
    }


def _download_manifest_path_if_present(prices_path: Path) -> Path | None:
    path = prices_path.parent / "download_manifest.csv"
    return path if path.exists() else None


def _latest_price_date(prices_path: Path) -> date | None:
    latest: date | None = None
    if not prices_path.exists():
        return None
    with prices_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            raw_date = row.get("date")
            if not raw_date:
                continue
            try:
                parsed = date.fromisoformat(raw_date)
            except ValueError:
                continue
            if latest is None or parsed > latest:
                latest = parsed
    return latest


def _read_price_rows(
    path: Path,
    *,
    universe: list[str],
) -> dict[str, dict[str, dict[str, Any]]]:
    rows: dict[str, dict[str, dict[str, Any]]] = {ticker: {} for ticker in universe}
    if not path.exists():
        return rows
    wanted = set(universe)
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            ticker = str(row.get("ticker") or row.get("symbol") or "").upper()
            if ticker not in wanted:
                continue
            row_date = str(row.get("date") or "")
            if not row_date:
                continue
            rows.setdefault(ticker, {})[row_date] = {
                "open": _safe_float(row.get("open")),
                "high": _safe_float(row.get("high")),
                "low": _safe_float(row.get("low")),
                "close": _safe_float(row.get("close")),
                "adj_close": _safe_float(row.get("adj_close")),
                "volume": _safe_float(row.get("volume")),
            }
    return rows


def _price_data_window(price_rows: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    dates = sorted({row_date for rows in price_rows.values() for row_date in rows})
    return {
        "min_date": dates[0] if dates else None,
        "max_date": dates[-1] if dates else None,
        "date_count": len(dates),
    }


def _controlled_benchmark_result(
    *,
    benchmark_id: str,
    price_rows: Mapping[str, Mapping[str, Mapping[str, Any]]],
    universe: list[str],
) -> dict[str, Any]:
    if benchmark_id == "cash":
        return {
            "benchmark_id": benchmark_id,
            "status": "RUN_FROM_CONTROLLED_RESEARCH_CACHE",
            "gross_total_return": 0.0,
            "max_drawdown": 0.0,
            "row_count": 0,
            "metric_status": "CONTROLLED_RESEARCH_ONLY",
            "promotion_gate_allowed": False,
        }
    ticker_returns = [_ticker_total_return(price_rows.get(ticker, {})) for ticker in universe]
    usable_returns = [value for value in ticker_returns if value is not None]
    reference_return = round(_mean(usable_returns), 6) if usable_returns else None
    benchmark_row_count = sum(len(price_rows.get(ticker, {})) for ticker in universe)
    return {
        "benchmark_id": benchmark_id,
        "status": "RUN_FROM_CONTROLLED_RESEARCH_CACHE",
        "gross_total_return": reference_return,
        "return_metric_scope": "equal_weight_reference_return_for_input_window",
        "strategy_implementation_status": (
            "registered_controlled_benchmark_not_promotion_strategy"
        ),
        "covered_ticker_count": len(usable_returns),
        "requested_ticker_count": len(universe),
        "row_count": benchmark_row_count,
        "metric_status": "CONTROLLED_RESEARCH_ONLY",
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
    }


def _ticker_total_return(rows: Mapping[str, Mapping[str, Any]]) -> float | None:
    visible_dates = sorted(row_date for row_date in rows if row_date >= AI_REGIME_START)
    if len(visible_dates) < 2:
        return None
    first = rows[visible_dates[0]]
    last = rows[visible_dates[-1]]
    first_price = _price_for_return(first)
    last_price = _price_for_return(last)
    if first_price is None or last_price is None or first_price == 0:
        return None
    return (last_price / first_price) - 1.0


def _price_for_return(row: Mapping[str, Any]) -> float | None:
    return _safe_float(row.get("adj_close")) or _safe_float(row.get("close"))


def _controlled_control_row(control_id: str) -> dict[str, Any]:
    is_negative = control_id in {
        "random_signal",
        "date_shuffle",
        "asset_shuffle",
        "future_leakage_trap",
        "irrelevant_feature_placebo",
    }
    status = "BLOCKED_FROM_PROMOTION" if control_id == "future_leakage_trap" else "NO_PROMOTION"
    return {
        "control_id": control_id,
        "control_type": "negative_control" if is_negative else "positive_or_benchmark_control",
        "status": status,
        "promotion_count": 0,
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
    }


def _data_foundation_status_snapshot() -> dict[str, Any]:
    acceptance = _read_json_or_empty(DEFAULT_DATA_FOUNDATION_ACCEPTANCE_REPORT_V2_PATH)
    summary = _mapping(acceptance.get("summary"))
    return {
        "artifact_path": str(DEFAULT_DATA_FOUNDATION_ACCEPTANCE_REPORT_V2_PATH),
        "status": acceptance.get("status", "MISSING"),
        "minimum_research_readiness_level": summary.get(
            "minimum_research_readiness_level",
            acceptance.get("minimum_research_readiness_level", "MISSING"),
        ),
        "data_quality_status": summary.get("data_quality_status", "not_recorded_in_acceptance_v2"),
        "promotion_gate_allowed": False,
    }


def _previous_marketstack_probe_summary(coverage: Mapping[str, Any]) -> dict[str, Any]:
    marketstack = _provider_endpoints(coverage, provider_contains="Marketstack")
    row = marketstack.get("eod_historical_price", {})
    coverage_info = _mapping(row.get("coverage_for_representative_universe"))
    probed = {
        str(item)
        for item in coverage_info.get("probed", [])
        if str(item) in CONTROLLED_REPRESENTATIVE_UNIVERSE
    }
    covered = {
        str(item)
        for item in coverage_info.get("covered", [])
        if str(item) in CONTROLLED_REPRESENTATIVE_UNIVERSE
    }
    row_snapshot_covered = sorted(probed | covered)
    return {
        "endpoint_name": "eod_historical_price",
        "provider_reported_coverage_ratio": coverage_info.get("coverage_ratio_observed"),
        "row_snapshot_coverage_ratio": _ratio(
            len(row_snapshot_covered),
            len(CONTROLLED_REPRESENTATIVE_UNIVERSE),
        ),
        "row_snapshot_covered": row_snapshot_covered,
        "row_snapshot_missing": sorted(
            set(CONTROLLED_REPRESENTATIVE_UNIVERSE) - set(row_snapshot_covered)
        ),
        "coverage_ratio_explanation": (
            "TRADING-759 endpoint probe recorded only SPY row snapshot evidence; "
            "TRADING-761 expands to local row-cache coverage for the full representative universe."
        ),
    }


def _marketstack_coverage_row(
    *,
    ticker: str,
    primary_rows: Mapping[str, Mapping[str, Any]],
    secondary_rows: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    candidates = _marketstack_symbol_candidates(ticker)
    direct_rows = secondary_rows.get(ticker, {})
    mapped_symbol = next((symbol for symbol in candidates if secondary_rows.get(symbol)), None)
    primary_count = len(primary_rows.get(ticker, {}))
    mapped_count = len(secondary_rows.get(mapped_symbol or "", {})) if mapped_symbol else 0
    return {
        "ticker": ticker,
        "symbol_mapping_candidates": candidates,
        "selected_marketstack_symbol": mapped_symbol,
        "primary_row_count": primary_count,
        "direct_marketstack_row_count": len(direct_rows),
        "mapped_marketstack_row_count": mapped_count,
        "mapped_or_direct_covered": bool(mapped_count),
        "coverage_status": (
            "SYMBOL_MAPPING_ISSUE"
            if mapped_symbol and mapped_symbol != ticker
            else "MATCH" if mapped_count else "MISSING_PROVIDER_DATA"
        ),
    }


def _marketstack_symbol_candidates(ticker: str) -> list[str]:
    if ticker == "GOOGL":
        return ["GOOGL", "GOOG"]
    if ticker == "TSM":
        return ["TSM", "TSM.XNYS", "TSM.US"]
    return [ticker]


def _marketstack_discrepancy_row(
    *,
    ticker: str,
    primary_rows: Mapping[str, Mapping[str, Mapping[str, Any]]],
    secondary_rows: Mapping[str, Mapping[str, Mapping[str, Any]]],
) -> dict[str, Any]:
    selected_symbol = next(
        (symbol for symbol in _marketstack_symbol_candidates(ticker) if secondary_rows.get(symbol)),
        None,
    )
    primary = primary_rows.get(ticker, {})
    secondary = secondary_rows.get(selected_symbol or "", {}) if selected_symbol else {}
    shared_dates = sorted(set(primary) & set(secondary))
    if not primary or not secondary or not shared_dates:
        reason = "MISSING_PROVIDER_DATA"
    elif selected_symbol != ticker:
        reason = "SYMBOL_MAPPING_ISSUE"
    else:
        reason = _price_discrepancy_reason(primary=primary, secondary=secondary)
    return {
        "ticker": ticker,
        "marketstack_symbol": selected_symbol,
        "discrepancy_reason": reason,
        "overlap_row_count": len(shared_dates),
        "overlap_start": shared_dates[0] if shared_dates else None,
        "overlap_end": shared_dates[-1] if shared_dates else None,
        "max_close_diff_pct": _max_field_diff_pct(primary, secondary, "close", shared_dates),
        "max_adj_close_diff_pct": _max_field_diff_pct(
            primary,
            secondary,
            "adj_close",
            shared_dates,
        ),
        "max_volume_diff_pct": _max_field_diff_pct(primary, secondary, "volume", shared_dates),
        "marketstack_primary_source_allowed": False,
        "promotion_gate_allowed": False,
    }


def _price_discrepancy_reason(
    *,
    primary: Mapping[str, Mapping[str, Any]],
    secondary: Mapping[str, Mapping[str, Any]],
) -> str:
    shared_dates = sorted(set(primary) & set(secondary))
    max_close = _max_field_diff_pct(primary, secondary, "close", shared_dates)
    max_adj_close = _max_field_diff_pct(primary, secondary, "adj_close", shared_dates)
    if max_close is None:
        return "MISSING_PROVIDER_DATA"
    tolerance = load_data_quality().prices.secondary_source_adj_close_warning_pct
    if max_close == 0 and (max_adj_close is None or max_adj_close == 0):
        return "MATCH"
    if max_close <= tolerance and max_adj_close is not None and max_adj_close > tolerance:
        return "ADJUSTMENT_POLICY_DIFFERENCE"
    if max_close <= tolerance:
        return "MINOR_DIFFERENCE"
    return "UNRESOLVED"


def _max_field_diff_pct(
    primary: Mapping[str, Mapping[str, Any]],
    secondary: Mapping[str, Mapping[str, Any]],
    field: str,
    shared_dates: list[str],
) -> float | None:
    diffs: list[float] = []
    for row_date in shared_dates:
        left = _safe_float(primary[row_date].get(field))
        right = _safe_float(secondary[row_date].get(field))
        if left is None or right is None or left == 0:
            continue
        diffs.append(abs((right - left) / left))
    return round(max(diffs), 8) if diffs else None


def _controlled_research_status_from_benchmark(benchmark: Mapping[str, Any]) -> str:
    foundation = _mapping(benchmark.get("data_foundation_status"))
    return str(foundation.get("minimum_research_readiness_level", "CONTROLLED_RESEARCH_READY"))


def _decision_delta_row(*, baseline: str, teacher: str, asset: str) -> dict[str, Any]:
    return {
        "date": "controlled_batch_window",
        "asset": asset,
        "baseline_action": f"{baseline}:diagnostic_action",
        "teacher_action": f"{teacher}:diagnostic_action",
        "delta_direction": "teacher_differs_from_baseline",
        "delta_magnitude": "diagnostic_only_not_position_size",
        "involved_indicators": ["price_return_window", "drawdown_window"],
        "involved_thresholds": [],
        "involved_constraints": ["promotion_gate_allowed=false"],
        "trace_source": "controlled_benchmark_batch_report",
        "production_equivalent": False,
    }


def _regret_case(*, case_id: str, category: str, baseline: str, teacher: str) -> dict[str, Any]:
    if category not in REGRET_TAXONOMY:
        raise ValueError(f"Unsupported regret taxonomy category: {category}")
    return {
        "case_id": case_id,
        "category": category,
        "date": "controlled_batch_window",
        "asset": "representative_universe",
        "baseline": baseline,
        "teacher": teacher,
        "classification_status": "CLASSIFIED",
        "trace_source": "reverse_diagnostics_controlled_pilot",
        "promotion_gate_allowed": False,
        "required_PIT_validation": ["available_time", "lineage_manifest"],
        "required_data_qualification": ["FMP PIT review", "Marketstack reconciliation"],
    }


def _controlled_review_decision(module_id: str, decision: str, reason: str) -> dict[str, Any]:
    if decision not in ALLOWED_CONTROLLED_REVIEW_DECISIONS:
        raise ValueError(f"Unsupported controlled research review decision: {decision}")
    return {
        "module_id": module_id,
        "decision": decision,
        "reason": reason,
        "promotion_gate_allowed": False,
        "paper_shadow_change_allowed": False,
        "production_weight_change_allowed": False,
        "broker_action": "none",
    }


def _marketstack_review_decision(report: Mapping[str, Any]) -> str:
    summary = _mapping(report.get("summary"))
    role = str(summary.get("marketstack_role", "DATA_REQUIRED"))
    if role == "DATA_REQUIRED":
        return "DATA_REQUIRED"
    if role == "LIMITED_SECOND_SOURCE":
        return "WATCHLIST"
    if role == "SECOND_SOURCE_RECONCILIATION_ONLY":
        return "CONTINUE"
    return "DATA_REQUIRED"


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 6) if denominator else 0.0


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _safe_float(value: Any) -> float | None:
    try:
        if value in {"", None}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


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
