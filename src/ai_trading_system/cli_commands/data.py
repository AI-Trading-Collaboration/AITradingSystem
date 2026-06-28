from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.cache_catalog import (
    DEFAULT_CACHE_CATALOG_DIR,
    DEFAULT_CACHE_CATALOG_POLICY_PATH,
    build_and_write_cache_catalog,
    load_cache_catalog_payload,
    load_cache_catalog_policy,
    resolve_cache_catalog_path,
    validate_cache_catalog_artifact,
)
from ai_trading_system.cache_catalog import (
    DEFAULT_DATA_REFRESH_AUDIT_DIR as CACHE_CATALOG_REFRESH_AUDIT_DIR,
)
from ai_trading_system.cache_catalog import (
    DEFAULT_VALIDATE_DATA_AUDIT_DIR as CACHE_CATALOG_VALIDATE_DATA_AUDIT_DIR,
)
from ai_trading_system.cli_commands.data_artifacts import (
    _parse_date,
    _resolve_market_data_freshness_path,
    _resolve_market_data_refresh_path,
)
from ai_trading_system.config import (
    DEFAULT_DATA_SOURCES_CONFIG_PATH,
    PROJECT_ROOT,
    load_data_sources,
)
from ai_trading_system.current_subscription_qualification import (
    DEFAULT_ASSET_MASTER_QUALIFICATION_CONFIG_PATH,
    DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
    DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_MATRIX_PATH,
    DEFAULT_CURRENT_SUBSCRIPTION_QUALIFICATION_BATCH_OUTPUT_ROOT,
    DEFAULT_DATA_SOURCE_USAGE_POLICY_PATH,
    DEFAULT_FMP_PIT_REVIEW_OUTPUT_ROOT,
    DEFAULT_FMP_PRICE_CORPORATE_ACTION_CONFIG_PATH,
    DEFAULT_FMP_WATCHLIST_CLOSURE_REPORT_PATH,
    DEFAULT_MACRO_RISK_CONFIG_PATH,
    DEFAULT_MARKETSTACK_COVERAGE_EXPANSION_OUTPUT_ROOT,
    DEFAULT_MARKETSTACK_DATA_REQUIRED_CLOSURE_REPORT_PATH,
    DEFAULT_MARKETSTACK_DISCREPANCY_REPORT_PATH,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_MARKETSTACK_RECONCILIATION_CONFIG_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_SEC_FUNDAMENTAL_PIT_CONFIG_PATH,
    DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
    run_asset_master_qualification,
    run_data_foundation_acceptance_v2,
    run_data_source_usage_guardrails,
    run_data_vendor_decision_gate,
    run_first_current_subscription_source_qualification_batch,
    run_fmp_pit_owner_review,
    run_fmp_price_corporate_action_qualification,
    run_fmp_watchlist_owner_review_closure,
    run_macro_risk_source_qualification,
    run_marketstack_coverage_expansion,
    run_marketstack_data_required_closure,
    run_marketstack_reconciliation_qualification,
    run_sec_fundamental_pit_qualification,
)
from ai_trading_system.data_foundation import (
    DEFAULT_ASSET_MASTER_OUTPUT_ROOT,
    DEFAULT_DATA_FOUNDATION_ACCEPTANCE_OUTPUT_ROOT,
    DEFAULT_DATA_FOUNDATION_ACCEPTANCE_REPORT_PATH,
    DEFAULT_DATA_FOUNDATION_ACCEPTANCE_SUMMARY_UPDATED_PATH,
    DEFAULT_DATA_FOUNDATION_REMEDIATION_PLAN_PATH,
    DEFAULT_DATA_SOURCE_QUALIFICATION_MATRIX_PATH,
    DEFAULT_DATA_SOURCE_QUALIFICATION_MATRIX_UPDATED_PATH,
    DEFAULT_DATA_SOURCE_QUALIFICATION_OUTPUT_ROOT,
    DEFAULT_DATA_SOURCE_REMEDIATION_EXECUTION_OUTPUT_ROOT,
    DEFAULT_DATA_SOURCE_REMEDIATION_EXECUTION_REPORT_PATH,
    DEFAULT_DATA_SOURCE_REMEDIATION_ITEM_RESULTS_PATH,
    DEFAULT_DATA_SOURCE_REQUIREMENTS_OUTPUT_ROOT,
    DEFAULT_PIT_FEATURE_STORE_OUTPUT_ROOT,
    audit_pit_feature_snapshot,
    audit_universe,
    build_pit_feature_snapshot,
    build_tradability_calendar,
    query_pit_feature,
    run_data_foundation_acceptance,
    run_data_source_qualification_remediation,
    run_data_source_remediation_execution,
    run_data_source_requirement_matrix,
    show_universe,
    validate_asset_master,
)
from ai_trading_system.data_refresh_audit import (
    DEFAULT_DATA_REFRESH_AUDIT_DIR,
    DEFAULT_VALIDATION_AUDIT_DIR,
    build_and_write_data_refresh_audit,
    load_data_refresh_audit_payload,
    resolve_data_refresh_audit_path,
    validate_data_refresh_audit_artifact,
)
from ai_trading_system.data_source_fallback_policy import (
    DEFAULT_DATA_SOURCE_FALLBACK_DIR,
    DEFAULT_DATA_SOURCE_FALLBACK_POLICY_PATH,
    build_and_write_data_source_fallback_policy,
    latest_data_source_fallback_policy_summary,
    load_data_source_fallback_policy,
    load_data_source_fallback_policy_payload,
    resolve_data_source_fallback_policy_path,
    validate_data_source_fallback_policy_artifact,
)
from ai_trading_system.data_source_subscription_audit import (
    DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_OUTPUT_ROOT,
    DEFAULT_SOURCE_REQUIREMENT_MATRIX_PATH,
    run_current_subscription_data_coverage_audit,
)
from ai_trading_system.free_pit_data_sources import (
    DEFAULT_FREE_DATA_SOURCE_REGISTRY_PATH,
    DEFAULT_FREE_FEATURE_OUTPUT_ROOT,
    DEFAULT_FREE_FEATURE_POLICY_PATH,
    DEFAULT_FREE_SOURCE_OUTPUT_ROOT,
    DEFAULT_MANIFEST_PATH,
    DEFAULT_PARTICIPATION_PROXY_REGISTRY_PATH,
    DEFAULT_RESEARCH_DOCS_ROOT,
    DEFAULT_RESEARCH_INPUTS_ROOT,
    run_free_data_source_ingestion,
    run_free_data_source_validation,
)
from ai_trading_system.free_pit_data_sources import (
    DEFAULT_MARKETSTACK_PRICES_PATH as DEFAULT_FREE_MARKETSTACK_PRICES_PATH,
)
from ai_trading_system.free_pit_data_sources import (
    DEFAULT_PRICES_PATH as DEFAULT_FREE_PRICES_PATH,
)
from ai_trading_system.free_pit_data_sources import (
    DEFAULT_RATES_PATH as DEFAULT_FREE_RATES_PATH,
)
from ai_trading_system.trading_engine.backtest_input_diagnostics import (
    run_backtest_input_diagnostics,
)
from ai_trading_system.trading_engine.data.price_history_repair import (
    build_price_history_repair_provider,
    repair_backtest_price_history,
)
from ai_trading_system.trading_engine.data_registry_consistency import (
    run_data_registry_consistency,
    validate_backtest_manifest_consistency,
)
from ai_trading_system.trading_engine.market_data_freshness import (
    DEFAULT_MARKET_DATA_FRESHNESS_CONFIG_PATH,
    load_market_data_freshness_payload,
    run_market_data_freshness,
    validate_market_data_freshness_payload,
)
from ai_trading_system.trading_engine.market_data_refresh import (
    DEFAULT_MARKET_DATA_REFRESH_CONFIG_PATH,
    load_market_data_refresh_payload,
    run_market_data_refresh,
    validate_market_data_refresh_payload,
)
from ai_trading_system.trading_engine.parameters import DEFAULT_SHADOW_BACKTEST_CONFIG_PATH
from ai_trading_system.trading_engine.price_cache_reconcile import (
    refresh_backtest_manifest,
    run_price_cache_reconcile,
)
from ai_trading_system.vendor_adapters.norgate_connector import (
    DEFAULT_NORGATE_TRIAL_OUTPUT_ROOT,
    run_norgate_membership_probe,
    run_norgate_trial_pack,
    run_norgate_trial_smoke_test,
)
from ai_trading_system.vendor_adapters.norgate_connector import (
    DEFAULT_RESEARCH_DOCS_ROOT as DEFAULT_NORGATE_RESEARCH_DOCS_ROOT,
)
from ai_trading_system.vendor_adapters.norgate_connector import (
    DEFAULT_RESEARCH_INPUTS_ROOT as DEFAULT_NORGATE_RESEARCH_INPUTS_ROOT,
)
from ai_trading_system.vendor_adapters.norgate_partial_effectiveness import (
    DEFAULT_PARTIAL_EFFECTIVENESS_POLICY_PATH,
    run_norgate_trial_partial_effectiveness,
)
from ai_trading_system.vendor_adapters.norgate_partial_evidence_review import (
    DEFAULT_PARTIAL_EVIDENCE_REVIEW_POLICY_PATH,
    run_norgate_2y_partial_evidence_review,
)

console = Console()
data_app = typer.Typer(help="缓存数据诊断和 backtest input repair planning。", no_args_is_help=True)
refresh_audit_app = typer.Typer(help="Data refresh audit trail 治理报告。")
fallback_policy_app = typer.Typer(help="Data source fallback policy 治理报告。")
cache_catalog_app = typer.Typer(help="Checksum and cache catalog 治理报告。")
pit_feature_store_app = typer.Typer(help="PIT feature store and snapshot registry。")
asset_master_app = typer.Typer(help="Asset master and tradability calendar。")
universe_app = typer.Typer(help="Research universe as-of view and audit。")
foundation_acceptance_app = typer.Typer(help="TRADING-734 data foundation acceptance。")
source_qualification_app = typer.Typer(help="Data source qualification remediation。")
free_sources_app = typer.Typer(help="Free PIT data source ingestion and validation。")
norgate_app = typer.Typer(help="Norgate trial access and summary-only probes。")
data_app.add_typer(refresh_audit_app, name="refresh-audit")
data_app.add_typer(fallback_policy_app, name="fallback-policy")
data_app.add_typer(cache_catalog_app, name="cache-catalog")
data_app.add_typer(pit_feature_store_app, name="pit-feature-store")
data_app.add_typer(asset_master_app, name="asset-master")
data_app.add_typer(universe_app, name="universe")
data_app.add_typer(foundation_acceptance_app, name="foundation-acceptance")
data_app.add_typer(source_qualification_app, name="source-qualification")
data_app.add_typer(free_sources_app, name="free-sources")
data_app.add_typer(norgate_app, name="norgate")


@norgate_app.command("trial-smoke-test")
def norgate_trial_smoke_test_command(
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Norgate trial derived output root。")
    ] = DEFAULT_NORGATE_TRIAL_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root", help="Research docs root。")
    ] = DEFAULT_NORGATE_RESEARCH_DOCS_ROOT,
    inputs_root: Annotated[
        Path, typer.Option("--inputs-root", help="Research review inputs root。")
    ] = DEFAULT_NORGATE_RESEARCH_INPUTS_ROOT,
) -> None:
    payload = run_norgate_trial_smoke_test(
        output_root=output_root,
        docs_root=docs_root,
        inputs_root=inputs_root,
    )
    console.print(
        "Norgate trial smoke test："
        f"{payload.get('status')}；raw_vendor_data_committed="
        f"{payload.get('raw_vendor_data_committed', False)}"
    )


@norgate_app.command("membership-probe")
def norgate_membership_probe_command(
    index_id: Annotated[str, typer.Option("--index", help="Index alias, e.g. nasdaq100 or $NDX。")]
    = "nasdaq100",
    dates: Annotated[
        str | None,
        typer.Option("--dates", help="Comma-separated YYYY-MM-DD anchor dates。"),
    ] = None,
    max_symbols: Annotated[
        int,
        typer.Option(
            "--max-symbols",
            help="0 means all symbols when live Norgate package/database are available。",
        ),
    ] = 0,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Norgate trial derived output root。")
    ] = DEFAULT_NORGATE_TRIAL_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root", help="Research docs root。")
    ] = DEFAULT_NORGATE_RESEARCH_DOCS_ROOT,
    inputs_root: Annotated[
        Path, typer.Option("--inputs-root", help="Research review inputs root。")
    ] = DEFAULT_NORGATE_RESEARCH_INPUTS_ROOT,
) -> None:
    requested_dates = _parse_norgate_dates(dates)
    payload = run_norgate_membership_probe(
        index_id=index_id,
        requested_dates=requested_dates,
        output_root=output_root,
        docs_root=docs_root,
        inputs_root=inputs_root,
        max_symbols=max_symbols,
    )
    console.print(
        "Norgate membership probe："
        f"{payload.get('status')}；query_success_count="
        f"{payload.get('query_success_count', 0)}"
    )


@norgate_app.command("trial-pack")
def norgate_trial_pack_command(
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Norgate trial derived output root。")
    ] = DEFAULT_NORGATE_TRIAL_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root", help="Research docs root。")
    ] = DEFAULT_NORGATE_RESEARCH_DOCS_ROOT,
    inputs_root: Annotated[
        Path, typer.Option("--inputs-root", help="Research review inputs root。")
    ] = DEFAULT_NORGATE_RESEARCH_INPUTS_ROOT,
) -> None:
    payload = run_norgate_trial_pack(
        output_root=output_root,
        docs_root=docs_root,
        inputs_root=inputs_root,
    )
    console.print(
        "Norgate trial pack："
        f"{payload.get('status')}；promotion_allowed={payload.get('promotion_allowed')}"
    )


@norgate_app.command("partial-effectiveness")
def norgate_partial_effectiveness_command(
    index_id: Annotated[str, typer.Option("--index", help="Index alias, e.g. nasdaq100 or $NDX。")]
    = "nasdaq100",
    max_symbols: Annotated[
        int,
        typer.Option(
            "--max-symbols",
            help="Debug limit for membership scan; 0 means all US equity symbols。",
        ),
    ] = 0,
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Norgate trial derived output root。")
    ] = DEFAULT_NORGATE_TRIAL_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root", help="Research docs root。")
    ] = DEFAULT_NORGATE_RESEARCH_DOCS_ROOT,
    inputs_root: Annotated[
        Path, typer.Option("--inputs-root", help="Research review inputs root。")
    ] = DEFAULT_NORGATE_RESEARCH_INPUTS_ROOT,
    policy_path: Annotated[
        Path,
        typer.Option("--policy", help="Norgate partial effectiveness diagnostic policy。"),
    ] = DEFAULT_PARTIAL_EFFECTIVENESS_POLICY_PATH,
) -> None:
    payload = run_norgate_trial_partial_effectiveness(
        index_id=index_id,
        output_root=output_root,
        docs_root=docs_root,
        inputs_root=inputs_root,
        policy_path=policy_path,
        max_symbols=max_symbols,
    )
    console.print(
        "Norgate partial effectiveness："
        f"{payload.get('status')}；"
        f"source_feature_useful_2y={payload.get('source_feature_useful_2y')}；"
        f"promotion_allowed={payload.get('promotion_allowed')}"
    )


@norgate_app.command("partial-evidence-review")
def norgate_partial_evidence_review_command(
    output_root: Annotated[
        Path, typer.Option("--output-root", help="Norgate trial derived output root。")
    ] = DEFAULT_NORGATE_TRIAL_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root", help="Research docs root。")
    ] = DEFAULT_NORGATE_RESEARCH_DOCS_ROOT,
    inputs_root: Annotated[
        Path, typer.Option("--inputs-root", help="Research review inputs root。")
    ] = DEFAULT_NORGATE_RESEARCH_INPUTS_ROOT,
    policy_path: Annotated[
        Path,
        typer.Option("--policy", help="Norgate 2Y partial evidence review policy。"),
    ] = DEFAULT_PARTIAL_EVIDENCE_REVIEW_POLICY_PATH,
) -> None:
    payload = run_norgate_2y_partial_evidence_review(
        output_root=output_root,
        docs_root=docs_root,
        inputs_root=inputs_root,
        policy_path=policy_path,
    )
    console.print(
        "Norgate partial evidence review："
        f"{payload.get('status')}；"
        f"local_signal_evidence_reason={payload.get('local_signal_evidence_reason')}；"
        f"purchase_platinum_recommendation="
        f"{payload.get('purchase_platinum_recommendation')}；"
        f"promotion_allowed={payload.get('promotion_allowed')}"
    )


def _parse_norgate_dates(value: str | None) -> list[date]:
    if not value:
        return [
            date(2024, 8, 5),
            date(2024, 11, 6),
            date(2025, 4, 7),
            date(2025, 8, 1),
            date(2026, 1, 2),
            date(2026, 6, 26),
        ]
    return [_parse_date(item.strip()) for item in value.split(",") if item.strip()]


@free_sources_app.command("ingest")
def free_sources_ingest_command(
    registry_path: Annotated[
        Path, typer.Option("--registry", help="Free data source registry YAML。")
    ] = DEFAULT_FREE_DATA_SOURCE_REGISTRY_PATH,
    feature_policy_path: Annotated[
        Path, typer.Option("--feature-policy", help="Free feature policy YAML。")
    ] = DEFAULT_FREE_FEATURE_POLICY_PATH,
    participation_proxy_registry_path: Annotated[
        Path,
        typer.Option("--participation-registry", help="Participation proxy registry YAML。"),
    ] = DEFAULT_PARTICIPATION_PROXY_REGISTRY_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_FREE_RATES_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_FREE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path | None, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_FREE_MARKETSTACK_PRICES_PATH,
    manifest_path: Annotated[Path, typer.Option("--manifest-path")] = DEFAULT_MANIFEST_PATH,
    output_root: Annotated[Path, typer.Option("--output-root")] = DEFAULT_FREE_SOURCE_OUTPUT_ROOT,
    feature_output_root: Annotated[
        Path, typer.Option("--feature-output-root")
    ] = DEFAULT_FREE_FEATURE_OUTPUT_ROOT,
    docs_root: Annotated[Path, typer.Option("--docs-root")] = DEFAULT_RESEARCH_DOCS_ROOT,
    inputs_root: Annotated[Path, typer.Option("--inputs-root")] = DEFAULT_RESEARCH_INPUTS_ROOT,
    calendar_input_path: Annotated[
        Path | None,
        typer.Option("--calendar-input", help="Optional CSV/YAML official macro calendar rows。"),
    ] = None,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_free_data_source_ingestion(
        registry_path=registry_path,
        feature_policy_path=feature_policy_path,
        participation_proxy_registry_path=participation_proxy_registry_path,
        rates_path=rates_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        manifest_path=manifest_path,
        output_root=output_root,
        feature_output_root=feature_output_root,
        docs_root=docs_root,
        inputs_root=inputs_root,
        calendar_input_path=calendar_input_path,
        as_of_date=_parse_date(as_of) if as_of else None,
    )
    _print_free_source_payload(payload)


@free_sources_app.command("validate")
def free_sources_validate_command(
    registry_path: Annotated[
        Path, typer.Option("--registry", help="Free data source registry YAML。")
    ] = DEFAULT_FREE_DATA_SOURCE_REGISTRY_PATH,
    participation_proxy_registry_path: Annotated[
        Path,
        typer.Option("--participation-registry", help="Participation proxy registry YAML。"),
    ] = DEFAULT_PARTICIPATION_PROXY_REGISTRY_PATH,
) -> None:
    payload = run_free_data_source_validation(
        registry_path=registry_path,
        participation_proxy_registry_path=participation_proxy_registry_path,
    )
    _print_free_source_payload(payload)
    if payload.get("status") == "FAIL":
        raise typer.Exit(code=1)


@foundation_acceptance_app.command("run")
def foundation_acceptance_run_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Data foundation acceptance 输出目录。"),
    ] = DEFAULT_DATA_FOUNDATION_ACCEPTANCE_OUTPUT_ROOT,
    include_qualified_sources: Annotated[
        bool,
        typer.Option(
            "--include-qualified-sources",
            help="生成 TRADING-748 data foundation acceptance v2 和 minimum readiness。",
        ),
    ] = False,
) -> None:
    if include_qualified_sources:
        payload = run_data_foundation_acceptance_v2(output_root=output_root)
    else:
        payload = run_data_foundation_acceptance(output_root=output_root)
    _print_foundation_payload(payload)


@source_qualification_app.command("remediate")
def source_qualification_remediate_command(
    acceptance_report: Annotated[
        Path,
        typer.Option("--acceptance-report", help="TRADING-734 acceptance report JSON。"),
    ] = DEFAULT_DATA_FOUNDATION_ACCEPTANCE_REPORT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Source qualification remediation 输出目录。"),
    ] = DEFAULT_DATA_SOURCE_QUALIFICATION_OUTPUT_ROOT,
) -> None:
    payload = run_data_source_qualification_remediation(
        acceptance_report_path=acceptance_report,
        output_root=output_root,
    )
    _print_foundation_payload(payload)


@source_qualification_app.command("execute-remediation")
def source_qualification_execute_remediation_command(
    acceptance_report: Annotated[
        Path,
        typer.Option("--acceptance-report", help="TRADING-734 acceptance report JSON。"),
    ] = DEFAULT_DATA_FOUNDATION_ACCEPTANCE_REPORT_PATH,
    qualification_matrix: Annotated[
        Path,
        typer.Option("--qualification-matrix", help="TRADING-735 qualification matrix JSON。"),
    ] = DEFAULT_DATA_SOURCE_QUALIFICATION_MATRIX_PATH,
    remediation_plan: Annotated[
        Path,
        typer.Option("--remediation-plan", help="TRADING-735 remediation plan JSON。"),
    ] = DEFAULT_DATA_FOUNDATION_REMEDIATION_PLAN_PATH,
    updated_acceptance_summary: Annotated[
        Path,
        typer.Option(
            "--updated-acceptance-summary",
            help="TRADING-735 updated acceptance summary JSON。",
        ),
    ] = DEFAULT_DATA_FOUNDATION_ACCEPTANCE_SUMMARY_UPDATED_PATH,
    acceptance_output_root: Annotated[
        Path,
        typer.Option(
            "--acceptance-output-root",
            help="重新运行 data foundation acceptance 的输出目录。",
        ),
    ] = DEFAULT_DATA_FOUNDATION_ACCEPTANCE_OUTPUT_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-736 remediation execution 输出目录。"),
    ] = DEFAULT_DATA_SOURCE_REMEDIATION_EXECUTION_OUTPUT_ROOT,
) -> None:
    payload = run_data_source_remediation_execution(
        acceptance_report_path=acceptance_report,
        qualification_matrix_path=qualification_matrix,
        remediation_plan_path=remediation_plan,
        updated_acceptance_summary_path=updated_acceptance_summary,
        acceptance_output_root=acceptance_output_root,
        output_root=output_root,
    )
    _print_foundation_payload(payload)


@source_qualification_app.command("requirements")
def source_qualification_requirements_command(
    remediation_execution_report: Annotated[
        Path,
        typer.Option(
            "--remediation-execution-report",
            help="TRADING-736 remediation execution report JSON。",
        ),
    ] = DEFAULT_DATA_SOURCE_REMEDIATION_EXECUTION_REPORT_PATH,
    remediation_item_results: Annotated[
        Path,
        typer.Option("--remediation-item-results", help="TRADING-736 item results JSON。"),
    ] = DEFAULT_DATA_SOURCE_REMEDIATION_ITEM_RESULTS_PATH,
    qualification_matrix_updated: Annotated[
        Path,
        typer.Option(
            "--qualification-matrix-updated",
            help="TRADING-736 updated qualification matrix JSON。",
        ),
    ] = DEFAULT_DATA_SOURCE_QUALIFICATION_MATRIX_UPDATED_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-737 source requirement matrix 输出目录。"),
    ] = DEFAULT_DATA_SOURCE_REQUIREMENTS_OUTPUT_ROOT,
) -> None:
    payload = run_data_source_requirement_matrix(
        remediation_execution_report_path=remediation_execution_report,
        remediation_item_results_path=remediation_item_results,
        qualification_matrix_updated_path=qualification_matrix_updated,
        output_root=output_root,
    )
    _print_foundation_payload(payload)


@source_qualification_app.command("subscription-audit")
def source_qualification_subscription_audit_command(
    source_requirement_matrix: Annotated[
        Path,
        typer.Option(
            "--source-requirement-matrix",
            help="TRADING-737 source requirement matrix JSON。",
        ),
    ] = DEFAULT_SOURCE_REQUIREMENT_MATRIX_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-738 subscription coverage 输出目录。"),
    ] = DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_OUTPUT_ROOT,
    timeout_seconds: Annotated[
        float,
        typer.Option("--timeout-seconds", help="单个 endpoint probe HTTP timeout 秒数。"),
    ] = 10.0,
) -> None:
    payload = run_current_subscription_data_coverage_audit(
        source_requirement_matrix_path=source_requirement_matrix,
        output_root=output_root,
        timeout_seconds=timeout_seconds,
    )
    _print_foundation_payload(payload)


@source_qualification_app.command("usage-guardrails")
def source_qualification_usage_guardrails_command(
    policy_path: Annotated[
        Path,
        typer.Option("--policy", help="Data source usage policy YAML。"),
    ] = DEFAULT_DATA_SOURCE_USAGE_POLICY_PATH,
    subscription_coverage: Annotated[
        Path,
        typer.Option("--subscription-coverage", help="TRADING-738 coverage matrix JSON。"),
    ] = DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_MATRIX_PATH,
    source_requirement_matrix: Annotated[
        Path,
        typer.Option("--source-requirement-matrix", help="TRADING-737 requirement matrix JSON。"),
    ] = DEFAULT_SOURCE_REQUIREMENT_MATRIX_PATH,
    qualification_matrix_updated: Annotated[
        Path,
        typer.Option("--qualification-matrix-updated", help="TRADING-736 updated matrix JSON。"),
    ] = DEFAULT_DATA_SOURCE_QUALIFICATION_MATRIX_UPDATED_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-739 usage guardrails 输出目录。"),
    ] = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
) -> None:
    payload = run_data_source_usage_guardrails(
        policy_path=policy_path,
        subscription_coverage_path=subscription_coverage,
        source_requirement_matrix_path=source_requirement_matrix,
        qualification_matrix_updated_path=qualification_matrix_updated,
        output_root=output_root,
    )
    _print_foundation_payload(payload)


@source_qualification_app.command("fmp-price-corporate-action")
def source_qualification_fmp_price_corporate_action_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="FMP price/corporate-action qualification config。"),
    ] = DEFAULT_FMP_PRICE_CORPORATE_ACTION_CONFIG_PATH,
    subscription_coverage: Annotated[
        Path,
        typer.Option("--subscription-coverage", help="TRADING-738 coverage matrix JSON。"),
    ] = DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_MATRIX_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-740 FMP qualification 输出目录。"),
    ] = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
) -> None:
    payload = run_fmp_price_corporate_action_qualification(
        config_path=config_path,
        subscription_coverage_path=subscription_coverage,
        output_root=output_root,
    )
    _print_foundation_payload(payload)


@source_qualification_app.command("fmp-pit-owner-review")
def source_qualification_fmp_pit_owner_review_command(
    fmp_qualification: Annotated[
        Path,
        typer.Option("--fmp-qualification", help="FMP qualification report JSON。"),
    ] = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT
    / "fmp_price_corporate_action_qualification_report.json",
    fmp_manifest: Annotated[
        Path,
        typer.Option("--fmp-manifest", help="FMP source manifest sample JSON。"),
    ] = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT
    / "fmp_source_manifest_sample.json",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-762 FMP PIT review 输出目录。"),
    ] = DEFAULT_FMP_PIT_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = run_fmp_pit_owner_review(
        fmp_qualification_path=fmp_qualification,
        fmp_manifest_path=fmp_manifest,
        output_root=output_root,
    )
    _print_foundation_payload(payload)


@source_qualification_app.command("fmp-watchlist-closure")
def source_qualification_fmp_watchlist_closure_command(
    fmp_owner_review: Annotated[
        Path,
        typer.Option("--fmp-owner-review", help="TRADING-762 FMP owner review JSON。"),
    ] = DEFAULT_FMP_WATCHLIST_CLOSURE_REPORT_PATH.parent
    / "fmp_pit_owner_review_package.json",
    fmp_delisted_report: Annotated[
        Path,
        typer.Option("--fmp-delisted-report", help="TRADING-762 delisted validation JSON。"),
    ] = DEFAULT_FMP_WATCHLIST_CLOSURE_REPORT_PATH.parent
    / "fmp_delisted_validation_report.json",
    fmp_allowed_uses: Annotated[
        Path,
        typer.Option("--fmp-allowed-uses", help="TRADING-762 allowed uses JSON。"),
    ] = DEFAULT_FMP_WATCHLIST_CLOSURE_REPORT_PATH.parent
    / "fmp_allowed_uses_update.json",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-767 FMP closure 输出目录。"),
    ] = DEFAULT_FMP_PIT_REVIEW_OUTPUT_ROOT,
) -> None:
    payload = run_fmp_watchlist_owner_review_closure(
        fmp_owner_review_path=fmp_owner_review,
        fmp_delisted_report_path=fmp_delisted_report,
        fmp_allowed_uses_path=fmp_allowed_uses,
        output_root=output_root,
    )
    _print_foundation_payload(payload)


@source_qualification_app.command("marketstack-reconciliation")
def source_qualification_marketstack_reconciliation_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Marketstack reconciliation qualification config。"),
    ] = DEFAULT_MARKETSTACK_RECONCILIATION_CONFIG_PATH,
    subscription_coverage: Annotated[
        Path,
        typer.Option("--subscription-coverage", help="TRADING-738 coverage matrix JSON。"),
    ] = DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_MATRIX_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-741 reconciliation 输出目录。"),
    ] = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
) -> None:
    payload = run_marketstack_reconciliation_qualification(
        config_path=config_path,
        subscription_coverage_path=subscription_coverage,
        output_root=output_root,
    )
    _print_foundation_payload(payload)


@source_qualification_app.command("marketstack-coverage-expansion")
def source_qualification_marketstack_coverage_expansion_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Marketstack reconciliation config。"),
    ] = DEFAULT_MARKETSTACK_RECONCILIATION_CONFIG_PATH,
    subscription_coverage: Annotated[
        Path,
        typer.Option("--subscription-coverage", help="TRADING-738 coverage matrix JSON。"),
    ] = DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_MATRIX_PATH,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="FMP 主价格缓存 CSV。"),
    ] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="Marketstack 第二源价格缓存 CSV。"),
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-761 Marketstack expansion 输出目录。"),
    ] = DEFAULT_MARKETSTACK_COVERAGE_EXPANSION_OUTPUT_ROOT,
) -> None:
    payload = run_marketstack_coverage_expansion(
        config_path=config_path,
        subscription_coverage_path=subscription_coverage,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=_parse_date(as_of) if as_of else None,
        output_root=output_root,
    )
    _print_foundation_payload(payload)


@source_qualification_app.command("marketstack-data-required-closure")
def source_qualification_marketstack_data_required_closure_command(
    marketstack_report: Annotated[
        Path,
        typer.Option("--marketstack-report", help="TRADING-761 Marketstack expansion JSON。"),
    ] = DEFAULT_MARKETSTACK_DATA_REQUIRED_CLOSURE_REPORT_PATH.parent
    / "marketstack_coverage_expansion_report.json",
    discrepancy_report: Annotated[
        Path,
        typer.Option("--discrepancy-report", help="FMP/Marketstack discrepancy JSON。"),
    ] = DEFAULT_MARKETSTACK_DISCREPANCY_REPORT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-766 Marketstack closure 输出目录。"),
    ] = DEFAULT_MARKETSTACK_COVERAGE_EXPANSION_OUTPUT_ROOT,
) -> None:
    payload = run_marketstack_data_required_closure(
        marketstack_report_path=marketstack_report,
        discrepancy_report_path=discrepancy_report,
        output_root=output_root,
    )
    _print_foundation_payload(payload)


@source_qualification_app.command("asset-master")
def source_qualification_asset_master_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Asset master qualification config。"),
    ] = DEFAULT_ASSET_MASTER_QUALIFICATION_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-742 asset master qualification 输出目录。"),
    ] = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
) -> None:
    payload = run_asset_master_qualification(config_path=config_path, output_root=output_root)
    _print_foundation_payload(payload)


@source_qualification_app.command("sec-fundamental-pit")
def source_qualification_sec_fundamental_pit_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="SEC fundamental PIT qualification config。"),
    ] = DEFAULT_SEC_FUNDAMENTAL_PIT_CONFIG_PATH,
    subscription_coverage: Annotated[
        Path,
        typer.Option("--subscription-coverage", help="TRADING-738 coverage matrix JSON。"),
    ] = DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_MATRIX_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-744 SEC qualification 输出目录。"),
    ] = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
) -> None:
    payload = run_sec_fundamental_pit_qualification(
        config_path=config_path,
        subscription_coverage_path=subscription_coverage,
        output_root=output_root,
    )
    _print_foundation_payload(payload)


@source_qualification_app.command("macro-risk")
def source_qualification_macro_risk_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="FRED/Cboe macro-risk qualification config。"),
    ] = DEFAULT_MACRO_RISK_CONFIG_PATH,
    subscription_coverage: Annotated[
        Path,
        typer.Option("--subscription-coverage", help="TRADING-738 coverage matrix JSON。"),
    ] = DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_MATRIX_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-745 macro-risk qualification 输出目录。"),
    ] = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
) -> None:
    payload = run_macro_risk_source_qualification(
        config_path=config_path,
        subscription_coverage_path=subscription_coverage,
        output_root=output_root,
    )
    _print_foundation_payload(payload)


@source_qualification_app.command("vendor-decision-gate")
def source_qualification_vendor_decision_gate_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-758 vendor decision gate 输出目录。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
) -> None:
    payload = run_data_vendor_decision_gate(output_root=output_root)
    _print_foundation_payload(payload)


@source_qualification_app.command("first-batch")
def source_qualification_first_batch_command(
    subscription_coverage: Annotated[
        Path,
        typer.Option("--subscription-coverage", help="TRADING-738 coverage matrix JSON。"),
    ] = DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_MATRIX_PATH,
    source_requirement_matrix: Annotated[
        Path,
        typer.Option("--source-requirement-matrix", help="TRADING-737 requirement matrix JSON。"),
    ] = DEFAULT_SOURCE_REQUIREMENT_MATRIX_PATH,
    qualification_matrix_updated: Annotated[
        Path,
        typer.Option("--qualification-matrix-updated", help="TRADING-736 updated matrix JSON。"),
    ] = DEFAULT_DATA_SOURCE_QUALIFICATION_MATRIX_UPDATED_PATH,
    source_output_root: Annotated[
        Path,
        typer.Option("--source-output-root", help="TRADING-739～747 source artifacts 输出目录。"),
    ] = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
    acceptance_output_root: Annotated[
        Path,
        typer.Option("--acceptance-output-root", help="TRADING-748 acceptance v2 输出目录。"),
    ] = DEFAULT_DATA_FOUNDATION_ACCEPTANCE_OUTPUT_ROOT,
    controlled_output_root: Annotated[
        Path,
        typer.Option("--controlled-output-root", help="小型 controlled strategy pilot 输出目录。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_RESEARCH_OUTPUT_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-759 batch review 输出目录。"),
    ] = DEFAULT_CURRENT_SUBSCRIPTION_QUALIFICATION_BATCH_OUTPUT_ROOT,
) -> None:
    payload = run_first_current_subscription_source_qualification_batch(
        subscription_coverage_path=subscription_coverage,
        source_requirement_matrix_path=source_requirement_matrix,
        qualification_matrix_updated_path=qualification_matrix_updated,
        source_output_root=source_output_root,
        acceptance_output_root=acceptance_output_root,
        controlled_output_root=controlled_output_root,
        output_root=output_root,
    )
    _print_foundation_payload(payload)


@pit_feature_store_app.command("build-snapshot")
def pit_feature_store_build_snapshot_command(
    as_of_date: Annotated[str, typer.Option("--as-of-date", help="Snapshot as-of date。")],
    decision_time: Annotated[str, typer.Option("--decision-time", help="Decision timestamp。")],
    asset_universe: Annotated[
        str,
        typer.Option("--asset-universe", help="Universe id。"),
    ] = "data_foundation_minimum",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="PIT feature store 输出目录。"),
    ] = DEFAULT_PIT_FEATURE_STORE_OUTPUT_ROOT,
) -> None:
    payload = build_pit_feature_snapshot(
        as_of_date=as_of_date,
        decision_time=decision_time,
        asset_universe=asset_universe,
        output_root=output_root,
    )
    _print_foundation_payload(payload)


@pit_feature_store_app.command("audit")
def pit_feature_store_audit_command(
    snapshot_id: Annotated[str, typer.Option("--snapshot-id", help="Snapshot id。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="PIT feature store 输出目录。"),
    ] = DEFAULT_PIT_FEATURE_STORE_OUTPUT_ROOT,
) -> None:
    payload = audit_pit_feature_snapshot(snapshot_id=snapshot_id, output_root=output_root)
    _print_foundation_payload(payload)


@pit_feature_store_app.command("query")
def pit_feature_store_query_command(
    feature_id: Annotated[str, typer.Option("--feature-id", help="Feature id。")],
    asset_id: Annotated[str, typer.Option("--asset-id", help="Asset id。")],
    as_of_date: Annotated[str, typer.Option("--as-of-date", help="As-of date。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="PIT feature store 输出目录。"),
    ] = DEFAULT_PIT_FEATURE_STORE_OUTPUT_ROOT,
) -> None:
    payload = query_pit_feature(
        feature_id=feature_id,
        asset_id=asset_id,
        as_of_date=as_of_date,
        output_root=output_root,
    )
    _print_foundation_payload(payload)


@asset_master_app.command("validate")
def asset_master_validate_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Asset master 输出目录。"),
    ] = DEFAULT_ASSET_MASTER_OUTPUT_ROOT,
) -> None:
    payload = validate_asset_master(output_root=output_root)
    _print_foundation_payload(payload)


@asset_master_app.command("build-tradability-calendar")
def asset_master_build_tradability_calendar_command(
    universe: Annotated[
        str,
        typer.Option("--universe", help="Universe id。"),
    ] = "data_foundation_minimum",
    date_range: Annotated[
        str,
        typer.Option("--date-range", help="Date range start:end。"),
    ] = "2022-12-01:2022-12-01",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Asset master 输出目录。"),
    ] = DEFAULT_ASSET_MASTER_OUTPUT_ROOT,
) -> None:
    payload = build_tradability_calendar(
        universe=universe,
        date_range=date_range,
        output_root=output_root,
    )
    _print_foundation_payload(payload)


@universe_app.command("show")
def universe_show_command(
    universe: Annotated[str, typer.Option("--universe", help="Universe id。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Asset master 输出目录。"),
    ] = DEFAULT_ASSET_MASTER_OUTPUT_ROOT,
) -> None:
    payload = show_universe(universe=universe, output_root=output_root)
    _print_foundation_payload(payload)


@universe_app.command("audit")
def universe_audit_command(
    universe: Annotated[str, typer.Option("--universe", help="Universe id。")],
    date_range: Annotated[
        str,
        typer.Option("--date-range", help="Date range start:end。"),
    ] = "2022-12-01:2022-12-01",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Asset master 输出目录。"),
    ] = DEFAULT_ASSET_MASTER_OUTPUT_ROOT,
) -> None:
    payload = audit_universe(universe=universe, date_range=date_range, output_root=output_root)
    _print_foundation_payload(payload)


@data_app.command("diagnose-backtest-inputs")
def data_diagnose_backtest_inputs_command(
    latest: Annotated[
        bool,
        typer.Option(help="使用价格缓存中的最新可用日期。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="诊断日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="shadow backtest 配置路径。"),
    ] = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
) -> None:
    """诊断 shadow backtest 输入数据并生成结构化质量报告。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    run = run_backtest_input_diagnostics(as_of=run_date, config_path=config_path)
    summary = run.payload.get("summary", {})
    status = summary.get("overall_status", "UNKNOWN") if isinstance(summary, dict) else "UNKNOWN"
    style = "green" if status == "OK" else "yellow" if status == "LIMITED" else "red"
    console.print(f"[{style}]Backtest input diagnostics：{status}[/{style}]")
    console.print(f"JSON：{run.json_path}")
    console.print(f"Markdown：{run.markdown_path}")
    console.print(f"Snapshot manifest：{run.manifest_path}")
    if isinstance(summary, dict):
        console.print(
            f"blocking_errors={summary.get('blocking_errors', 0)}；"
            f"warnings={summary.get('warnings', 0)}；"
            f"can_run_shadow_backtest={summary.get('can_run_shadow_backtest', False)}"
        )
    console.print("production_effect=none；不修改 production 参数或 promotion 规则")


@data_app.command("inspect-registry")
def data_inspect_registry_command(
    latest: Annotated[
        bool,
        typer.Option(help="解析 latest data registry / manifest 状态。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="registry 诊断日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="shadow backtest 配置路径。"),
    ] = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
) -> None:
    """检查 repair、manifest、validate-data 与 portfolio sensitivity 的数据视图一致性。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    run = run_data_registry_consistency(as_of=run_date, config_path=config_path)
    metadata = run.payload.get("metadata", {})
    latest_resolution = run.payload.get("latest_resolution", {})
    path_consistency = run.payload.get("path_consistency", {})
    status = metadata.get("status", "UNKNOWN") if isinstance(metadata, dict) else "UNKNOWN"
    style = "green" if status == "OK" else "yellow" if status == "LIMITED" else "red"
    console.print(f"[{style}]Data registry consistency：{status}[/{style}]")
    console.print(f"JSON：{run.json_path}")
    console.print(f"Markdown：{run.markdown_path}")
    if isinstance(latest_resolution, dict):
        console.print(
            "latest_resolution="
            f"{latest_resolution.get('status', 'UNKNOWN')}；"
            f"market_data={latest_resolution.get('resolved_market_data_date', '')}；"
            f"manifest={latest_resolution.get('resolved_backtest_manifest_date', '')}"
        )
    if isinstance(path_consistency, dict):
        console.print(
            "price_cache_paths="
            f"{path_consistency.get('status', 'UNKNOWN')}；"
            f"validate_data_read_path={path_consistency.get('validate_data_read_path', '')}"
        )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@data_app.command("validate-backtest-manifest")
def data_validate_backtest_manifest_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验 latest valid backtest input manifest 与价格缓存一致性。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="manifest 校验日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="shadow backtest 配置路径。"),
    ] = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
) -> None:
    """验证 backtest_input_manifest 与实际价格缓存、symbol mapping 是否一致。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    result = validate_backtest_manifest_consistency(as_of=run_date, config_path=config_path)
    status = str(result.get("status") or "UNKNOWN")
    style = "green" if status == "OK" else "yellow" if status == "LIMITED" else "red"
    console.print(f"[{style}]Backtest manifest consistency：{status}[/{style}]")
    manifest = result.get("backtest_manifest", {})
    if isinstance(manifest, dict):
        console.print(f"manifest：{manifest.get('path', '')}")
        console.print(f"manifest_validation={manifest.get('validation_status', 'UNKNOWN')}")
    for asset in result.get("asset_registry", []):
        if not isinstance(asset, dict):
            continue
        symbol = asset.get("canonical_symbol", "")
        source_symbol = asset.get("source_symbol", "")
        code = asset.get("error_code", "OK")
        if code == "OK":
            suffix = f" via {source_symbol}" if source_symbol and source_symbol != symbol else ""
            console.print(f"{symbol}: OK{suffix}")
        else:
            console.print(f"[red]{symbol}: {code}[/red] - {asset.get('diagnosis', '')}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")
    if status == "FAILED":
        raise typer.Exit(code=1)


@data_app.command(
    "reconcile-price-cache",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def data_reconcile_price_cache_command(
    ctx: typer.Context,
    latest: Annotated[
        bool,
        typer.Option(help="为 latest data registry mismatch 执行或规划 reconcile。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="reconcile 诊断日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="shadow backtest 配置路径。"),
    ] = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    dry_run: Annotated[
        bool,
        typer.Option(help="只输出修复计划，不改写价格缓存。"),
    ] = False,
    refresh_manifest_only: Annotated[
        bool,
        typer.Option(help="只刷新 backtest input manifest，不注册 repaired artifacts。"),
    ] = False,
    register_repaired_only: Annotated[
        bool,
        typer.Option(help="只注册 repaired artifacts，不刷新 backtest input manifest。"),
    ] = False,
    symbols: Annotated[
        list[str] | None,
        typer.Option(
            "--symbols",
            help="指定 reconcile 资产；可重复。也兼容 `--symbols GOOGL BRK.B SGOV`。",
        ),
    ] = None,
) -> None:
    """执行 price cache / manifest reconcile；dry-run 只输出计划。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    if refresh_manifest_only and register_repaired_only:
        raise typer.BadParameter("--refresh-manifest-only 不能和 --register-repaired-only 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    requested_symbols = tuple([*(symbols or []), *[str(item) for item in ctx.args]])
    run = run_price_cache_reconcile(
        as_of=run_date,
        config_path=config_path,
        dry_run=dry_run,
        refresh_manifest_only=refresh_manifest_only,
        register_repaired_only=register_repaired_only,
        symbols=requested_symbols,
    )
    result = run.payload
    metadata = result.get("metadata", {})
    status = str(result.get("status") or "UNKNOWN")
    if isinstance(metadata, dict):
        status = str(metadata.get("status") or status)
    style = (
        "green"
        if status in {"OK", "NOT_REQUIRED"}
        else (
            "yellow"
            if status
            in {
                "DRY_RUN",
                "LIMITED",
            }
            else "red"
        )
    )
    console.print(f"[{style}]Price cache reconcile：{status}[/{style}]")
    if run.json_path is not None:
        console.print(f"JSON：{run.json_path}")
    if run.markdown_path is not None:
        console.print(f"Markdown：{run.markdown_path}")
    console.print(f"Price cache registry：{run.registry_path}")
    for step in result.get("planned_actions", []):
        if not isinstance(step, dict):
            continue
        console.print(
            "- "
            f"action={step.get('action', '')}；"
            f"symbols={', '.join(str(item) for item in step.get('symbols', [])) or 'n/a'}"
        )
    for item in result.get("repaired_artifact_inspection", []):
        if not isinstance(item, dict):
            continue
        console.print(
            f"{item.get('canonical_symbol')}: {item.get('status')} "
            f"via {item.get('source_symbol')} rows={item.get('rows')} "
            f"error_code={item.get('error_code')}"
        )
    after = result.get("after", {})
    if isinstance(after, dict):
        console.print(
            "after="
            f"latest_resolution={after.get('latest_resolution', 'UNKNOWN')}；"
            f"market_data={after.get('market_data_date', '')}；"
            f"manifest={after.get('manifest_date', '')}"
        )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")
    if status == "FAILED":
        raise typer.Exit(code=1)


@data_app.command("refresh-backtest-manifest")
def data_refresh_backtest_manifest_command(
    latest: Annotated[
        bool,
        typer.Option(help="刷新 latest backtest input manifest。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="manifest 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="shadow backtest 配置路径。"),
    ] = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    dry_run: Annotated[
        bool,
        typer.Option(help="只显示将写入的 manifest，不实际生成。"),
    ] = False,
) -> None:
    """刷新 backtest input manifest；dry-run 不写 artifact。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    run = refresh_backtest_manifest(
        as_of=run_date,
        config_path=config_path,
        dry_run=dry_run,
    )
    metadata = run.payload.get("metadata", {})
    status = str(metadata.get("status") if isinstance(metadata, dict) else "UNKNOWN")
    style = "green" if status == "OK" else "yellow" if status == "DRY_RUN" else "red"
    console.print(f"[{style}]Backtest manifest refresh：{status}[/{style}]")
    console.print(f"target_manifest_date={run.payload.get('target_manifest_date', '')}")
    if run.diagnostic_run is not None:
        console.print(f"Diagnostic JSON：{run.diagnostic_run.json_path}")
        console.print(f"Snapshot manifest：{run.diagnostic_run.manifest_path}")
    else:
        console.print(f"would_write_manifest={run.payload.get('would_write_manifest', '')}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")
    if status == "FAILED":
        raise typer.Exit(code=1)


@data_app.command("freshness")
def data_freshness_command(
    latest: Annotated[
        bool,
        typer.Option(help="使用 raw price cache latest date 作为 tracking date。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="freshness tracking 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    market: Annotated[
        str,
        typer.Option("--market", help="市场代码；当前支持 US。"),
    ] = "US",
    config_path: Annotated[
        Path,
        typer.Option("--config", help="market data freshness 配置路径。"),
    ] = DEFAULT_MARKET_DATA_FRESHNESS_CONFIG_PATH,
    dry_run: Annotated[
        bool,
        typer.Option(help="写入 outputs/dry_runs/data_freshness，不改正式 artifacts。"),
    ] = False,
) -> None:
    """生成 market data freshness 和 tracking readiness 报告。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    try:
        run = run_market_data_freshness(
            as_of=run_date,
            market=market,
            config_path=config_path,
            dry_run=dry_run,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    metadata = run.payload.get("metadata", {}) if isinstance(run.payload, dict) else {}
    freshness = run.payload.get("freshness", {}) if isinstance(run.payload, dict) else {}
    data_dates = run.payload.get("data_dates", {}) if isinstance(run.payload, dict) else {}
    readiness = run.payload.get("tracking_readiness", {}) if isinstance(run.payload, dict) else {}
    status = freshness.get("status", metadata.get("status", "UNKNOWN"))
    style = "green" if status in {"OK", "NON_TRADING_DAY"} else "yellow"
    if status in {"MISSING", "FAILED", "MARKET_CALENDAR_UNKNOWN"}:
        style = "red"
    console.print(f"[{style}]Market data freshness：{status}[/{style}]")
    console.print(f"JSON：{run.json_path}")
    console.print(f"Markdown：{run.markdown_path}")
    if isinstance(data_dates, dict):
        console.print(
            "data_dates="
            f"tracking_date={data_dates.get('tracking_date', 'UNKNOWN')}；"
            f"effective_data_date={data_dates.get('effective_data_date', 'UNKNOWN')}；"
            f"latest_manifest_date={data_dates.get('latest_manifest_date', 'UNKNOWN')}"
        )
    if isinstance(freshness, dict):
        console.print(
            "freshness_status="
            f"{freshness.get('status', 'UNKNOWN')}；"
            f"lag_trading_days={freshness.get('lag_trading_days', 'UNKNOWN')}；"
            f"lag_calendar_days={freshness.get('lag_calendar_days', 'UNKNOWN')}"
        )
    if isinstance(readiness, dict):
        console.print(
            "tracking_readiness="
            f"{readiness.get('readiness', 'UNKNOWN')}；"
            f"tracking_status_recommendation="
            f"{readiness.get('tracking_status_recommendation', 'UNKNOWN')}"
        )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@data_app.command("validate-freshness")
def data_validate_freshness_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验最新正式 market data freshness artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="freshness 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 market_data_freshness_summary.json 路径。"),
    ] = None,
) -> None:
    """校验 market data freshness report schema、安全字段和 readiness 输出。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    source_path = input_path or _resolve_market_data_freshness_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_market_data_freshness_payload(source_path)
    issues = validate_market_data_freshness_payload(payload)
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    freshness = payload.get("freshness", {}) if isinstance(payload, dict) else {}
    readiness = payload.get("tracking_readiness", {}) if isinstance(payload, dict) else {}
    status = (
        freshness.get("status", metadata.get("status", "UNKNOWN"))
        if isinstance(freshness, dict)
        else "UNKNOWN"
    )
    style = "green" if not issues and status in {"OK", "NON_TRADING_DAY"} else "yellow"
    if issues or status in {"MISSING", "FAILED", "MARKET_CALENDAR_UNKNOWN"}:
        style = "red"
    console.print(f"[{style}]Market data freshness validation：{status}[/{style}]")
    console.print(f"source：{source_path}")
    if issues:
        for issue in issues:
            console.print(f"[red]- {issue}[/red]")
        raise typer.Exit(code=1)
    if isinstance(freshness, dict):
        console.print(f"freshness_status={freshness.get('status', 'UNKNOWN')}")
    if isinstance(readiness, dict):
        console.print(f"tracking_readiness={readiness.get('readiness', 'UNKNOWN')}")
        console.print(
            "tracking_status_recommendation="
            f"{readiness.get('tracking_status_recommendation', 'UNKNOWN')}"
        )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")
    if status in {"FAILED", "MISSING", "MARKET_CALENDAR_UNKNOWN"}:
        raise typer.Exit(code=1)


@data_app.command(
    "refresh-market",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def data_refresh_market_command(
    ctx: typer.Context,
    latest: Annotated[
        bool,
        typer.Option(help="使用最新 market data freshness report 生成或执行 refresh。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="refresh 目标日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="market data refresh 配置路径。"),
    ] = DEFAULT_MARKET_DATA_REFRESH_CONFIG_PATH,
    symbols: Annotated[
        list[str] | None,
        typer.Option(
            "--symbols",
            help="指定 refresh 资产；可重复。也兼容 `--symbols GOOGL BRK.B SGOV`。",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(help="只生成 refresh plan，不写价格缓存、registry 或 manifest。"),
    ] = False,
    plan_only: Annotated[
        bool,
        typer.Option(help="只生成 refresh plan，不执行 recovery。"),
    ] = False,
) -> None:
    """刷新 stale market data 并输出 freshness recovery summary。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    requested_symbols = tuple([*(symbols or []), *[str(item) for item in ctx.args]])
    run = run_market_data_refresh(
        as_of=run_date,
        symbols=requested_symbols,
        config_path=config_path,
        dry_run=dry_run,
        plan_only=plan_only,
    )
    metadata = run.payload.get("metadata", {}) if isinstance(run.payload, dict) else {}
    status = str(metadata.get("status") or "UNKNOWN")
    style = "green" if status in {"OK", "NOT_NEEDED"} else "yellow"
    if status in {"FAILED", "BLOCKED"}:
        style = "red"
    mode = "PLAN" if dry_run or plan_only else "EXECUTE"
    console.print(f"[{style}]Market data refresh：{status} ({mode})[/{style}]")
    console.print(f"Plan JSON：{run.plan_path}")
    if run.json_path is not None:
        console.print(f"JSON：{run.json_path}")
    if run.markdown_path is not None:
        console.print(f"Markdown：{run.markdown_path}")
    freshness_input = run.payload.get("freshness_input", {})
    if not isinstance(freshness_input, dict) or not freshness_input:
        before = run.payload.get("before", {})
        actions = run.payload.get("actions", {})
        if isinstance(before, dict) and isinstance(actions, dict):
            freshness_input = {
                "freshness_status": before.get("freshness_status", "UNKNOWN"),
                "required_target_date": actions.get("target_date", ""),
            }
    if isinstance(freshness_input, dict):
        console.print(
            "freshness_input="
            f"status={freshness_input.get('freshness_status', 'UNKNOWN')}；"
            f"target_date={freshness_input.get('required_target_date', '')}"
        )
    actions = run.payload.get("actions", {})
    if isinstance(actions, dict):
        fetched_assets = (
            ", ".join(str(item) for item in actions.get("fetched_assets", [])) or "none"
        )
        console.print(
            "actions="
            f"target_date={actions.get('target_date', '')}；"
            f"fetched_assets={fetched_assets}；"
            f"manifest_refreshed={actions.get('refreshed_backtest_manifest', False)}"
        )
    after = run.payload.get("after", {})
    if isinstance(after, dict):
        console.print(
            "after="
            f"freshness_status={after.get('freshness_status', 'UNKNOWN')}；"
            f"tracking_readiness={after.get('tracking_readiness', 'UNKNOWN')}；"
            f"candidate_tracking_status={after.get('candidate_tracking_status', 'UNKNOWN')}"
        )
    for item in run.payload.get("asset_results", []):
        if isinstance(item, dict):
            console.print(
                f"{item.get('symbol', '')}: {item.get('status', '')} "
                f"via {item.get('source_symbol', '')} source={item.get('source', '')}"
            )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")
    if status in {"FAILED", "BLOCKED"}:
        raise typer.Exit(code=1)


@data_app.command("validate-refresh")
def data_validate_refresh_command(
    latest: Annotated[
        bool,
        typer.Option(help="校验最新正式 market data refresh artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="refresh 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    input_path: Annotated[
        Path | None,
        typer.Option(help="显式 market_data_refresh_summary.json 路径。"),
    ] = None,
) -> None:
    """校验 market data refresh report schema、安全字段和 recovery 输出。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    source_path = input_path or _resolve_market_data_refresh_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_market_data_refresh_payload(source_path)
    issues = validate_market_data_refresh_payload(payload)
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    status = str(metadata.get("status") or "UNKNOWN")
    style = "green" if not issues and status in {"OK", "NOT_NEEDED"} else "yellow"
    if issues or status in {"FAILED", "BLOCKED"}:
        style = "red"
    console.print(f"[{style}]Market data refresh validation：{status}[/{style}]")
    console.print(f"source：{source_path}")
    if issues:
        for issue in issues:
            console.print(f"[red]- {issue}[/red]")
        raise typer.Exit(code=1)
    console.print(f"refresh_status={status}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")
    if status in {"FAILED", "BLOCKED"}:
        raise typer.Exit(code=1)


@fallback_policy_app.command("run")
def data_source_fallback_policy_run_command(
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="fallback policy 评估日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option(help="data_sources.yaml 路径。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    policy_path: Annotated[
        Path,
        typer.Option(help="data source fallback policy YAML 路径。"),
    ] = DEFAULT_DATA_SOURCE_FALLBACK_POLICY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="Data source fallback policy artifact 根目录。"),
    ] = DEFAULT_DATA_SOURCE_FALLBACK_DIR,
    unavailable_source_id: Annotated[
        list[str] | None,
        typer.Option(
            "--unavailable-source-id",
            help="显式标记不可用的 source_id；可重复。",
        ),
    ] = None,
    fallback_used_source_id: Annotated[
        list[str] | None,
        typer.Option(
            "--fallback-used-source-id",
            help="显式标记已使用且已披露 metadata 的 fallback source_id；可重复。",
        ),
    ] = None,
    fallback_reason: Annotated[
        list[str] | None,
        typer.Option(
            "--fallback-reason",
            help="fallback reason，格式为 source_id=reason 或 data_type=reason；可重复。",
        ),
    ] = None,
) -> None:
    """生成 paper-shadow research data source fallback policy report。"""
    evaluation_date = _parse_date(as_of) if as_of else date.today()
    payload, paths = build_and_write_data_source_fallback_policy(
        config=load_data_sources(config_path),
        policy=load_data_source_fallback_policy(policy_path),
        as_of=evaluation_date,
        output_dir=output_dir,
        unavailable_source_ids=unavailable_source_id or [],
        fallback_used_source_ids=fallback_used_source_id or [],
        fallback_reasons=_parse_key_value_options(fallback_reason or []),
    )
    _print_fallback_policy_summary(payload, paths.get("report_json"))
    if payload.get("status") == "FAIL":
        raise typer.Exit(code=1)


@fallback_policy_app.command("report")
def data_source_fallback_policy_report_command(
    report_id: Annotated[
        str | None,
        typer.Option(help="要读取的 fallback policy report_id；不传时可用 --latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取 latest fallback policy artifact。"),
    ] = False,
    output_dir: Annotated[
        Path,
        typer.Option(help="Data source fallback policy artifact 根目录。"),
    ] = DEFAULT_DATA_SOURCE_FALLBACK_DIR,
) -> None:
    """读取 paper-shadow research data source fallback policy report。"""
    report_path = resolve_data_source_fallback_policy_path(
        report_id=report_id,
        latest=latest or report_id is None,
        output_dir=output_dir,
    )
    payload = load_data_source_fallback_policy_payload(report_path)
    _print_fallback_policy_summary(payload, report_path)
    if payload.get("status") == "FAIL":
        raise typer.Exit(code=1)


@fallback_policy_app.command("validate")
def data_source_fallback_policy_validate_command(
    report_id: Annotated[
        str | None,
        typer.Option(help="要校验的 fallback policy report_id；不传时可用 --latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="校验 latest fallback policy artifact。"),
    ] = False,
    output_dir: Annotated[
        Path,
        typer.Option(help="Data source fallback policy artifact 根目录。"),
    ] = DEFAULT_DATA_SOURCE_FALLBACK_DIR,
) -> None:
    """校验 fallback policy states、eligibility、metadata 和安全边界。"""
    validation, report_path = validate_data_source_fallback_policy_artifact(
        report_id=report_id,
        latest=latest or report_id is None,
        output_dir=output_dir,
    )
    status_style = (
        "green" if validation.status == "PASS" else "yellow" if validation.passed else "red"
    )
    console.print(
        f"[{status_style}]Data source fallback policy validation status="
        f"{validation.status}[/{status_style}]"
    )
    console.print(f"report_id={validation.report_id}")
    console.print(f"report={report_path}")
    console.print(f"source_group_count={validation.source_group_count}")
    console.print(f"error_count={validation.error_count}; warning_count={validation.warning_count}")
    console.print("production_effect=none；校验只读 existing artifact。")
    if not validation.passed:
        raise typer.Exit(code=1)


@cache_catalog_app.command("run")
def cache_catalog_run_command(
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="cache catalog 评估日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option(help="data_sources.yaml 路径。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    policy_path: Annotated[
        Path,
        typer.Option(help="cache catalog YAML 路径。"),
    ] = DEFAULT_CACHE_CATALOG_POLICY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="Cache catalog artifact 根目录。"),
    ] = DEFAULT_CACHE_CATALOG_DIR,
    expected_checksum: Annotated[
        list[str] | None,
        typer.Option(
            "--expected-checksum",
            help="显式 checksum 断言，格式为 entry_id=sha256；可重复。",
        ),
    ] = None,
    previous_catalog_path: Annotated[
        Path | None,
        typer.Option(help="显式 previous cache catalog JSON；缺省读取 latest。"),
    ] = None,
    refresh_audit_report_path: Annotated[
        Path | None,
        typer.Option(help="显式 data refresh audit JSON；缺省读取 latest。"),
    ] = None,
    refresh_audit_output_dir: Annotated[
        Path,
        typer.Option(help="Data refresh audit artifact 根目录。"),
    ] = CACHE_CATALOG_REFRESH_AUDIT_DIR,
    validation_audit_dir: Annotated[
        Path,
        typer.Option(help="validate-data audit sidecar 根目录。"),
    ] = CACHE_CATALOG_VALIDATE_DATA_AUDIT_DIR,
) -> None:
    """生成 read-only checksum/cache catalog。"""
    evaluation_date = _parse_date(as_of) if as_of else date.today()
    payload, paths = build_and_write_cache_catalog(
        config=load_data_sources(config_path),
        policy=load_cache_catalog_policy(policy_path),
        as_of=evaluation_date,
        output_dir=output_dir,
        expected_checksums=_parse_key_value_options(
            expected_checksum or [],
            option_name="--expected-checksum",
        ),
        previous_catalog_path=previous_catalog_path,
        refresh_audit_report_path=refresh_audit_report_path,
        refresh_audit_output_dir=refresh_audit_output_dir,
        validation_audit_dir=validation_audit_dir,
    )
    _print_cache_catalog_summary(payload, paths.get("catalog_json"))
    if payload.get("status") == "FAIL":
        raise typer.Exit(code=1)


@cache_catalog_app.command("report")
def cache_catalog_report_command(
    catalog_id: Annotated[
        str | None,
        typer.Option(help="要读取的 cache catalog id；不传时可用 --latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取 latest cache catalog artifact。"),
    ] = False,
    output_dir: Annotated[
        Path,
        typer.Option(help="Cache catalog artifact 根目录。"),
    ] = DEFAULT_CACHE_CATALOG_DIR,
) -> None:
    """读取 checksum/cache catalog report。"""
    catalog_path = resolve_cache_catalog_path(
        catalog_id=catalog_id,
        latest=latest or catalog_id is None,
        output_dir=output_dir,
    )
    payload = load_cache_catalog_payload(catalog_path)
    _print_cache_catalog_summary(payload, catalog_path)
    if payload.get("status") == "FAIL":
        raise typer.Exit(code=1)


@cache_catalog_app.command("validate")
def cache_catalog_validate_command(
    catalog_id: Annotated[
        str | None,
        typer.Option(help="要校验的 cache catalog id；不传时可用 --latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="校验 latest cache catalog artifact。"),
    ] = False,
    output_dir: Annotated[
        Path,
        typer.Option(help="Cache catalog artifact 根目录。"),
    ] = DEFAULT_CACHE_CATALOG_DIR,
) -> None:
    """校验 cache catalog schema、checksum、missing entry 和安全边界。"""
    validation, catalog_path = validate_cache_catalog_artifact(
        catalog_id=catalog_id,
        latest=latest or catalog_id is None,
        output_dir=output_dir,
    )
    status_style = (
        "green" if validation.status == "PASS" else "yellow" if validation.passed else "red"
    )
    console.print(
        f"[{status_style}]Cache catalog validation status={validation.status}" f"[/{status_style}]"
    )
    console.print(f"catalog_id={validation.catalog_id}")
    console.print(f"catalog={catalog_path}")
    console.print(f"entry_count={validation.entry_count}")
    console.print(f"error_count={validation.error_count}; warning_count={validation.warning_count}")
    console.print("production_effect=none；校验只读 existing artifact。")
    if not validation.passed:
        raise typer.Exit(code=1)


@refresh_audit_app.command("report")
def data_refresh_audit_report_command(
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="audit 评估日期，格式为 YYYY-MM-DD。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="Data refresh audit artifact 根目录。"),
    ] = DEFAULT_DATA_REFRESH_AUDIT_DIR,
    validation_audit_dir: Annotated[
        Path,
        typer.Option(help="validate-data audit sidecar 根目录。"),
    ] = DEFAULT_VALIDATION_AUDIT_DIR,
    market_refresh_root: Annotated[
        Path | None,
        typer.Option(
            help="market_data_refresh_summary.json 根目录；默认使用项目 artifact 根目录。"
        ),
    ] = None,
    price_cache_path: Annotated[
        Path,
        typer.Option(help="只读 price cache 路径，用于 skipped refresh checksum/row count。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    fallback_policy_report_path: Annotated[
        Path | None,
        typer.Option(help="显式 fallback policy report JSON 路径；缺省读取 latest。"),
    ] = None,
    fallback_policy_output_dir: Annotated[
        Path,
        typer.Option(help="Data source fallback policy artifact 根目录。"),
    ] = DEFAULT_DATA_SOURCE_FALLBACK_DIR,
    cache_catalog_report_path: Annotated[
        Path | None,
        typer.Option(help="显式 cache catalog JSON 路径；缺省读取 latest。"),
    ] = None,
    cache_catalog_output_dir: Annotated[
        Path,
        typer.Option(help="Cache catalog artifact 根目录。"),
    ] = DEFAULT_CACHE_CATALOG_DIR,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="只读取 latest artifact，不生成新 audit。"),
    ] = False,
) -> None:
    """生成或读取 paper-shadow data refresh audit trail。"""
    if latest:
        audit_path = resolve_data_refresh_audit_path(latest=True, output_dir=output_dir)
        payload = load_data_refresh_audit_payload(audit_path)
        paths = {"audit_json": audit_path}
    else:
        evaluation_date = _parse_date(as_of) if as_of else None
        if evaluation_date is None:
            evaluation_date = date.today()
        payload, paths = build_and_write_data_refresh_audit(
            as_of=evaluation_date,
            output_dir=output_dir,
            validation_audit_dir=validation_audit_dir,
            market_refresh_root=market_refresh_root,
            price_cache_path=price_cache_path,
            fallback_policy_report_path=fallback_policy_report_path,
            fallback_policy_output_dir=fallback_policy_output_dir,
            cache_catalog_report_path=cache_catalog_report_path,
            cache_catalog_output_dir=cache_catalog_output_dir,
        )

    summary = payload.get("summary", {})
    status = str(payload.get("status", "UNKNOWN"))
    status_style = "green" if status == "PASS" else "yellow" if status != "FAIL" else "red"
    console.print(f"[{status_style}]Data refresh audit status={status}[/{status_style}]")
    console.print(f"audit_id={payload.get('audit_id')}")
    if isinstance(summary, dict):
        console.print(f"audit_record_count={summary.get('audit_record_count')}")
        console.print(
            "record_counts="
            f"failed:{summary.get('failed_record_count')}, "
            f"skipped:{summary.get('skipped_record_count')}, "
            f"warnings:{summary.get('warning_count')}, "
            f"errors:{summary.get('error_count')}"
        )
        console.print(f"next_action={summary.get('next_action')}")
    cache_catalog = payload.get("cache_catalog_summary", {})
    if isinstance(cache_catalog, dict):
        console.print(
            "cache_catalog="
            f"integrity={cache_catalog.get('cache_integrity_status', 'MISSING')}; "
            f"missing_required={cache_catalog.get('missing_required_count', 0)}; "
            f"checksum_mismatch={cache_catalog.get('checksum_mismatch_count', 0)}"
        )
    console.print(f"report={paths.get('audit_json')}")
    console.print("production_effect=none；只读治理报告，不刷新数据、不补造 cache、不触发 broker。")

    if status == "FAIL":
        raise typer.Exit(code=1)


@refresh_audit_app.command("validate")
def validate_data_refresh_audit_command(
    audit_id: Annotated[
        str | None,
        typer.Option(help="要校验的 audit_id；不传时可用 --latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="校验 latest data refresh audit。"),
    ] = False,
    output_dir: Annotated[
        Path,
        typer.Option(help="Data refresh audit artifact 根目录。"),
    ] = DEFAULT_DATA_REFRESH_AUDIT_DIR,
) -> None:
    """校验 data refresh audit schema、status、checksum、record counts 和安全边界。"""
    validation, audit_path = validate_data_refresh_audit_artifact(
        audit_id=audit_id,
        latest=latest or audit_id is None,
        output_dir=output_dir,
    )
    status_style = (
        "green" if validation.status == "PASS" else "yellow" if validation.passed else "red"
    )
    console.print(
        f"[{status_style}]Data refresh audit validation status={validation.status}"
        f"[/{status_style}]"
    )
    console.print(f"audit_id={validation.audit_id}")
    console.print(f"audit={audit_path}")
    console.print(f"audit_record_count={validation.audit_record_count}")
    console.print(f"error_count={validation.error_count}; warning_count={validation.warning_count}")
    console.print("production_effect=none；校验只读 existing artifact。")

    if not validation.passed:
        raise typer.Exit(code=1)


def _parse_key_value_options(
    values: list[str],
    *,
    option_name: str = "--fallback-reason",
) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise typer.BadParameter(f"{option_name} must use key=value format")
        key, reason = value.split("=", 1)
        key = key.strip()
        if not key:
            raise typer.BadParameter(f"{option_name} key cannot be empty")
        parsed[key] = reason.strip()
    return parsed


def _print_fallback_policy_summary(
    payload: dict[str, object],
    report_path: Path | None,
) -> None:
    summary = latest_data_source_fallback_policy_summary(report_path=report_path)
    status = str(payload.get("status", "UNKNOWN"))
    style = "green" if status == "PASS" else "yellow" if status != "FAIL" else "red"
    console.print(f"[{style}]Data source fallback policy status={status}[/{style}]")
    console.print(f"report_id={payload.get('report_id')}")
    console.print(f"fallback_status={summary.get('fallback_status')}")
    console.print(f"source_group_count={summary.get('source_group_count')}")
    console.print(f"fallback_used_count={summary.get('fallback_used_count')}")
    console.print(f"blocking_source_count={summary.get('blocking_source_count')}")
    console.print(f"fallback_used_sources={summary.get('fallback_used_sources')}")
    console.print(f"blocking_data_types={summary.get('blocking_data_types')}")
    console.print(f"next_action={summary.get('next_action')}")
    console.print(f"report={report_path}")
    console.print(
        "production_effect=none；只读 fallback policy，不刷新数据、不补造 cache、不触发 broker。"
    )


def _print_cache_catalog_summary(
    payload: dict[str, object],
    report_path: Path | None,
) -> None:
    summary = payload.get("summary", {})
    if not isinstance(summary, dict):
        summary = {}
    status = str(payload.get("status", "UNKNOWN"))
    style = "green" if status == "PASS" else "yellow" if status != "FAIL" else "red"
    console.print(f"[{style}]Cache catalog status={status}[/{style}]")
    console.print(f"catalog_id={payload.get('catalog_id')}")
    console.print(f"cache_integrity_status={payload.get('cache_integrity_status')}")
    console.print(f"entry_count={summary.get('entry_count')}")
    console.print(f"missing_required_count={summary.get('missing_required_count')}")
    console.print(f"checksum_mismatch_count={summary.get('checksum_mismatch_count')}")
    console.print(
        "checksum_changed_without_refresh_count="
        f"{summary.get('checksum_changed_without_refresh_count')}"
    )
    console.print(f"blocking_entry_ids={summary.get('blocking_entry_ids')}")
    console.print(f"next_action={summary.get('next_action')}")
    console.print(f"report={report_path}")
    console.print(
        "production_effect=none；只读 cache catalog，不刷新数据、不修复 cache、不触发 broker。"
    )


@data_app.command("recover-freshness")
def data_recover_freshness_command(
    latest: Annotated[
        bool,
        typer.Option(help="使用 latest freshness/tracking 日期执行 recovery。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="recovery 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    refresh_config_path: Annotated[
        Path,
        typer.Option("--refresh-config", help="market data refresh 配置路径。"),
    ] = DEFAULT_MARKET_DATA_REFRESH_CONFIG_PATH,
    freshness_config_path: Annotated[
        Path,
        typer.Option("--freshness-config", help="market data freshness 配置路径。"),
    ] = DEFAULT_MARKET_DATA_FRESHNESS_CONFIG_PATH,
) -> None:
    """执行 freshness -> refresh -> manifest/freshness/tracking recovery 链路。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    freshness_run = run_market_data_freshness(
        as_of=run_date,
        config_path=freshness_config_path,
        dry_run=False,
    )
    freshness = freshness_run.payload.get("freshness", {})
    freshness_status = (
        freshness.get("status", "UNKNOWN") if isinstance(freshness, dict) else "UNKNOWN"
    )
    console.print(f"freshness_before={freshness_status}；report={freshness_run.json_path}")
    refresh_run = run_market_data_refresh(
        as_of=freshness_run.as_of,
        config_path=refresh_config_path,
        dry_run=False,
    )
    metadata = (
        refresh_run.payload.get("metadata", {}) if isinstance(refresh_run.payload, dict) else {}
    )
    status = str(metadata.get("status") or "UNKNOWN")
    style = "green" if status in {"OK", "NOT_NEEDED"} else "yellow"
    if status in {"FAILED", "BLOCKED"}:
        style = "red"
    console.print(f"[{style}]Freshness recovery：{status}[/{style}]")
    console.print(f"Refresh JSON：{refresh_run.json_path}")
    after = refresh_run.payload.get("after", {})
    if isinstance(after, dict):
        console.print(
            f"freshness_status={after.get('freshness_status', 'UNKNOWN')}；"
            f"tracking_readiness={after.get('tracking_readiness', 'UNKNOWN')}；"
            f"tracking_status={after.get('candidate_tracking_status', 'UNKNOWN')}"
        )
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")
    if status in {"FAILED", "BLOCKED"}:
        raise typer.Exit(code=1)


@data_app.command(
    "repair-backtest-inputs",
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def data_repair_backtest_inputs_command(
    ctx: typer.Context,
    latest: Annotated[
        bool,
        typer.Option(help="使用价格缓存中的最新可用日期。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="repair planning 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="shadow backtest 配置路径。"),
    ] = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    dry_run: Annotated[
        bool,
        typer.Option(help="只输出 repair plan，不下载或改写外部数据。"),
    ] = False,
    price_only: Annotated[
        bool,
        typer.Option("--price-only", help="只修复价格历史，不尝试生成 signal snapshots。"),
    ] = False,
    symbols: Annotated[
        list[str] | None,
        typer.Option(
            "--symbols",
            help="指定修复资产；可重复。也兼容 `--symbols GOOGL BRK.B SGOV`。",
        ),
    ] = None,
    price_provider: Annotated[
        str,
        typer.Option(help="价格 repair provider：fmp 或 yahoo。默认使用 active 主源 fmp。"),
    ] = "fmp",
    fmp_api_key_env: Annotated[
        str,
        typer.Option(help="读取 FMP API key 的环境变量名。"),
    ] = "FMP_API_KEY",
) -> None:
    """修复 shadow backtest 输入价格历史；dry-run 只输出 repair plan。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    run_date = None if latest or as_of is None else _parse_date(as_of)
    extra_symbol_args = [str(item) for item in ctx.args]
    requested_symbols = tuple([*(symbols or []), *extra_symbol_args])
    run = run_backtest_input_diagnostics(as_of=run_date, config_path=config_path)
    repair_plan = run.payload.get("repair_plan", {})
    status = repair_plan.get("status", "UNKNOWN") if isinstance(repair_plan, dict) else "UNKNOWN"
    mode = "DRY_RUN" if dry_run else "EXECUTE"
    style = "green" if status == "NOT_REQUIRED" else "yellow"
    console.print(f"[{style}]Backtest input repair plan：{status} ({mode})[/{style}]")
    console.print(f"Diagnostic JSON：{run.json_path}")
    console.print(f"Diagnostic Markdown：{run.markdown_path}")
    if isinstance(repair_plan, dict):
        steps = repair_plan.get("steps", [])
        if isinstance(steps, list) and steps:
            for step in steps:
                if isinstance(step, dict):
                    console.print(
                        f"- step {step.get('step')}: {step.get('action')} "
                        f"required={step.get('required', False)}"
                    )
        else:
            console.print("- repair plan 为空。")
    if dry_run:
        console.print("production_effect=none；不修改 production 参数或 promotion 规则")
        return

    try:
        provider = build_price_history_repair_provider(
            provider_name=price_provider,
            fmp_api_key=os.getenv(fmp_api_key_env, ""),
        )
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1) from exc

    repair = repair_backtest_price_history(
        as_of=run_date,
        config_path=config_path,
        symbols=requested_symbols,
        price_provider=provider,
        provider_name=price_provider,
        price_only=price_only,
    )
    result_style = "green" if repair.status in {"REPAIRED", "NOT_REQUIRED"} else "yellow"
    console.print(f"[{result_style}]Price history repair：{repair.status}[/{result_style}]")
    for result in repair.asset_results:
        console.print(
            f"- {result.symbol}: {result.status}; source_symbol={result.source_symbol}; "
            f"rows={result.rows_written}; error={result.error or 'none'}"
        )
    final_summary = repair.final_diagnostics.payload.get("summary", {})
    if isinstance(final_summary, dict):
        console.print(
            f"final_status={final_summary.get('overall_status', 'UNKNOWN')}；"
            f"backtest_mode={final_summary.get('backtest_mode', 'UNKNOWN')}；"
            f"can_run_shadow_backtest={final_summary.get('can_run_shadow_backtest', False)}；"
            f"can_promote_candidate={final_summary.get('can_promote_candidate', False)}"
        )
    console.print(f"Price cache：{repair.price_cache_path}")
    console.print(f"Download manifest：{repair.manifest_path}")
    console.print(f"Final diagnostic JSON：{repair.final_diagnostics.json_path}")
    console.print(f"Final snapshot manifest：{repair.final_diagnostics.manifest_path}")
    console.print("production_effect=none；不修改 production 参数或 promotion 规则")


def _print_foundation_payload(payload: dict[str, object]) -> None:
    status = str(payload.get("status", "UNKNOWN"))
    style = "green" if status == "PASS" else "yellow" if "WARNING" in status else "red"
    console.print(
        f"[{style}]{payload.get('title', payload.get('report_type'))}：{status}[/{style}]"
    )
    summary = payload.get("summary")
    if isinstance(summary, dict):
        for key in sorted(summary):
            console.print(f"{key}={summary[key]}")
    artifact_paths = payload.get("artifact_paths")
    if isinstance(artifact_paths, dict):
        for label, path in artifact_paths.items():
            console.print(f"{label}={path}")
    console.print("production_effect=none；broker_action=none；validation_only=true")


def _print_free_source_payload(payload: dict[str, object]) -> None:
    status = str(payload.get("status", "UNKNOWN"))
    style = "green" if status == "PASS" or "READY" in status else "yellow"
    if status == "FAIL":
        style = "red"
    console.print(f"[{style}]Free PIT data sources：{status}[/{style}]")
    summary = payload.get("summary")
    if isinstance(summary, dict):
        for key in sorted(summary):
            console.print(f"{key}={summary[key]}")
    artifact_paths = payload.get("artifact_paths")
    if isinstance(artifact_paths, dict):
        for label, path in artifact_paths.items():
            console.print(f"{label}={path}")
    console.print("promotion_allowed=false；paper_shadow_allowed=false；production_allowed=false；broker_action=none")
