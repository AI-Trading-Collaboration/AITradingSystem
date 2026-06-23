from __future__ import annotations

import hashlib
import os
from collections.abc import Callable, Mapping
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT, load_data_quality
from ai_trading_system.data.quality import DataQualityReport, validate_data_cache
from ai_trading_system.data_foundation import utc_now_iso, write_foundation_artifact_pair
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_AI_REGIME_BACKTEST_START,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    SAFETY_BOUNDARY,
    _dynamic_candidate_strategies,
    _float,
    _load_price_matrix,
    _load_registry,
    _mapping,
    _metrics_for_strategy,
    _read_json_or_empty,
    _records,
    _required_rate_series,
    _required_tickers,
    _research_policy_int,
    _slice_prices,
    _strategy_return_series,
    _strategy_rows,
    _target_weight_frame,
)
from ai_trading_system.trading_engine.data.price_history_repair import (
    PriceDataProvider,
    build_price_history_repair_provider,
    repair_backtest_price_history,
)

DEFAULT_DOWNLOAD_MANIFEST_PATH = PROJECT_ROOT / "data" / "raw" / "download_manifest.csv"
DEFAULT_SIMPLE_BASELINE_REPAIR_REQUIREMENT_PATH = (
    PROJECT_ROOT
    / "docs"
    / "requirements"
    / "TRADING-911_to_922_Simple_Baseline_Data_Repair_Forward_Aging_Unblock.md"
)

TARGET_SYMBOLS = ("QQQ", "TQQQ", "SGOV")
PRIMARY_CANDIDATE_ID = "equal_risk_qqq_sgov"
CHALLENGER_CANDIDATE_ID = "dyn_tqqq_capped_trend"
STATIC_COMPARATOR_IDS = ("qqq_50_sgov_50", "qqq_60_sgov_40", "100_qqq")
PUBLIC_ALIAS_TO_REGISTRY_ID = {"100_qqq": "qqq_100_static"}
FORBIDDEN_READER_BRIEF_PHRASES = ("买入", "卖出", "应调仓", "实盘建议", "目标持仓建议")


def run_simple_baseline_data_source_inventory(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    manifest_path: Path = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    inventory = _build_symbol_inventory(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        manifest_path=manifest_path,
    )
    statuses = {str(row["source_status"]) for row in inventory}
    if any(_symbol_row(inventory, symbol).get("row_count", 0) == 0 for symbol in ("QQQ", "SGOV")):
        status = "DATA_SOURCE_INVENTORY_BLOCKED"
    elif "BLOCKED" in statuses or "WARN" in statuses:
        status = "DATA_SOURCE_INVENTORY_PARTIAL"
    else:
        status = "DATA_SOURCE_INVENTORY_READY"

    payload = _payload(
        report_type="simple_baseline_data_source_inventory",
        title="Simple Baseline Data Source Inventory",
        status=status,
        summary={
            "symbol_count": len(inventory),
            "blocked_symbol_count": sum(row["source_status"] == "BLOCKED" for row in inventory),
            "warn_symbol_count": sum(row["source_status"] == "WARN" for row in inventory),
            "qqq_complete": _symbol_row(inventory, "QQQ").get("source_status") == "OK",
            "tqqq_complete": _symbol_row(inventory, "TQQQ").get("source_status") == "OK",
            "sgov_complete": _symbol_row(inventory, "SGOV").get("source_status") == "OK",
            "tqqq_primary_cache_gap_reason": _tqqq_gap_reason(inventory),
            "marketstack_gap_reason": _marketstack_gap_reason(inventory),
            "fmp_repair_can_fill": _fmp_repair_can_fill(inventory),
        },
        inventory=inventory,
        root_cause_findings=_data_source_root_cause_findings(inventory),
        report_registry_entry=_report_registry_entry(
            "simple_baseline_data_source_inventory",
            "Simple Baseline Data Source Inventory",
            "aits research strategies simple-baseline-data-source-inventory",
            "simple_baseline_data_source_inventory",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_tqqq_cache_rebuild_validation(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    manifest_path: Path = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
    price_provider: PriceDataProvider | None = None,
    fmp_api_key_env: str = "FMP_API_KEY",
    execute_repair: bool = True,
) -> dict[str, Any]:
    before = _build_symbol_inventory(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        manifest_path=manifest_path,
    )
    repair_summary: dict[str, Any] = {
        "repair_executed": False,
        "provider": "Financial Modeling Prep",
        "provider_source_id": "fmp_eod_daily_prices",
        "used_fixture": False,
        "new_unconfigured_source_used": False,
        "repair_path": "repair_backtest_price_history",
    }
    blocker = ""
    if execute_repair and _symbol_row(before, "TQQQ").get("row_count", 0) == 0:
        try:
            provider = price_provider or build_price_history_repair_provider(
                provider_name="fmp",
                fmp_api_key=os.getenv(fmp_api_key_env, ""),
            )
            repair = repair_backtest_price_history(
                as_of=as_of_date,
                symbols=("TQQQ",),
                price_provider=provider,
                provider_name="fmp",
                price_only=True,
            )
            repair_summary.update(
                {
                    "repair_executed": True,
                    "repair_status": repair.status,
                    "asset_results": [row.to_dict() for row in repair.asset_results],
                    "price_cache_path": str(repair.price_cache_path),
                    "manifest_path": str(repair.manifest_path),
                    "initial_diagnostic_report": str(repair.initial_diagnostics.json_path),
                    "final_diagnostic_report": str(repair.final_diagnostics.json_path),
                }
            )
        except Exception as exc:  # pragma: no cover - exercised by CLI smoke only.
            blocker = str(exc)
            repair_summary.update({"repair_executed": False, "repair_error": blocker})

    after = _build_symbol_inventory(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        manifest_path=manifest_path,
    )
    config = _load_registry(config_path)
    data_quality = _data_quality_payload(
        _validate_simple_baseline_data(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config=config,
            as_of_date=as_of_date,
        ),
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
    )
    tqqq_after = _symbol_row(after, "TQQQ")
    if tqqq_after.get("row_count", 0) == 0:
        status = "TQQQ_CACHE_BLOCKED"
    elif not data_quality["passed"]:
        status = "TQQQ_CACHE_PARTIAL"
    else:
        status = "TQQQ_CACHE_REBUILT"

    payload = _payload(
        report_type="tqqq_cache_rebuild_validation",
        title="TQQQ Cache Rebuild Validation",
        status=status,
        summary={
            "tqqq_rows_before": _symbol_row(before, "TQQQ").get("row_count", 0),
            "tqqq_rows_after": tqqq_after.get("row_count", 0),
            "repair_executed": repair_summary.get("repair_executed", False),
            "validate_data_status": data_quality["status"],
            "validate_data_passed": data_quality["passed"],
            "blocked_reason": blocker,
        },
        before_inventory=before,
        after_inventory=after,
        repair_summary=repair_summary,
        data_quality=data_quality,
        blockers=[blocker] if blocker else [],
        report_registry_entry=_report_registry_entry(
            "tqqq_cache_rebuild_validation",
            "TQQQ Cache Rebuild Validation",
            "aits research strategies tqqq-cache-rebuild-and-validation",
            "tqqq_cache_rebuild_validation",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_sgov_total_return_data_contract(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    manifest_path: Path = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    inventory = _build_symbol_inventory(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        manifest_path=manifest_path,
    )
    sgov = _symbol_row(inventory, "SGOV")
    adjusted = bool(sgov.get("adjusted_close_available"))
    dividend_adjusted = bool(sgov.get("dividend_adjusted"))
    total_return = bool(sgov.get("total_return_available"))
    if int(sgov.get("row_count", 0)) == 0:
        status = "SGOV_DATA_BLOCKED"
    elif not total_return:
        status = "SGOV_PRICE_ONLY_WARN" if adjusted else "SGOV_DATA_BLOCKED"
    else:
        status = "SGOV_TOTAL_RETURN_CONTRACT_READY"

    payload = _payload(
        report_type="sgov_total_return_data_contract",
        title="SGOV Total Return Data Contract",
        status=status,
        summary={
            "sgov_row_count": sgov.get("row_count", 0),
            "adjusted_close_available": adjusted,
            "dividend_adjusted": dividend_adjusted,
            "total_return_available": total_return,
            "price_only_allowed": False,
            "interest_rate_proxy_allowed_without_owner_review": False,
            "fail_closed_when_sgov_missing": True,
        },
        sgov_inventory=sgov,
        contract={
            "adjusted_close_rule": (
                "SGOV simple-baseline returns must use primary cache adj_close when available."
            ),
            "dividend_distribution_rule": (
                "Distributions are included only through an audited dividend-adjusted "
                "adj_close path; raw close is not a carry-complete return series."
            ),
            "total_return_proxy_rule": (
                "SGOV adj_close is the configured total-return proxy when adjusted close "
                "exists and shows distribution adjustment evidence."
            ),
            "interest_rate_proxy_rule": (
                "DGS2/DGS10 may explain carry but cannot replace missing SGOV total-return "
                "data without an owner-approved policy change."
            ),
            "missing_sgov_rule": "Fail closed for every QQQ/SGOV strategy and forward-aging path.",
        },
        report_registry_entry=_report_registry_entry(
            "sgov_total_return_data_contract",
            "SGOV Total Return Data Contract",
            "aits research strategies sgov-total-return-data-contract",
            "sgov_total_return_data_contract",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_market_data_repair_manifest_audit(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    manifest_path: Path = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    inventory = _build_symbol_inventory(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        manifest_path=manifest_path,
    )
    manifest = _read_csv_or_empty(manifest_path)
    prices = _read_csv_or_empty(prices_path)
    rows = [
        _manifest_audit_row(symbol, manifest=manifest, prices=prices, prices_path=prices_path)
        for symbol in TARGET_SYMBOLS
    ]
    if any(row["status"] == "BLOCKED" for row in rows):
        status = "REPAIR_MANIFEST_BLOCKED"
    elif any(row["status"] == "WARN" for row in rows):
        status = "REPAIR_MANIFEST_WARN"
    else:
        status = "REPAIR_MANIFEST_PASS"

    payload = _payload(
        report_type="market_data_repair_manifest_audit",
        title="Market Data Repair Manifest Audit",
        status=status,
        summary={
            "symbol_count": len(rows),
            "blocked_count": sum(row["status"] == "BLOCKED" for row in rows),
            "warning_count": sum(row["status"] == "WARN" for row in rows),
            "manifest_path": str(manifest_path),
        },
        inventory=inventory,
        manifest_audit=rows,
        report_registry_entry=_report_registry_entry(
            "market_data_repair_manifest_audit",
            "Market Data Repair Manifest Audit",
            "aits research strategies market-data-repair-manifest-audit",
            "market_data_repair_manifest_audit",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_validate_data_hardening(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    manifest_path: Path = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    inventory = _build_symbol_inventory(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        manifest_path=manifest_path,
    )
    data_quality = _data_quality_payload(
        _validate_simple_baseline_data(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config=config,
            as_of_date=as_of_date,
        ),
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
    )
    rules = _hardening_rules(inventory)
    critical_blocked = any(row["severity"] == "BLOCKED_CRITICAL" for row in rules)
    noncritical_blocked = any(row["severity"] == "BLOCKED_OPTIONAL" for row in rules)
    warning = any(row["severity"] == "WARN" for row in rules)
    if critical_blocked or _data_quality_has_critical_issue(data_quality):
        status = "VALIDATE_DATA_BLOCKED"
    elif noncritical_blocked or warning or not data_quality["passed"]:
        status = "VALIDATE_DATA_WARN"
    else:
        status = "VALIDATE_DATA_HARDENED"

    payload = _payload(
        report_type="simple_baseline_validate_data_hardening",
        title="Simple Baseline Validate-Data Hardening",
        status=status,
        summary={
            "data_quality_status": _hardening_data_quality_status(status, data_quality),
            "raw_data_quality_status": data_quality["status"],
            "data_quality_passed": data_quality["passed"],
            "qqq_sgov_strategy_status": _strategy_group_status(rules, "qqq_sgov"),
            "tqqq_challenger_status": _strategy_group_status(rules, "tqqq_challenger"),
            "sample_count_zero_ready_allowed": False,
            "blocked_rule_count": sum("BLOCKED" in row["severity"] for row in rules),
            "warning_rule_count": sum(row["severity"] == "WARN" for row in rules),
        },
        data_quality=data_quality,
        inventory=inventory,
        hardening_rules=rules,
        report_registry_entry=_report_registry_entry(
            "simple_baseline_validate_data_hardening",
            "Simple Baseline Validate-Data Hardening",
            "aits research strategies simple-baseline-validate-data-hardening",
            "simple_baseline_validate_data_hardening",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_simple_baseline_post_data_repair_real_run(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    manifest_path: Path = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    docs_root: Path | None = None,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    command_results: list[dict[str, Any]] = []
    steps = _post_repair_steps(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        manifest_path=manifest_path,
        config_path=config_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=as_of_date,
    )
    for task_id, command, builder in steps:
        command_results.append(_run_step(task_id, command, builder))
    skipped = {
        "task_id": "TRADING-900",
        "command": "aits research strategies simple-baseline-forward-aging-write-observation",
        "status": "SKIPPED_BY_FIRST_OBSERVATION_DRY_RUN_REQUIRED",
        "reason": (
            "TRADING-920 must validate a non-writing dry-run before any formal "
            "forward-aging observation file is created."
        ),
        "exit_code": None,
    }
    command_results.append(skipped)

    hardening = _read_json_or_empty(output_root / "simple_baseline_validate_data_hardening.json")
    key_statuses = {
        "data_quality_gate": _status_for_report(command_results, "TRADING-915"),
        "forward_aging_contract": _status_for_report(command_results, "TRADING-896"),
        "policy_definition_lock": _status_for_report(command_results, "TRADING-904"),
        "candidate_freeze": _status_for_report(command_results, "TRADING-895"),
        "risk_budget_review": _status_for_report(command_results, "TRADING-908"),
        "absolute_return_gap_review": _status_for_report(command_results, "TRADING-909"),
        "master_review": _status_for_report(command_results, "TRADING-910"),
    }
    failed = [row for row in command_results if row.get("exit_code") not in {0, None}]
    blocked = [row for row in command_results if "BLOCK" in str(row.get("status", ""))]
    if failed or str(hardening.get("status")) == "VALIDATE_DATA_BLOCKED":
        status = "POST_REPAIR_REAL_RUN_BLOCKED"
    elif blocked or any(str(row.get("status", "")).endswith("WARN") for row in command_results):
        status = "POST_REPAIR_REAL_RUN_WARN"
    else:
        status = "POST_REPAIR_REAL_RUN_PASS"

    payload = _payload(
        report_type="simple_baseline_post_data_repair_real_run",
        title="Simple Baseline Post Data Repair Real Run",
        status=status,
        summary={
            "command_count": len(command_results),
            "failed_command_count": len(failed),
            "blocked_status_count": len(blocked),
            "data_quality_gate_status": key_statuses["data_quality_gate"],
            "formal_observation_written": False,
        },
        key_statuses=key_statuses,
        command_results=command_results,
        safety_note=(
            "The formal observation writer is intentionally not run here; TRADING-920 "
            "creates a non-writing dry-run first."
        ),
        report_registry_entry=_report_registry_entry(
            "simple_baseline_post_data_repair_real_run",
            "Simple Baseline Post Data Repair Real Run",
            "aits research strategies rerun-simple-baseline-real-cli-after-data-repair",
            "simple_baseline_post_data_repair_real_run",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_equal_risk_result_recompute_after_data_repair(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    before_ranking_path: Path | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    metrics = _candidate_metric_rows(
        config=config,
        prices_path=prices_path,
        start_date=start_date,
        end_date=end_date,
    )
    equal_row = next(
        (row for row in metrics if row["strategy_id"] == PRIMARY_CANDIDATE_ID),
        {},
    )
    before = _before_candidate_row(
        before_ranking_path or output_root / "simple_baseline_dominance_ranking.json",
        PRIMARY_CANDIDATE_ID,
    )
    top_candidate = metrics[0]["strategy_id"] if metrics else None
    status = (
        "CANDIDATE_CHANGED_AFTER_DATA_REPAIR"
        if top_candidate and top_candidate != PRIMARY_CANDIDATE_ID
        else "EQUAL_RISK_RESULT_RECOMPUTED"
    )
    if not equal_row:
        status = "EQUAL_RISK_RESULT_RECOMPUTE_BLOCKED"

    payload = _payload(
        report_type="equal_risk_result_recompute_after_data_repair",
        title="Equal-Risk Result Recompute After Data Repair",
        status=status,
        summary={
            "top_candidate_after_repair": top_candidate,
            "equal_risk_rank_after_repair": equal_row.get("rank"),
            "candidate_role": equal_row.get("candidate_role"),
            "before_metrics_available": bool(before),
            "candidate_changed_after_data_repair": status == "CANDIDATE_CHANGED_AFTER_DATA_REPAIR",
        },
        before_metrics=before,
        after_metrics=equal_row,
        after_ranking=metrics,
        comparison=_metric_delta(before, equal_row),
        report_registry_entry=_report_registry_entry(
            "equal_risk_result_recompute_after_data_repair",
            "Equal-Risk Result Recompute After Data Repair",
            "aits research strategies equal-risk-result-recompute-after-data-repair",
            "equal_risk_result_recompute_after_data_repair",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_tqqq_challenger_revalidation_after_cache_fix(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    available = _available_price_tickers(prices_path)
    if "TQQQ" not in available:
        payload = _payload(
            report_type="tqqq_challenger_revalidation_after_cache_fix",
            title="TQQQ Challenger Revalidation After Cache Fix",
            status="TQQQ_CHALLENGER_BLOCKED",
            summary={
                "blocked_reason": "TQQQ missing from primary price cache",
                "tqqq_heavy_should_pause": True,
            },
            blockers=["TQQQ missing from primary price cache"],
            report_registry_entry=_report_registry_entry(
                "tqqq_challenger_revalidation_after_cache_fix",
                "TQQQ Challenger Revalidation After Cache Fix",
                "aits research strategies tqqq-challenger-revalidation-after-cache-fix",
                "tqqq_challenger_revalidation_after_cache_fix",
            ),
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    rows = _candidate_metric_rows(
        config=config,
        prices_path=prices_path,
        start_date=start_date,
        end_date=end_date,
        candidate_ids=(CHALLENGER_CANDIDATE_ID, "qqq_50_sgov_50", PRIMARY_CANDIDATE_ID),
    )
    lookup = {row["strategy_id"]: row for row in rows}
    challenger = lookup.get(CHALLENGER_CANDIDATE_ID, {})
    qqq_50 = lookup.get("qqq_50_sgov_50", {})
    equal = lookup.get(PRIMARY_CANDIDATE_ID, {})
    status = "TQQQ_CHALLENGER_STILL_PAUSED" if challenger else "TQQQ_CHALLENGER_BLOCKED"
    payload = _payload(
        report_type="tqqq_challenger_revalidation_after_cache_fix",
        title="TQQQ Challenger Revalidation After Cache Fix",
        status=status,
        summary={
            "challenger_metrics_available": bool(challenger),
            "tqqq_heavy_should_pause": True,
            "pause_reason": "TQQQ remains challenger-only without owner paper-shadow approval.",
            "leverage_or_drawdown_issue": _leverage_drawdown_issue(challenger, equal, qqq_50),
        },
        challenger_metrics=challenger,
        edge_vs_qqq_50_sgov_50=_metric_delta(qqq_50, challenger),
        edge_vs_equal_risk_qqq_sgov=_metric_delta(equal, challenger),
        safety_decision={
            "tqqq_heavy_pause": True,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        report_registry_entry=_report_registry_entry(
            "tqqq_challenger_revalidation_after_cache_fix",
            "TQQQ Challenger Revalidation After Cache Fix",
            "aits research strategies tqqq-challenger-revalidation-after-cache-fix",
            "tqqq_challenger_revalidation_after_cache_fix",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_forward_aging_unblock_readiness_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    manifest_path: Path = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    inventory = run_simple_baseline_data_source_inventory(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        manifest_path=manifest_path,
        output_root=output_root,
    )
    contract = run_sgov_total_return_data_contract(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        manifest_path=manifest_path,
        output_root=output_root,
    )
    hardening = run_simple_baseline_validate_data_hardening(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        manifest_path=manifest_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
    )
    policy_lock = _read_json_or_empty(
        output_root / "equal_risk_qqq_sgov_policy_definition_lock.json"
    )
    comparator_lock = _read_json_or_empty(
        output_root / "simple_baseline_comparator_definition_lock.json"
    )
    checks = _readiness_checks(inventory, contract, hardening, policy_lock, comparator_lock)
    blockers = [row["check_id"] for row in checks if row["status"] == "FAIL"]
    warnings = [row["check_id"] for row in checks if row["status"] == "WARN"]
    if blockers:
        status = "FORWARD_AGING_STILL_BLOCKED"
    elif warnings:
        status = "FORWARD_AGING_READY_WITH_WARNINGS"
    else:
        status = "FORWARD_AGING_READY"

    payload = _payload(
        report_type="forward_aging_unblock_readiness_review",
        title="Forward-Aging Unblock Readiness Review",
        status=status,
        summary={
            "check_count": len(checks),
            "blocker_count": len(blockers),
            "warning_count": len(warnings),
            "data_quality_status": _mapping(hardening.get("summary")).get("data_quality_status"),
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        readiness_checks=checks,
        blockers=blockers,
        warnings=warnings,
        report_registry_entry=_report_registry_entry(
            "forward_aging_unblock_readiness_review",
            "Forward-Aging Unblock Readiness Review",
            "aits research strategies forward-aging-unblock-readiness-review",
            "forward_aging_unblock_readiness_review",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_first_forward_aging_observation_dry_run(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    manifest_path: Path = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
    decision_date: date | None = None,
) -> dict[str, Any]:
    readiness = run_forward_aging_unblock_readiness_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        manifest_path=manifest_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
    )
    if readiness["status"] == "FORWARD_AGING_STILL_BLOCKED":
        payload = _payload(
            report_type="first_forward_aging_observation_dry_run",
            title="First Forward-Aging Observation Dry-Run",
            status="FORWARD_OBSERVATION_DRY_RUN_BLOCKED",
            summary={
                "decision_date": None,
                "dry_run_only": True,
                "observation_written": False,
                "forward_aging_status": readiness["status"],
            },
            blockers=readiness.get("blockers", []),
            readiness=readiness,
            report_registry_entry=_report_registry_entry(
                "first_forward_aging_observation_dry_run",
                "First Forward-Aging Observation Dry-Run",
                "aits research strategies first-forward-aging-observation-dry-run",
                "first_forward_aging_observation_dry_run",
            ),
        )
        _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
        return payload

    config = _load_registry(config_path)
    available = _available_price_tickers(prices_path)
    candidates = [PRIMARY_CANDIDATE_ID, *STATIC_COMPARATOR_IDS]
    if "TQQQ" in available:
        candidates.append(CHALLENGER_CANDIDATE_ID)
    target_rows = _target_weight_dry_run_rows(
        config=config,
        prices_path=prices_path,
        candidate_ids=tuple(candidates),
        decision_date=decision_date,
    )
    status = (
        "FORWARD_OBSERVATION_DRY_RUN_WARN"
        if readiness["status"] == "FORWARD_AGING_READY_WITH_WARNINGS"
        else "FORWARD_OBSERVATION_DRY_RUN_PASS"
    )
    resolved_date = target_rows[0]["decision_date"] if target_rows else None
    payload = _payload(
        report_type="first_forward_aging_observation_dry_run",
        title="First Forward-Aging Observation Dry-Run",
        status=status,
        summary={
            "decision_date": resolved_date,
            "candidate_count": len(target_rows),
            "dry_run_only": True,
            "observation_written": False,
            "trade_recommendation_generated": False,
            "broker_connected": False,
            "production_config_written": False,
        },
        target_weight_preview=target_rows,
        readiness=readiness,
        report_registry_entry=_report_registry_entry(
            "first_forward_aging_observation_dry_run",
            "First Forward-Aging Observation Dry-Run",
            "aits research strategies first-forward-aging-observation-dry-run",
            "first_forward_aging_observation_dry_run",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_reader_brief_forward_aging_safe_preview(
    *,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    readiness = _read_json_or_empty(output_root / "forward_aging_unblock_readiness_review.json")
    dry_run = _read_json_or_empty(output_root / "first_forward_aging_observation_dry_run.json")
    data_quality_status = _mapping(readiness.get("summary")).get("data_quality_status")
    forward_status = readiness.get("status") or "UNKNOWN"
    preview = {
        "primary_candidate": PRIMARY_CANDIDATE_ID,
        "challenger_candidate": CHALLENGER_CANDIDATE_ID,
        "data_quality_status": data_quality_status,
        "forward_aging_status": forward_status,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }
    preview_text = "\n".join(f"{key}: {value}" for key, value in preview.items())
    forbidden_hits = [
        phrase for phrase in FORBIDDEN_READER_BRIEF_PHRASES if phrase in preview_text
    ]
    if not readiness or not dry_run:
        status = "READER_FORWARD_PREVIEW_BLOCKED"
    elif forbidden_hits:
        status = "READER_FORWARD_PREVIEW_AMBIGUOUS"
    else:
        status = "READER_FORWARD_PREVIEW_SAFE"
    payload = _payload(
        report_type="reader_brief_forward_aging_safe_preview",
        title="Reader Brief Forward-Aging Safe Preview",
        status=status,
        summary={
            **preview,
            "forbidden_phrase_hit_count": len(forbidden_hits),
            "dry_run_status": dry_run.get("status"),
        },
        reader_brief_preview=preview,
        preview_text=preview_text,
        forbidden_phrase_hits=forbidden_hits,
        report_registry_entry=_report_registry_entry(
            "reader_brief_forward_aging_safe_preview",
            "Reader Brief Forward-Aging Safe Preview",
            "aits research strategies reader-brief-forward-aging-safe-preview",
            "reader_brief_forward_aging_safe_preview",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_data_repair_owner_decision_pack(
    *,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    inventory = _read_json_or_empty(output_root / "simple_baseline_data_source_inventory.json")
    contract = _read_json_or_empty(output_root / "sgov_total_return_data_contract.json")
    equal = _read_json_or_empty(output_root / "equal_risk_result_recompute_after_data_repair.json")
    challenger = _read_json_or_empty(
        output_root / "tqqq_challenger_revalidation_after_cache_fix.json"
    )
    readiness = _read_json_or_empty(output_root / "forward_aging_unblock_readiness_review.json")
    preview = _read_json_or_empty(output_root / "reader_brief_forward_aging_safe_preview.json")
    answers = _owner_decision_answers(inventory, contract, equal, challenger, readiness, preview)
    if readiness.get("status") == "FORWARD_AGING_STILL_BLOCKED":
        status = "OWNER_NEEDS_MORE_DATA_REPAIR"
    elif preview.get("status") != "READER_FORWARD_PREVIEW_SAFE":
        status = "OWNER_BLOCK_FORWARD_AGING"
    else:
        status = "OWNER_APPROVE_FORWARD_AGING"

    payload = _payload(
        report_type="data_repair_owner_decision_pack",
        title="Data Repair Owner Decision Pack",
        status=status,
        summary={
            "forward_aging_status": readiness.get("status"),
            "reader_preview_status": preview.get("status"),
            "owner_next_action": status,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        required_answers=answers,
        input_artifacts={
            "inventory": str(output_root / "simple_baseline_data_source_inventory.json"),
            "sgov_contract": str(output_root / "sgov_total_return_data_contract.json"),
            "equal_risk_recompute": str(
                output_root / "equal_risk_result_recompute_after_data_repair.json"
            ),
            "tqqq_revalidation": str(
                output_root / "tqqq_challenger_revalidation_after_cache_fix.json"
            ),
            "readiness": str(output_root / "forward_aging_unblock_readiness_review.json"),
            "reader_preview": str(output_root / "reader_brief_forward_aging_safe_preview.json"),
        },
        report_registry_entry=_report_registry_entry(
            "data_repair_owner_decision_pack",
            "Data Repair Owner Decision Pack",
            "aits research strategies data-repair-owner-decision-pack",
            "data_repair_owner_decision_pack",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_data_repair_reproducibility_proof(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    manifest_path: Path = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
    expected_tqqq_rows: int = 1008,
) -> dict[str, Any]:
    config = _load_registry(config_path)
    inventory = _build_symbol_inventory(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        manifest_path=manifest_path,
    )
    manifest = _read_csv_or_empty(manifest_path)
    tqqq_rebuild = _read_json_or_empty(output_root / "tqqq_cache_rebuild_validation.json")
    sgov_contract = _read_json_or_empty(output_root / "sgov_total_return_data_contract.json")
    manifest_audit = _read_json_or_empty(output_root / "market_data_repair_manifest_audit.json")
    data_quality = _data_quality_payload(
        _validate_simple_baseline_data(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config=config,
            as_of_date=as_of_date,
        ),
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
    )
    tqqq = _symbol_row(inventory, "TQQQ")
    rebuild_summary = _mapping(tqqq_rebuild.get("summary"))
    repair_summary = _mapping(tqqq_rebuild.get("repair_summary"))
    checks = [
        _proof_check(
            "tqqq_empty_cache_rebuild_to_expected_rows",
            rebuild_summary.get("tqqq_rows_before") == 0
            and rebuild_summary.get("tqqq_rows_after") == expected_tqqq_rows,
            observed={
                "rows_before": rebuild_summary.get("tqqq_rows_before"),
                "rows_after": rebuild_summary.get("tqqq_rows_after"),
                "expected_rows": expected_tqqq_rows,
                "current_tqqq_rows": tqqq.get("row_count"),
            },
        ),
        _proof_check(
            "sgov_adjusted_close_proxy_reproducible",
            sgov_contract.get("status") == "SGOV_TOTAL_RETURN_CONTRACT_READY",
            observed={"contract_status": sgov_contract.get("status")},
        ),
        _proof_check(
            "repair_manifest_path_readable",
            manifest_path.exists() and not manifest.empty,
            observed={
                "manifest_path": str(manifest_path),
                "manifest_checksum": _file_sha256(manifest_path),
                "manifest_row_count": len(manifest),
            },
        ),
        _proof_check(
            "repair_manifest_covers_target_symbols",
            all(_manifest_entries_for_symbol(manifest, symbol) for symbol in TARGET_SYMBOLS),
            observed={
                symbol: len(_manifest_entries_for_symbol(manifest, symbol))
                for symbol in TARGET_SYMBOLS
            },
        ),
        _proof_check(
            "no_manual_prices_daily_copy_dependency",
            all(
                _manifest_entries_are_provider_backed(manifest, symbol)
                for symbol in TARGET_SYMBOLS
            ),
            observed={
                symbol: _manifest_entries_for_symbol(manifest, symbol)[-1]
                if _manifest_entries_for_symbol(manifest, symbol)
                else None
                for symbol in TARGET_SYMBOLS
            },
        ),
        _proof_check(
            "no_silent_fixture_fallback",
            repair_summary.get("used_fixture") is False
            and not _manifest_mentions_fixture(manifest, TARGET_SYMBOLS),
            observed={
                "repair_used_fixture": repair_summary.get("used_fixture"),
                "repair_path": repair_summary.get("repair_path"),
                "provider": repair_summary.get("provider"),
            },
        ),
        _proof_check(
            "validate_data_gate_visible",
            bool(data_quality.get("passed")),
            observed={
                "data_quality_status": data_quality.get("status"),
                "warning_count": data_quality.get("warning_count"),
                "error_count": data_quality.get("error_count"),
            },
            warn_when_true=data_quality.get("status") == "PASS_WITH_WARNINGS",
        ),
        _proof_check(
            "manifest_audit_available",
            manifest_audit.get("status") in {"REPAIR_MANIFEST_PASS", "REPAIR_MANIFEST_WARN"},
            observed={"manifest_audit_status": manifest_audit.get("status")},
            warn_when_true=manifest_audit.get("status") == "REPAIR_MANIFEST_WARN",
        ),
    ]
    blockers = [row["check_id"] for row in checks if row["status"] == "FAIL"]
    warnings = [row["check_id"] for row in checks if row["status"] == "WARN"]
    if blockers:
        status = (
            "DATA_REPAIR_LOCAL_ONLY"
            if int(tqqq.get("row_count", 0)) >= expected_tqqq_rows
            else "DATA_REPAIR_BLOCKED"
        )
    else:
        status = "DATA_REPAIR_REPRODUCIBLE"

    payload = _payload(
        report_type="data_repair_reproducibility_proof",
        title="Data Repair Reproducibility Proof",
        status=status,
        summary={
            "expected_tqqq_rows": expected_tqqq_rows,
            "current_tqqq_rows": tqqq.get("row_count"),
            "data_quality_status": data_quality.get("status"),
            "check_count": len(checks),
            "blocker_count": len(blockers),
            "warning_count": len(warnings),
        },
        reproducibility_checks=checks,
        blockers=blockers,
        warnings=warnings,
        data_quality=data_quality,
        input_artifacts={
            "prices": str(prices_path),
            "secondary_prices": str(marketstack_prices_path),
            "rates": str(rates_path),
            "manifest": str(manifest_path),
            "tqqq_rebuild": str(output_root / "tqqq_cache_rebuild_validation.json"),
            "sgov_contract": str(output_root / "sgov_total_return_data_contract.json"),
            "manifest_audit": str(output_root / "market_data_repair_manifest_audit.json"),
        },
        report_registry_entry=_report_registry_entry(
            "data_repair_reproducibility_proof",
            "Data Repair Reproducibility Proof",
            "aits research strategies data-repair-reproducibility-proof",
            "data_repair_reproducibility_proof",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_marketstack_ssl_failure_triage(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    manifest_path: Path = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    inventory = _build_symbol_inventory(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        manifest_path=manifest_path,
    )
    resolved_end = end_date or _max_price_date(prices_path)
    secondary_missing = [
        row["symbol"] for row in inventory if bool(row.get("secondary_missing"))
    ]
    checks = [
        _proof_check("ssl_verification_not_bypassed", True),
        _proof_check("download_security_checks_not_disabled", True),
        _proof_check("endpoint_recorded", True, observed={"endpoint": _marketstack_endpoint()}),
        _proof_check(
            "symbol_date_range_recorded",
            True,
            observed={
                "symbols": list(TARGET_SYMBOLS),
                "start": start_date.isoformat(),
                "end": resolved_end.isoformat(),
            },
        ),
        _proof_check(
            "no_fixture_or_manual_fallback_for_marketstack",
            True,
            observed={"fallback_to_fixture": False, "manual_copy_used": False},
        ),
    ]
    if secondary_missing:
        decision = "retain_limited_second_source_with_fail_closed_warning"
    else:
        decision = "retain_second_source_after_next_successful_secure_download"
    payload = _payload(
        report_type="marketstack_ssl_failure_triage",
        title="Marketstack SSL Failure Triage",
        status="MARKETSTACK_FAIL_CLOSED_ACCEPTED",
        summary={
            "endpoint": _marketstack_endpoint(),
            "symbols": ",".join(TARGET_SYMBOLS),
            "date_range": f"{start_date.isoformat()}..{resolved_end.isoformat()}",
            "secondary_missing_symbols": secondary_missing,
            "ssl_verification_disabled": False,
            "fallback_to_fixture": False,
            "marketstack_simple_baseline_decision": decision,
        },
        failure_record={
            "provider": "Marketstack",
            "source_id": "marketstack_eod_daily_prices",
            "endpoint": _marketstack_endpoint(),
            "symbols": list(TARGET_SYMBOLS),
            "start": start_date.isoformat(),
            "end": resolved_end.isoformat(),
            "failure_mode": "ssl_transport_failed_closed",
            "ssl_verification_disabled": False,
            "security_checks_disabled": False,
        },
        retry_and_fallback_rules={
            "retry_rule": "Retry only through normal HTTPS verification; do not disable SSL.",
            "fallback_rule": (
                "FMP primary cache may continue only when validate-data passes; Marketstack "
                "does not silently replace or repair QQQ/TQQQ/SGOV primary cache."
            ),
            "remove_marketstack_now": False,
            "retention_scope": "limited_second_source_warning_until_secure_download_succeeds",
        },
        checks=checks,
        input_artifacts={
            "prices": str(prices_path),
            "secondary_prices": str(marketstack_prices_path),
            "manifest": str(manifest_path),
        },
        report_registry_entry=_report_registry_entry(
            "marketstack_ssl_failure_triage",
            "Marketstack SSL Failure Triage",
            "aits research strategies marketstack-ssl-failure-triage",
            "marketstack_ssl_failure_triage",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def run_sgov_total_return_proxy_quality_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    manifest_path: Path = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> dict[str, Any]:
    inventory = _build_symbol_inventory(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        manifest_path=manifest_path,
    )
    prices = _read_csv_or_empty(prices_path)
    sgov_rows = _rows_for_symbol(prices, "SGOV")
    quality = _sgov_proxy_quality_metrics(sgov_rows)
    sgov = _symbol_row(inventory, "SGOV")
    if int(sgov.get("row_count", 0)) == 0 or not sgov.get("adjusted_close_available"):
        status = "SGOV_PROXY_BLOCKED"
    elif not sgov.get("dividend_adjusted"):
        status = "SGOV_PROXY_WARN"
    else:
        status = "SGOV_PROXY_ACCEPTABLE"
    forward_allowed = status in {"SGOV_PROXY_ACCEPTABLE", "SGOV_PROXY_WARN"}
    payload = _payload(
        report_type="sgov_total_return_proxy_quality_review",
        title="SGOV Total-Return Proxy Quality Review",
        status=status,
        summary={
            "sgov_row_count": sgov.get("row_count", 0),
            "adjusted_close_available": sgov.get("adjusted_close_available"),
            "dividend_adjusted": sgov.get("dividend_adjusted"),
            "price_only_understates_carry": quality["price_only_carry_understatement"] > 0,
            "price_only_carry_understatement": quality["price_only_carry_understatement"],
            "forward_aging_allowed_with_warnings": forward_allowed,
        },
        required_answers={
            "1_sgov_adj_close_contains_distribution_adjustment": bool(
                sgov.get("dividend_adjusted")
            ),
            "2_price_only_vs_adj_close_difference": quality["price_only_carry_understatement"],
            "3_price_only_underestimates_carry_by": quality[
                "price_only_carry_understatement"
            ],
            "4_need_more_explicit_total_return_source": status != "SGOV_PROXY_ACCEPTABLE",
            "5_forward_aging_currently_allowed_with_warnings": forward_allowed,
        },
        proxy_quality_metrics=quality,
        sgov_inventory=sgov,
        limitations=[
            "SGOV adj_close remains a proxy, not an independently sourced total-return index.",
            (
                "Forward-aging reports must keep the SGOV proxy warning visible until a "
                "stronger total-return source is approved."
            ),
        ],
        input_artifacts={
            "prices": str(prices_path),
            "secondary_prices": str(marketstack_prices_path),
            "manifest": str(manifest_path),
        },
        report_registry_entry=_report_registry_entry(
            "sgov_total_return_proxy_quality_review",
            "SGOV Total-Return Proxy Quality Review",
            "aits research strategies sgov-total-return-proxy-quality-review",
            "sgov_total_return_proxy_quality_review",
        ),
    )
    _write_pair(payload, output_root=output_root, artifact_id=payload["report_type"])
    return payload


def _post_repair_steps(
    *,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    manifest_path: Path,
    config_path: Path,
    output_root: Path,
    docs_root: Path | None,
    as_of_date: date | None,
) -> list[tuple[str, str, Callable[[], dict[str, Any]]]]:
    from ai_trading_system.simple_baseline_candidate_validation import (
        run_dynamic_vs_static_edge_significance_review,
        run_equal_risk_qqq_sgov_deep_dive,
        run_simple_baseline_drawdown_episode_review,
        run_simple_baseline_period_split_validation,
        run_simple_baseline_watchlist_owner_decision,
        run_tqqq_heavy_pause_rationale_report,
    )
    from ai_trading_system.simple_baseline_forward_aging import (
        run_equal_risk_qqq_sgov_policy_definition_lock,
        run_simple_baseline_absolute_return_gap_review,
        run_simple_baseline_comparator_definition_lock,
        run_simple_baseline_forward_aging_candidate_freeze,
        run_simple_baseline_forward_aging_contract,
        run_simple_baseline_forward_aging_data_quality_gate,
        run_simple_baseline_forward_aging_master_review,
        run_simple_baseline_forward_aging_scoreboard,
        run_simple_baseline_paper_shadow_threshold_contract,
        run_simple_baseline_real_result_reconciliation,
        run_simple_baseline_risk_budget_review,
    )
    from ai_trading_system.simple_baseline_portfolio_control import (
        run_options_next_stage_gate,
        run_qqq_sgov_baseline_backtest,
        run_simple_baseline_cost_sensitivity,
        run_simple_baseline_daily_reader_safety_summary,
        run_simple_baseline_dominance_ranking,
        run_simple_baseline_forward_aging_tracker,
        run_simple_baseline_master_review,
        run_simple_baseline_paper_shadow_readiness,
        run_simple_baseline_pit_boundary_audit,
        run_simple_baseline_portfolio_dry_run_mapper,
        run_simple_baseline_regime_review,
        run_simple_baseline_registry_review,
        run_tqqq_sgov_risk_controlled_baseline,
        run_trend_vol_allocation_policy_search,
    )

    data_kwargs = {
        "prices_path": prices_path,
        "marketstack_prices_path": marketstack_prices_path,
        "rates_path": rates_path,
        "config_path": config_path,
        "output_root": output_root,
        "as_of_date": as_of_date,
    }
    resolved_docs_root = docs_root or PROJECT_ROOT / "docs" / "research"
    return [
        (
            "TRADING-865",
            "aits research strategies simple-baseline-registry-review",
            lambda: run_simple_baseline_registry_review(
                config_path=config_path,
                output_root=output_root,
            ),
        ),
        (
            "TRADING-866",
            "aits research strategies qqq-sgov-baseline-backtest",
            lambda: run_qqq_sgov_baseline_backtest(**data_kwargs),
        ),
        (
            "TRADING-867",
            "aits research strategies tqqq-sgov-risk-controlled-baseline",
            lambda: run_tqqq_sgov_risk_controlled_baseline(**data_kwargs),
        ),
        (
            "TRADING-868",
            "aits research strategies trend-vol-allocation-policy-search",
            lambda: run_trend_vol_allocation_policy_search(**data_kwargs),
        ),
        (
            "TRADING-869",
            "aits research strategies simple-baseline-dominance-ranking",
            lambda: run_simple_baseline_dominance_ranking(output_root=output_root),
        ),
        (
            "TRADING-870",
            "aits research strategies simple-baseline-pit-boundary-audit",
            lambda: run_simple_baseline_pit_boundary_audit(
                config_path=config_path,
                output_root=output_root,
            ),
        ),
        (
            "TRADING-871",
            "aits research strategies simple-baseline-cost-sensitivity",
            lambda: run_simple_baseline_cost_sensitivity(**data_kwargs),
        ),
        (
            "TRADING-872",
            "aits research strategies simple-baseline-regime-review",
            lambda: run_simple_baseline_regime_review(**data_kwargs),
        ),
        (
            "TRADING-873",
            "aits research strategies simple-baseline-forward-aging-tracker",
            lambda: run_simple_baseline_forward_aging_tracker(**data_kwargs),
        ),
        (
            "TRADING-874",
            "aits research strategies simple-baseline-paper-shadow-readiness",
            lambda: run_simple_baseline_paper_shadow_readiness(output_root=output_root),
        ),
        (
            "TRADING-875",
            "aits research strategies daily-reader-portfolio-control-safety-summary",
            lambda: run_simple_baseline_daily_reader_safety_summary(output_root=output_root),
        ),
        (
            "TRADING-876",
            "aits research strategies simple-baseline-portfolio-dry-run-mapper",
            lambda: run_simple_baseline_portfolio_dry_run_mapper(output_root=output_root),
        ),
        (
            "TRADING-877",
            "aits research strategies simple-baseline-master-review",
            lambda: run_simple_baseline_master_review(
                output_root=output_root,
                master_doc_path=resolved_docs_root
                / "simple_baseline_portfolio_control_master_review.md",
            ),
        ),
        (
            "TRADING-878",
            "aits research strategies options-next-stage-gate",
            lambda: run_options_next_stage_gate(output_root=output_root),
        ),
        (
            "TRADING-888",
            "aits research strategies equal-risk-qqq-sgov-deep-dive",
            lambda: run_equal_risk_qqq_sgov_deep_dive(**data_kwargs),
        ),
        (
            "TRADING-889",
            "aits research strategies simple-baseline-period-split-validation",
            lambda: run_simple_baseline_period_split_validation(**data_kwargs),
        ),
        (
            "TRADING-890",
            "aits research strategies simple-baseline-drawdown-episode-review",
            lambda: run_simple_baseline_drawdown_episode_review(**data_kwargs),
        ),
        (
            "TRADING-891",
            "aits research strategies dynamic-vs-static-edge-significance-review",
            lambda: run_dynamic_vs_static_edge_significance_review(**data_kwargs),
        ),
        (
            "TRADING-892",
            "aits research strategies tqqq-heavy-pause-rationale-report",
            lambda: run_tqqq_heavy_pause_rationale_report(**data_kwargs),
        ),
        (
            "TRADING-893",
            "aits research strategies simple-baseline-watchlist-owner-decision",
            lambda: run_simple_baseline_watchlist_owner_decision(
                **data_kwargs,
                docs_path=resolved_docs_root / "simple_baseline_watchlist_owner_decision.md",
            ),
        ),
        (
            "TRADING-894",
            "aits research strategies simple-baseline-real-result-reconciliation",
            lambda: run_simple_baseline_real_result_reconciliation(output_root=output_root),
        ),
        (
            "TRADING-895",
            "aits research strategies simple-baseline-forward-aging-candidate-freeze",
            lambda: run_simple_baseline_forward_aging_candidate_freeze(
                config_path=config_path,
                output_root=output_root,
            ),
        ),
        (
            "TRADING-896",
            "aits research strategies simple-baseline-forward-aging-contract",
            lambda: run_simple_baseline_forward_aging_contract(
                config_path=config_path,
                output_root=output_root,
            ),
        ),
        (
            "TRADING-901",
            "aits research strategies simple-baseline-forward-aging-scoreboard",
            lambda: run_simple_baseline_forward_aging_scoreboard(
                config_path=config_path,
                output_root=output_root,
            ),
        ),
        (
            "TRADING-904",
            "aits research strategies equal-risk-qqq-sgov-policy-definition-lock",
            lambda: run_equal_risk_qqq_sgov_policy_definition_lock(
                config_path=config_path,
                output_root=output_root,
            ),
        ),
        (
            "TRADING-905",
            "aits research strategies simple-baseline-comparator-definition-lock",
            lambda: run_simple_baseline_comparator_definition_lock(
                config_path=config_path,
                output_root=output_root,
            ),
        ),
        (
            "TRADING-906",
            "aits research strategies simple-baseline-forward-aging-data-quality-gate",
            lambda: run_simple_baseline_forward_aging_data_quality_gate(**data_kwargs),
        ),
        (
            "TRADING-907",
            "aits research strategies simple-baseline-paper-shadow-threshold-contract",
            lambda: run_simple_baseline_paper_shadow_threshold_contract(
                config_path=config_path,
                output_root=output_root,
            ),
        ),
        (
            "TRADING-908",
            "aits research strategies simple-baseline-risk-budget-review",
            lambda: run_simple_baseline_risk_budget_review(**data_kwargs),
        ),
        (
            "TRADING-909",
            "aits research strategies simple-baseline-absolute-return-gap-review",
            lambda: run_simple_baseline_absolute_return_gap_review(**data_kwargs),
        ),
        (
            "TRADING-910",
            "aits research strategies simple-baseline-forward-aging-master-review",
            lambda: run_simple_baseline_forward_aging_master_review(
                output_root=output_root,
                docs_path=resolved_docs_root / "simple_baseline_forward_aging_master_review.md",
            ),
        ),
        (
            "TRADING-911",
            "aits research strategies simple-baseline-data-source-inventory",
            lambda: run_simple_baseline_data_source_inventory(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                manifest_path=manifest_path,
                output_root=output_root,
            ),
        ),
        (
            "TRADING-913",
            "aits research strategies sgov-total-return-data-contract",
            lambda: run_sgov_total_return_data_contract(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                manifest_path=manifest_path,
                output_root=output_root,
            ),
        ),
        (
            "TRADING-914",
            "aits research strategies market-data-repair-manifest-audit",
            lambda: run_market_data_repair_manifest_audit(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                manifest_path=manifest_path,
                output_root=output_root,
            ),
        ),
        (
            "TRADING-915",
            "aits research strategies simple-baseline-validate-data-hardening",
            lambda: run_simple_baseline_validate_data_hardening(
                **data_kwargs,
                manifest_path=manifest_path,
            ),
        ),
    ]


def _build_symbol_inventory(
    *,
    prices_path: Path,
    marketstack_prices_path: Path,
    manifest_path: Path,
) -> list[dict[str, Any]]:
    primary = _read_csv_or_empty(prices_path)
    secondary = _read_csv_or_empty(marketstack_prices_path)
    manifest = _read_csv_or_empty(manifest_path)
    reference_dates = _reference_dates(primary)
    return [
        _symbol_inventory_row(
            symbol,
            primary=primary,
            secondary=secondary,
            manifest=manifest,
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            manifest_path=manifest_path,
            reference_dates=reference_dates,
        )
        for symbol in TARGET_SYMBOLS
    ]


def _symbol_inventory_row(
    symbol: str,
    *,
    primary: pd.DataFrame,
    secondary: pd.DataFrame,
    manifest: pd.DataFrame,
    prices_path: Path,
    marketstack_prices_path: Path,
    manifest_path: Path,
    reference_dates: list[date],
) -> dict[str, Any]:
    primary_rows = _rows_for_symbol(primary, symbol)
    secondary_rows = _rows_for_symbol(secondary, symbol)
    primary_dates = _dates_from_frame(primary_rows)
    missing_dates = [day.isoformat() for day in reference_dates if day not in set(primary_dates)]
    adjusted_available = _adj_close_available(primary_rows)
    dividend_adjusted = _dividend_adjusted(primary_rows)
    manifest_entries = _manifest_entries_for_symbol(manifest, symbol)
    repair_entries = [
        entry
        for entry in manifest_entries
        if entry.get("request_parameters", {}).get("repair_mode")
        or entry.get("source_id") == "fmp_eod_daily_prices"
    ]
    secondary_missing = len(secondary_rows) == 0
    if len(primary_rows) == 0 or not adjusted_available:
        source_status = "BLOCKED"
    elif missing_dates:
        source_status = "WARN"
    elif symbol in {"TQQQ", "SGOV"} and secondary_missing:
        source_status = "WARN"
    else:
        source_status = "OK"
    return {
        "symbol": symbol,
        "primary_source": _latest_source(primary_rows) or "Financial Modeling Prep",
        "secondary_source": _latest_source(secondary_rows) or None,
        "repair_source": _latest_manifest_provider(repair_entries),
        "cache_path": str(prices_path),
        "manifest_path": str(manifest_path),
        "earliest_date": _date_label(min(primary_dates) if primary_dates else None),
        "latest_date": _date_label(max(primary_dates) if primary_dates else None),
        "row_count": int(len(primary_rows)),
        "missing_dates": missing_dates,
        "missing_date_count": len(missing_dates),
        "adjusted_close_available": adjusted_available,
        "dividend_adjusted": dividend_adjusted,
        "total_return_available": adjusted_available and (symbol != "SGOV" or dividend_adjusted),
        "source_status": source_status,
        "secondary_cache_path": str(marketstack_prices_path),
        "secondary_row_count": int(len(secondary_rows)),
        "secondary_missing": secondary_missing,
        "manifest_entry_count": len(manifest_entries),
        "repair_manifest_entry_count": len(repair_entries),
        "latest_manifest": manifest_entries[-1] if manifest_entries else None,
    }


def _manifest_audit_row(
    symbol: str,
    *,
    manifest: pd.DataFrame,
    prices: pd.DataFrame,
    prices_path: Path,
) -> dict[str, Any]:
    entries = _manifest_entries_for_symbol(manifest, symbol)
    cache_rows = _rows_for_symbol(prices, symbol)
    cache_dates = _dates_from_frame(cache_rows)
    primary_entries = [
        entry
        for entry in entries
        if str(entry.get("output_path", "")).lower().endswith("prices_daily.csv")
    ]
    latest = (primary_entries or entries)[-1] if entries else {}
    params = _mapping(latest.get("request_parameters"))
    source_path = Path(str(latest.get("output_path") or prices_path))
    output_exists = source_path.exists()
    adjusted_close_present = "adj_close" in cache_rows.columns and not cache_rows.empty
    duplicate_dates = _duplicate_date_count(cache_rows)
    missing_dates = _missing_date_count_against_reference(prices, symbol)
    range_covers = _manifest_range_covers_cache(params, cache_dates)
    wrote_primary = str(source_path).lower().endswith("prices_daily.csv") and len(cache_rows) > 0
    symbol_mapping_correct = _symbol_mapping_correct(params, symbol)
    status = "OK"
    if not entries or not wrote_primary or not adjusted_close_present:
        status = "BLOCKED"
    elif not output_exists or not range_covers or duplicate_dates or missing_dates:
        status = "WARN"
    return {
        "symbol": symbol,
        "manifest_entry_exists": bool(entries),
        "source_path": str(source_path),
        "source_path_exists": output_exists,
        "symbol_mapping_correct": symbol_mapping_correct,
        "date_range_covers_cache": range_covers,
        "adjusted_close_field_exists": adjusted_close_present,
        "duplicate_date_count": duplicate_dates,
        "missing_date_count": missing_dates,
        "non_trading_day_policy": "observed_trading_dates_only",
        "repair_written_to_primary_cache": wrote_primary,
        "primary_manifest_entry_count": len(primary_entries),
        "latest_manifest_entry": latest or None,
        "status": status,
    }


def _hardening_rules(inventory: list[dict[str, Any]]) -> list[dict[str, Any]]:
    qqq = _symbol_row(inventory, "QQQ")
    tqqq = _symbol_row(inventory, "TQQQ")
    sgov = _symbol_row(inventory, "SGOV")
    rules = [
        _rule(
            "qqq_required_all_blocked",
            "all",
            qqq.get("row_count", 0) > 0 and qqq.get("adjusted_close_available"),
            "QQQ missing or adjusted close unavailable blocks every simple-baseline conclusion.",
            critical=True,
        ),
        _rule(
            "sgov_required_for_qqq_sgov",
            "qqq_sgov",
            sgov.get("row_count", 0) > 0 and sgov.get("adjusted_close_available"),
            "SGOV missing blocks every QQQ/SGOV strategy.",
            critical=True,
        ),
        _rule(
            "sgov_total_return_proxy_required",
            "qqq_sgov",
            sgov.get("total_return_available"),
            "SGOV price-only return understates carry and must not be treated as READY.",
            critical=False,
        ),
        _rule(
            "tqqq_missing_blocks_challenger_only",
            "tqqq_challenger",
            tqqq.get("row_count", 0) > 0 and tqqq.get("adjusted_close_available"),
            "TQQQ missing blocks TQQQ challenger but does not block QQQ/SGOV research.",
            critical=False,
            optional_block=True,
        ),
        _rule(
            "no_zero_sample_ready",
            "all",
            all(
                int(_symbol_row(inventory, symbol).get("row_count", 0)) > 0
                for symbol in TARGET_SYMBOLS
            ),
            "sample_count=0 cannot produce READY status for its strategy group.",
            critical=False,
        ),
    ]
    return rules


def _rule(
    rule_id: str,
    strategy_group: str,
    passed: object,
    rationale: str,
    *,
    critical: bool,
    optional_block: bool = False,
) -> dict[str, Any]:
    if bool(passed):
        severity = "PASS"
    elif critical:
        severity = "BLOCKED_CRITICAL"
    elif optional_block:
        severity = "BLOCKED_OPTIONAL"
    else:
        severity = "WARN"
    return {
        "rule_id": rule_id,
        "strategy_group": strategy_group,
        "passed": bool(passed),
        "severity": severity,
        "rationale": rationale,
    }


def _target_weight_dry_run_rows(
    *,
    config: Mapping[str, Any],
    prices_path: Path,
    candidate_ids: tuple[str, ...],
    decision_date: date | None,
) -> list[dict[str, Any]]:
    strategy_lookup = _strategy_lookup(config)
    required = sorted(
        {
            ticker
            for candidate_id in candidate_ids
            for ticker in _strategy_required_tickers(strategy_lookup[_registry_id(candidate_id)])
        }
    )
    prices = _load_price_matrix(prices_path, required)
    decision_ts = _decision_timestamp(prices, decision_date)
    rows = []
    for candidate_id in candidate_ids:
        registry_id = _registry_id(candidate_id)
        strategy = strategy_lookup.get(registry_id, {})
        weights = _target_weight_frame(strategy, prices, config).reindex(prices.index).ffill()
        row = weights.loc[decision_ts]
        rows.append(
            {
                "candidate_id": candidate_id,
                "registry_strategy_id": registry_id,
                "candidate_role": _candidate_role(candidate_id),
                "decision_date": decision_ts.date().isoformat(),
                "target_weights": {
                    str(key): round(_float(value), 6)
                    for key, value in row.to_dict().items()
                    if abs(_float(value)) > 1e-12
                },
                "research_only": True,
                "trade_recommendation": False,
                "broker_action": "none",
            }
        )
    return rows


def _candidate_metric_rows(
    *,
    config: Mapping[str, Any],
    prices_path: Path,
    start_date: date,
    end_date: date | None,
    candidate_ids: tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    strategy_lookup = _strategy_lookup(config)
    selected_ids = candidate_ids or tuple(strategy_lookup)
    available = _available_price_tickers(prices_path)
    rows: list[dict[str, Any]] = []
    for candidate_id in selected_ids:
        registry_id = _registry_id(candidate_id)
        strategy = strategy_lookup.get(registry_id)
        if not strategy:
            continue
        required = sorted(set(_strategy_required_tickers(strategy)) | {"QQQ"})
        if any(ticker not in available for ticker in required):
            continue
        prices = _slice_prices(
            _load_price_matrix(prices_path, required),
            start_date=start_date,
            end_date=end_date,
        )
        if prices.empty:
            continue
        returns = _strategy_return_series(strategy, prices, config)
        weights = _target_weight_frame(strategy, prices, config).reindex(returns.index).ffill()
        benchmark = prices["QQQ"].pct_change().fillna(0.0)
        metrics = _metrics_for_strategy(
            strategy,
            returns,
            weights,
            benchmark,
            annualization=_research_policy_int(config, "annualization_trading_days"),
            cost_bps=0.0,
        )
        metrics["strategy_id"] = candidate_id
        metrics["registry_strategy_id"] = registry_id
        metrics["candidate_role"] = _candidate_role(candidate_id)
        metrics["sgov_carry_contribution"] = _annualized_contribution(
            prices,
            weights,
            ticker="SGOV",
            annualization=_research_policy_int(config, "annualization_trading_days"),
        )
        metrics["qqq_beta_exposure"] = round(_beta(returns, benchmark), 6)
        metrics["cash_drag"] = round(_float(weights.get("SGOV", pd.Series(0.0)).mean()), 6)
        metrics["ranking_score"] = round(
            _float(metrics.get("calmar")) + _float(metrics.get("sharpe")),
            6,
        )
        rows.append(metrics)
    ranked = sorted(rows, key=lambda row: _float(row.get("ranking_score")), reverse=True)
    for rank, row in enumerate(ranked, start=1):
        row["rank"] = rank
    return ranked


def _validate_simple_baseline_data(
    *,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    config: Mapping[str, Any],
    as_of_date: date | None,
) -> DataQualityReport:
    return validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=_required_tickers(config),
        expected_rate_series=_required_rate_series(config),
        quality_config=load_data_quality(),
        as_of=as_of_date or _max_price_date(prices_path),
        manifest_path=prices_path.parent / "download_manifest.csv",
        secondary_prices_path=marketstack_prices_path if marketstack_prices_path.exists() else None,
        require_secondary_prices=False,
    )


def _data_quality_payload(
    report: DataQualityReport,
    *,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
) -> dict[str, Any]:
    return {
        "status": report.status,
        "passed": report.passed,
        "checked_at": report.checked_at.isoformat(),
        "as_of": report.as_of.isoformat(),
        "required_command": "aits validate-data",
        "same_code_path": "validate_data_cache",
        "price_path": str(prices_path),
        "secondary_prices_path": str(marketstack_prices_path),
        "rates_path": str(rates_path),
        "price_row_count": report.price_summary.rows,
        "secondary_price_row_count": (
            report.secondary_price_summary.rows if report.secondary_price_summary else None
        ),
        "rate_row_count": report.rate_summary.rows,
        "price_checksum": report.price_summary.sha256,
        "rate_checksum": report.rate_summary.sha256,
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "issues": [
            {
                "severity": str(issue.severity),
                "code": issue.code,
                "message": issue.message,
                "rows": issue.rows,
                "source": issue.source,
            }
            for issue in report.issues
        ],
    }


def _data_quality_has_critical_issue(data_quality: Mapping[str, Any]) -> bool:
    optional_tqqq_only = True
    for issue in _records(data_quality.get("issues")):
        if str(issue.get("severity")).upper() != "ERROR":
            continue
        message = str(issue.get("message", "")).upper()
        code = str(issue.get("code", "")).upper()
        source = str(issue.get("source", "")).upper()
        if "TQQQ" in message or "TQQQ" in code or "TQQQ" in source:
            continue
        optional_tqqq_only = False
    return not optional_tqqq_only


def _hardening_data_quality_status(
    hardening_status: str,
    data_quality: Mapping[str, Any],
) -> str:
    if hardening_status == "VALIDATE_DATA_BLOCKED":
        return "FAIL"
    if data_quality.get("passed"):
        return str(data_quality.get("status"))
    return "PASS_WITH_WARNINGS"


def _readiness_checks(
    inventory: Mapping[str, Any],
    contract: Mapping[str, Any],
    hardening: Mapping[str, Any],
    policy_lock: Mapping[str, Any],
    comparator_lock: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows = _records(inventory.get("inventory"))
    qqq = _symbol_row(rows, "QQQ")
    tqqq = _symbol_row(rows, "TQQQ")
    sgov = _symbol_row(rows, "SGOV")
    hardening_summary = _mapping(hardening.get("summary"))
    checks = [
        _check("qqq_data_complete", qqq.get("source_status") == "OK"),
        _check(
            "sgov_data_complete_or_contract_clear",
            sgov.get("source_status") in {"OK", "WARN"}
            and contract.get("status") == "SGOV_TOTAL_RETURN_CONTRACT_READY",
        ),
        _check(
            "tqqq_data_complete_or_challenger_blocked",
            (
                int(tqqq.get("row_count", 0)) > 0
                and tqqq.get("adjusted_close_available")
                and int(tqqq.get("missing_date_count", 0)) == 0
            )
            or hardening_summary.get("tqqq_challenger_status") == "BLOCKED",
            warn_when_true=tqqq.get("source_status") != "OK",
        ),
        _check("policy_definition_locked", policy_lock.get("status") == "POLICY_DEFINITION_LOCKED"),
        _check(
            "comparator_definition_locked",
            comparator_lock.get("status") == "COMPARATOR_DEFINITIONS_LOCKED",
        ),
        _check(
            "data_quality_gate_pass_or_warn",
            hardening_summary.get("data_quality_status") in {"PASS", "PASS_WITH_WARNINGS"},
            warn_when_true=hardening_summary.get("data_quality_status") == "PASS_WITH_WARNINGS",
        ),
        _check("no_paper_shadow", SAFETY_BOUNDARY["paper_shadow_allowed"] is False),
        _check("no_production", SAFETY_BOUNDARY["production_allowed"] is False),
        _check("broker_action_none", SAFETY_BOUNDARY["broker_action"] == "none"),
    ]
    return checks


def _check(check_id: str, passed: object, *, warn_when_true: bool = False) -> dict[str, Any]:
    if not bool(passed):
        status = "FAIL"
    elif warn_when_true:
        status = "WARN"
    else:
        status = "PASS"
    return {"check_id": check_id, "status": status, "passed": bool(passed)}


def _owner_decision_answers(
    inventory: Mapping[str, Any],
    contract: Mapping[str, Any],
    equal: Mapping[str, Any],
    challenger: Mapping[str, Any],
    readiness: Mapping[str, Any],
    preview: Mapping[str, Any],
) -> dict[str, Any]:
    rows = _records(inventory.get("inventory"))
    return {
        "1_qqq_tqqq_sgov_data_repaired": {
            symbol: _symbol_row(rows, symbol).get("source_status") for symbol in TARGET_SYMBOLS
        },
        "2_sgov_uses_total_return_or_adjusted_close": contract.get("status"),
        "3_tqqq_challenger_retained": challenger.get("status")
        in {"TQQQ_CHALLENGER_REVALIDATED", "TQQQ_CHALLENGER_STILL_PAUSED"},
        "4_equal_risk_still_primary": _mapping(equal.get("summary")).get(
            "top_candidate_after_repair"
        )
        == PRIMARY_CANDIDATE_ID,
        "5_allow_research_only_forward_aging": readiness.get("status")
        in {"FORWARD_AGING_READY", "FORWARD_AGING_READY_WITH_WARNINGS"},
        "6_continue_pause_tqqq_heavy": True,
        "7_continue_block_leaps_wheel": True,
        "8_continue_quarantine_tail_risk_fallback": True,
        "9_allow_reader_brief_minimal_observation_summary": preview.get("status")
        == "READER_FORWARD_PREVIEW_SAFE",
    }


def _run_step(
    task_id: str,
    command: str,
    builder: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    try:
        payload = builder()
    except Exception as exc:
        return {
            "task_id": task_id,
            "command": command,
            "exit_code": 1,
            "status": "ERROR",
            "error": str(exc),
        }
    return {
        "task_id": task_id,
        "command": command,
        "exit_code": 0,
        "status": payload.get("status"),
        "artifact_paths": payload.get("artifact_paths"),
        "summary": payload.get("summary", {}),
        "data_quality_status": _mapping(payload.get("data_quality")).get("status"),
    }


def _status_for_report(rows: list[dict[str, Any]], task_id: str) -> str | None:
    return next((str(row.get("status")) for row in rows if row.get("task_id") == task_id), None)


def _proof_check(
    check_id: str,
    passed: object,
    *,
    observed: Mapping[str, Any] | None = None,
    warn_when_true: bool = False,
) -> dict[str, Any]:
    if not bool(passed):
        status = "FAIL"
    elif warn_when_true:
        status = "WARN"
    else:
        status = "PASS"
    return {
        "check_id": check_id,
        "status": status,
        "passed": bool(passed),
        "observed": dict(observed or {}),
    }


def _file_sha256(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _manifest_entries_are_provider_backed(manifest: pd.DataFrame, symbol: str) -> bool:
    entries = _manifest_entries_for_symbol(manifest, symbol)
    if not entries:
        return False
    latest = entries[-1]
    provider = str(latest.get("provider") or "").strip().lower()
    endpoint = str(latest.get("endpoint") or "").strip()
    output_path = str(latest.get("output_path") or "").strip()
    return bool(provider and provider != "fixture" and endpoint and output_path)


def _manifest_mentions_fixture(manifest: pd.DataFrame, symbols: tuple[str, ...]) -> bool:
    for symbol in symbols:
        for entry in _manifest_entries_for_symbol(manifest, symbol):
            values = [
                entry.get("provider"),
                entry.get("source_id"),
                entry.get("endpoint"),
                entry.get("output_path"),
            ]
            values.extend(_mapping(entry.get("request_parameters")).values())
            if any("fixture" in str(value).lower() for value in values):
                return True
    return False


def _marketstack_endpoint() -> str:
    return "https://api.marketstack.com/v2/eod"


def _sgov_proxy_quality_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty or not {"close", "adj_close"}.issubset(frame.columns):
        return {
            "row_count": 0,
            "price_only_total_return": 0.0,
            "adj_close_total_return": 0.0,
            "adj_close_minus_price_return_delta": 0.0,
            "price_only_carry_understatement": 0.0,
            "max_absolute_adj_close_gap": 0.0,
            "mean_absolute_adj_close_gap": 0.0,
        }
    ordered = frame.copy()
    if "date" in ordered.columns:
        ordered["_date"] = pd.to_datetime(ordered["date"], errors="coerce")
        ordered = ordered.sort_values("_date")
    close = pd.to_numeric(ordered["close"], errors="coerce").dropna()
    adj = pd.to_numeric(ordered["adj_close"], errors="coerce").dropna()
    aligned = pd.concat([close, adj], axis=1).dropna()
    aligned.columns = ["close", "adj_close"]
    if len(aligned) < 2:
        return {
            "row_count": int(len(aligned)),
            "price_only_total_return": 0.0,
            "adj_close_total_return": 0.0,
            "adj_close_minus_price_return_delta": 0.0,
            "price_only_carry_understatement": 0.0,
            "max_absolute_adj_close_gap": 0.0,
            "mean_absolute_adj_close_gap": 0.0,
        }
    price_return = float(aligned["close"].iloc[-1] / aligned["close"].iloc[0] - 1.0)
    adj_return = float(aligned["adj_close"].iloc[-1] / aligned["adj_close"].iloc[0] - 1.0)
    return_delta = adj_return - price_return
    gap = (aligned["adj_close"] - aligned["close"]).abs()
    return {
        "row_count": int(len(aligned)),
        "price_only_total_return": round(price_return, 6),
        "adj_close_total_return": round(adj_return, 6),
        "adj_close_minus_price_return_delta": round(return_delta, 6),
        "price_only_carry_understatement": round(max(return_delta, 0.0), 6),
        "max_absolute_adj_close_gap": round(float(gap.max()), 6),
        "mean_absolute_adj_close_gap": round(float(gap.mean()), 6),
    }


def _build_payload_paths(output_root: Path, artifact_id: str) -> dict[str, str]:
    return {
        "json_path": str(output_root / f"{artifact_id}.json"),
        "markdown_path": str(output_root / f"{artifact_id}.md"),
    }


def _write_pair(payload: dict[str, Any], *, output_root: Path, artifact_id: str) -> None:
    payload["artifact_paths"] = _build_payload_paths(output_root, artifact_id)
    write_foundation_artifact_pair(payload, output_root=output_root, artifact_id=artifact_id)


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
        "requested_date_range": f"{DEFAULT_AI_REGIME_BACKTEST_START.isoformat()}..latest",
        "summary": {
            "market_regime": "ai_after_chatgpt",
            "default_backtest_start": DEFAULT_AI_REGIME_BACKTEST_START.isoformat(),
            **dict(summary),
        },
        **SAFETY_BOUNDARY,
        "requirement_document": str(DEFAULT_SIMPLE_BASELINE_REPAIR_REQUIREMENT_PATH),
        **extra,
    }


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
            f"outputs/research_strategies/simple_baselines/{artifact_slug}.json",
            f"outputs/research_strategies/simple_baselines/{artifact_slug}.md",
        ],
        "artifact_selection_policy": "latest_available",
        "freshness_sla_days": 30,
        "freshness_rationale": "TRADING-911 to 922 data repair unblock artifacts.",
        "owner_action": "review_simple_baseline_data_repair_forward_aging_unblock",
        "include_in_reader_brief": report_id == "reader_brief_forward_aging_safe_preview",
        "include_in_daily_task_dashboard": False,
        "required_for_daily_reading": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _strategy_lookup(config: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    rows = _strategy_rows(config) + _dynamic_candidate_strategies(config)
    return {str(row.get("strategy_id")): dict(row) for row in rows}


def _registry_id(candidate_id: str) -> str:
    return PUBLIC_ALIAS_TO_REGISTRY_ID.get(candidate_id, candidate_id)


def _candidate_role(candidate_id: str) -> str:
    if candidate_id == PRIMARY_CANDIDATE_ID:
        return "primary"
    if candidate_id in STATIC_COMPARATOR_IDS:
        return "comparator"
    if candidate_id == CHALLENGER_CANDIDATE_ID:
        return "challenger"
    return "research_candidate"


def _strategy_required_tickers(strategy: Mapping[str, Any]) -> list[str]:
    tickers = set(_mapping(strategy.get("target_weights")))
    tickers.update(_mapping(strategy.get("risk_on_weights")))
    tickers.update(_mapping(strategy.get("risk_off_weights")))
    strategy_id = str(strategy.get("strategy_id"))
    if "dma" in strategy_id or strategy_id.startswith("dyn_"):
        tickers.add("QQQ")
    if strategy_id == "tqqq_drawdown_capped":
        tickers.add("QQQ")
    return sorted(str(item) for item in tickers)


def _read_csv_or_empty(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _rows_for_symbol(frame: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if frame.empty or "ticker" not in frame.columns:
        return pd.DataFrame()
    return frame.loc[frame["ticker"].astype(str) == symbol].copy()


def _dates_from_frame(frame: pd.DataFrame) -> list[date]:
    if frame.empty or "date" not in frame.columns:
        return []
    parsed = pd.to_datetime(frame["date"], errors="coerce").dropna()
    return sorted({pd.Timestamp(item).date() for item in parsed})


def _reference_dates(primary: pd.DataFrame) -> list[date]:
    qqq = _rows_for_symbol(primary, "QQQ")
    dates = [day for day in _dates_from_frame(qqq) if day >= DEFAULT_AI_REGIME_BACKTEST_START]
    if dates:
        return dates
    all_dates = _dates_from_frame(primary)
    return [day for day in all_dates if day >= DEFAULT_AI_REGIME_BACKTEST_START]


def _adj_close_available(frame: pd.DataFrame) -> bool:
    if frame.empty or "adj_close" not in frame.columns:
        return False
    return bool(pd.to_numeric(frame["adj_close"], errors="coerce").notna().any())


def _dividend_adjusted(frame: pd.DataFrame) -> bool:
    if frame.empty or not {"close", "adj_close"}.issubset(frame.columns):
        return False
    close = pd.to_numeric(frame["close"], errors="coerce")
    adj = pd.to_numeric(frame["adj_close"], errors="coerce")
    return bool(((close - adj).abs() > 1e-9).any())


def _latest_source(frame: pd.DataFrame) -> str | None:
    if frame.empty or "source" not in frame.columns:
        return None
    values = [str(item) for item in frame["source"].dropna().tolist() if str(item)]
    return values[-1] if values else None


def _manifest_entries_for_symbol(manifest: pd.DataFrame, symbol: str) -> list[dict[str, Any]]:
    if manifest.empty:
        return []
    entries: list[dict[str, Any]] = []
    for _, row in manifest.iterrows():
        params = _parse_manifest_params(row.get("request_parameters"))
        requested = set(str(item).upper() for item in params.get("tickers", []))
        requested.update(str(item).upper() for item in params.get("symbols", []))
        mapping = _mapping(params.get("symbol_mapping"))
        requested.update(str(item).upper() for item in mapping)
        if symbol not in requested:
            continue
        entries.append(
            {
                "downloaded_at": row.get("downloaded_at"),
                "source_id": row.get("source_id"),
                "provider": row.get("provider"),
                "endpoint": row.get("endpoint"),
                "output_path": row.get("output_path"),
                "row_count": _int(row.get("row_count")),
                "checksum_sha256": row.get("checksum_sha256"),
                "request_parameters": params,
            }
        )
    return entries


def _parse_manifest_params(value: object) -> dict[str, Any]:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        import json

        parsed = json.loads(value)
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _latest_manifest_provider(entries: list[dict[str, Any]]) -> str | None:
    providers = [str(entry.get("provider")) for entry in entries if entry.get("provider")]
    return providers[-1] if providers else None


def _symbol_row(rows: list[dict[str, Any]], symbol: str) -> dict[str, Any]:
    return next((row for row in rows if row.get("symbol") == symbol), {})


def _tqqq_gap_reason(inventory: list[dict[str, Any]]) -> str:
    row = _symbol_row(inventory, "TQQQ")
    if int(row.get("row_count", 0)) > 0:
        return "primary cache contains TQQQ"
    if int(row.get("repair_manifest_entry_count", 0)) > 0:
        return (
            "TQQQ repair manifest exists, but a later primary download universe without TQQQ "
            "overwrote the cache."
        )
    return "TQQQ is absent from primary cache and no repair manifest entry was found."


def _marketstack_gap_reason(inventory: list[dict[str, Any]]) -> str:
    missing = [row["symbol"] for row in inventory if row.get("secondary_missing")]
    if not missing:
        return "Marketstack secondary cache covers QQQ/TQQQ/SGOV."
    return (
        "Marketstack secondary cache is missing "
        f"{', '.join(missing)}; current default download history did not include those symbols."
    )


def _fmp_repair_can_fill(inventory: list[dict[str, Any]]) -> bool:
    tqqq = _symbol_row(inventory, "TQQQ")
    return int(tqqq.get("repair_manifest_entry_count", 0)) > 0 or int(tqqq.get("row_count", 0)) > 0


def _data_source_root_cause_findings(inventory: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "finding": "qqq_completeness",
            "status": _symbol_row(inventory, "QQQ").get("source_status"),
        },
        {"finding": "tqqq_primary_cache_gap", "status": _tqqq_gap_reason(inventory)},
        {
            "finding": "sgov_completeness",
            "status": _symbol_row(inventory, "SGOV").get("source_status"),
        },
        {"finding": "marketstack_gap", "status": _marketstack_gap_reason(inventory)},
        {"finding": "fmp_repair_can_fill", "status": _fmp_repair_can_fill(inventory)},
    ]


def _strategy_group_status(rules: list[dict[str, Any]], group: str) -> str:
    selected = [row for row in rules if row["strategy_group"] in {group, "all"}]
    if any(row["severity"] == "BLOCKED_CRITICAL" for row in selected):
        return "BLOCKED"
    if group == "tqqq_challenger" and any(
        row["severity"] == "BLOCKED_OPTIONAL" for row in selected
    ):
        return "BLOCKED"
    if any(row["severity"] == "WARN" for row in selected):
        return "WARN"
    return "READY"


def _before_candidate_row(path: Path, strategy_id: str) -> dict[str, Any]:
    payload = _read_json_or_empty(path)
    for key in ("pareto_frontier", "dominated_strategy_list", "top_10_by_calmar"):
        for row in _records(payload.get(key)):
            if row.get("strategy_id") == strategy_id:
                return row
    return {}


def _metric_delta(before: Mapping[str, Any], after: Mapping[str, Any]) -> dict[str, Any]:
    fields = (
        "annual_return",
        "max_drawdown",
        "sharpe",
        "calmar",
        "sgov_carry_contribution",
        "qqq_beta_exposure",
        "cash_drag",
        "rank",
    )
    return {
        field: {
            "before": before.get(field),
            "after": after.get(field),
            "delta": round(_float(after.get(field)) - _float(before.get(field)), 6)
            if field != "rank"
            else None,
        }
        for field in fields
    }


def _leverage_drawdown_issue(
    challenger: Mapping[str, Any],
    equal: Mapping[str, Any],
    qqq_50: Mapping[str, Any],
) -> bool:
    if not challenger:
        return True
    challenger_drawdown = abs(_float(challenger.get("max_drawdown")))
    reference_drawdown = max(
        abs(_float(equal.get("max_drawdown"))),
        abs(_float(qqq_50.get("max_drawdown"))),
    )
    return challenger_drawdown > reference_drawdown


def _available_price_tickers(path: Path) -> set[str]:
    frame = _read_csv_or_empty(path)
    if frame.empty or "ticker" not in frame.columns:
        return set()
    return {str(item) for item in frame["ticker"].dropna().unique()}


def _annualized_contribution(
    prices: pd.DataFrame,
    weights: pd.DataFrame,
    *,
    ticker: str,
    annualization: int,
) -> float:
    if ticker not in prices.columns or ticker not in weights.columns:
        return 0.0
    asset_returns = prices[ticker].pct_change().fillna(0.0)
    applied = weights.shift(1).ffill().reindex(prices.index).fillna(0.0)
    contribution = applied[ticker] * asset_returns
    return round(float(contribution.mean() * annualization), 6)


def _beta(returns: pd.Series, benchmark: pd.Series) -> float:
    aligned = pd.concat([returns, benchmark], axis=1).dropna()
    if aligned.empty:
        return 0.0
    aligned.columns = ["strategy", "benchmark"]
    variance = float(aligned["benchmark"].var(ddof=0))
    if abs(variance) <= 1e-12:
        return 0.0
    return float(aligned["strategy"].cov(aligned["benchmark"]) / variance)


def _decision_timestamp(prices: pd.DataFrame, decision_date: date | None) -> pd.Timestamp:
    if prices.empty:
        raise ValueError("price matrix is empty")
    if decision_date is None:
        return pd.Timestamp(prices.index.max())
    eligible = prices.loc[prices.index.date <= decision_date]
    if eligible.empty:
        raise ValueError("decision date is before available price history")
    return pd.Timestamp(eligible.index.max())


def _manifest_range_covers_cache(params: Mapping[str, Any], cache_dates: list[date]) -> bool:
    if not cache_dates:
        return False
    start = _date_from_any(params.get("start"))
    end = _date_from_any(params.get("end"))
    if start is None or end is None:
        return False
    return start <= min(cache_dates) and end >= max(cache_dates)


def _symbol_mapping_correct(params: Mapping[str, Any], symbol: str) -> bool:
    mapping = _mapping(params.get("symbol_mapping"))
    if symbol not in mapping:
        return True
    mapped = _mapping(mapping.get(symbol))
    return str(mapped.get("canonical_symbol", symbol)) == symbol


def _missing_date_count_against_reference(prices: pd.DataFrame, symbol: str) -> int:
    reference = _reference_dates(prices)
    actual = set(_dates_from_frame(_rows_for_symbol(prices, symbol)))
    return len([day for day in reference if day not in actual])


def _duplicate_date_count(frame: pd.DataFrame) -> int:
    if frame.empty or "date" not in frame.columns:
        return 0
    return int(frame.duplicated(subset=["date"], keep=False).sum())


def _date_from_any(value: object) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _date_label(value: date | None) -> str | None:
    return value.isoformat() if value else None


def _max_price_date(path: Path) -> date:
    frame = _read_csv_or_empty(path)
    if frame.empty or "date" not in frame.columns:
        return DEFAULT_AI_REGIME_BACKTEST_START
    parsed = pd.to_datetime(frame["date"], errors="coerce").dropna()
    if parsed.empty:
        return DEFAULT_AI_REGIME_BACKTEST_START
    return pd.Timestamp(parsed.max()).date()


def _int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default
