from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.research_governance import ResearchGovernanceError
from ai_trading_system.simple_baseline_candidate_validation import (
    DEFAULT_SIMPLE_BASELINE_WATCHLIST_OWNER_DECISION_DOC_PATH,
    run_dynamic_vs_static_edge_significance_review,
    run_equal_risk_qqq_sgov_deep_dive,
    run_simple_baseline_drawdown_episode_review,
    run_simple_baseline_period_split_validation,
    run_simple_baseline_watchlist_owner_decision,
    run_tqqq_heavy_pause_rationale_report,
)
from ai_trading_system.simple_baseline_data_repair import (
    DEFAULT_DOWNLOAD_MANIFEST_PATH,
    run_data_repair_owner_decision_pack,
    run_equal_risk_result_recompute_after_data_repair,
    run_first_forward_aging_observation_dry_run,
    run_forward_aging_unblock_readiness_review,
    run_market_data_repair_manifest_audit,
    run_reader_brief_forward_aging_safe_preview,
    run_sgov_total_return_data_contract,
    run_simple_baseline_data_source_inventory,
    run_simple_baseline_post_data_repair_real_run,
    run_simple_baseline_validate_data_hardening,
    run_tqqq_cache_rebuild_validation,
    run_tqqq_challenger_revalidation_after_cache_fix,
)
from ai_trading_system.simple_baseline_forward_aging import (
    DEFAULT_FORWARD_AGING_MASTER_REVIEW_DOC_PATH,
    DEFAULT_FORWARD_AGING_OWNER_REVIEW_DOC_PATH,
    run_daily_reader_forward_aging_summary,
    run_equal_risk_qqq_sgov_policy_definition_lock,
    run_simple_baseline_absolute_return_gap_review,
    run_simple_baseline_candidate_role_assignment,
    run_simple_baseline_comparator_definition_lock,
    run_simple_baseline_forward_aging_automation_readiness,
    run_simple_baseline_forward_aging_candidate_freeze,
    run_simple_baseline_forward_aging_contract,
    run_simple_baseline_forward_aging_data_quality_gate,
    run_simple_baseline_forward_aging_master_review,
    run_simple_baseline_forward_aging_owner_review_pack,
    run_simple_baseline_forward_aging_scoreboard,
    run_simple_baseline_forward_aging_update_maturity,
    run_simple_baseline_forward_aging_write_observation,
    run_simple_baseline_paper_shadow_threshold_contract,
    run_simple_baseline_real_result_reconciliation,
    run_simple_baseline_risk_budget_review,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_MARKETSTACK_PRICES_PATH as DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_PRICES_PATH as DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_RATES_PATH as DEFAULT_SIMPLE_BASELINE_RATES_PATH,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_SIMPLE_BASELINE_MASTER_REVIEW_DOC_PATH,
    DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
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

console = Console()


def register_simple_baseline_strategy_commands(strategies_app: typer.Typer) -> None:
    for command_name, command in _SIMPLE_BASELINE_STRATEGY_COMMANDS:
        strategies_app.command(command_name)(command)


def strategies_simple_baseline_registry_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_registry_review(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline registry review", payload)


def strategies_qqq_sgov_baseline_backtest_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="回测开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选回测结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_qqq_sgov_baseline_backtest(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("QQQ / SGOV baseline backtest", payload)


def strategies_tqqq_sgov_risk_controlled_baseline_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="回测开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选回测结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tqqq_sgov_risk_controlled_baseline(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("TQQQ / SGOV risk-controlled baseline", payload)


def strategies_trend_vol_allocation_policy_search_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="回测开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选回测结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_trend_vol_allocation_policy_search(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Trend / volatility allocation policy search", payload)


def strategies_simple_baseline_dominance_ranking_command(
    qqq_sgov: Annotated[
        Path | None,
        typer.Option("--qqq-sgov", help="QQQ / SGOV baseline JSON。"),
    ] = None,
    tqqq_sgov: Annotated[
        Path | None,
        typer.Option("--tqqq-sgov", help="TQQQ / SGOV baseline JSON。"),
    ] = None,
    policy_search: Annotated[
        Path | None,
        typer.Option("--policy-search", help="Trend-vol policy search JSON。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_dominance_ranking(
            qqq_sgov_path=qqq_sgov,
            tqqq_sgov_path=tqqq_sgov,
            policy_search_path=policy_search,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline dominance ranking", payload)


def strategies_simple_baseline_pit_boundary_audit_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_pit_boundary_audit(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline PIT boundary audit", payload)


def strategies_simple_baseline_cost_sensitivity_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="回测开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选回测结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_cost_sensitivity(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Simple baseline cost sensitivity", payload)


def strategies_simple_baseline_regime_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="回测开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选回测结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_regime_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Simple baseline regime review", payload)


def strategies_simple_baseline_forward_aging_tracker_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="回测开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选回测结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_forward_aging_tracker(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Simple baseline forward aging tracker", payload)


def strategies_simple_baseline_paper_shadow_readiness_command(
    pit_audit: Annotated[
        Path | None,
        typer.Option("--pit-audit", help="PIT boundary audit JSON。"),
    ] = None,
    ranking: Annotated[
        Path | None,
        typer.Option("--ranking", help="Dominance ranking JSON。"),
    ] = None,
    regime_review: Annotated[
        Path | None,
        typer.Option("--regime-review", help="Regime review JSON。"),
    ] = None,
    cost_sensitivity: Annotated[
        Path | None,
        typer.Option("--cost-sensitivity", help="Cost sensitivity JSON。"),
    ] = None,
    forward_aging: Annotated[
        Path | None,
        typer.Option("--forward-aging", help="Forward aging JSON。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_paper_shadow_readiness(
            pit_audit_path=pit_audit,
            ranking_path=ranking,
            regime_review_path=regime_review,
            cost_sensitivity_path=cost_sensitivity,
            forward_aging_path=forward_aging,
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline paper-shadow readiness", payload)


def strategies_daily_reader_portfolio_control_safety_summary_command(
    ranking: Annotated[
        Path | None,
        typer.Option("--ranking", help="Dominance ranking JSON。"),
    ] = None,
    readiness: Annotated[
        Path | None,
        typer.Option("--readiness", help="Paper-shadow readiness JSON。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_daily_reader_safety_summary(
            ranking_path=ranking,
            readiness_path=readiness,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Daily reader portfolio control safety summary", payload)


def strategies_simple_baseline_portfolio_dry_run_mapper_command(
    strategy_id: Annotated[
        str,
        typer.Option("--strategy-id", help="Simple baseline strategy id。"),
    ] = "qqq_80_sgov_20",
    hypothetical_portfolio_value: Annotated[
        float | None,
        typer.Option("--hypothetical-portfolio-value", help="假设组合金额。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_portfolio_dry_run_mapper(
            strategy_id=strategy_id,
            hypothetical_portfolio_value=hypothetical_portfolio_value,
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline portfolio dry-run mapper", payload)


def strategies_simple_baseline_master_review_command(
    ranking: Annotated[
        Path | None,
        typer.Option("--ranking", help="Dominance ranking JSON。"),
    ] = None,
    cost_sensitivity: Annotated[
        Path | None,
        typer.Option("--cost-sensitivity", help="Cost sensitivity JSON。"),
    ] = None,
    regime_review: Annotated[
        Path | None,
        typer.Option("--regime-review", help="Regime review JSON。"),
    ] = None,
    forward_aging: Annotated[
        Path | None,
        typer.Option("--forward-aging", help="Forward aging JSON。"),
    ] = None,
    readiness: Annotated[
        Path | None,
        typer.Option("--readiness", help="Paper-shadow readiness JSON。"),
    ] = None,
    docs_path: Annotated[
        Path,
        typer.Option("--docs-path", help="Master review Markdown path。"),
    ] = DEFAULT_SIMPLE_BASELINE_MASTER_REVIEW_DOC_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_master_review(
            ranking_path=ranking,
            cost_sensitivity_path=cost_sensitivity,
            regime_review_path=regime_review,
            forward_aging_path=forward_aging,
            readiness_path=readiness,
            output_root=output_root,
            master_doc_path=docs_path,
        )
    )
    _print_simple_baseline_payload("Simple baseline master review", payload)


def strategies_options_next_stage_gate_command(
    master_review: Annotated[
        Path | None,
        typer.Option("--master-review", help="Simple baseline master review JSON。"),
    ] = None,
    forward_aging: Annotated[
        Path | None,
        typer.Option("--forward-aging", help="Forward aging JSON。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_options_next_stage_gate(
            master_review_path=master_review,
            forward_aging_path=forward_aging,
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Options next-stage gate", payload)


def strategies_equal_risk_qqq_sgov_deep_dive_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="分析开始日期；默认 2022-12-01。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选分析结束日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_equal_risk_qqq_sgov_deep_dive(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_simple_baseline_payload("Equal-risk QQQ / SGOV deep dive", payload)


def strategies_simple_baseline_period_split_validation_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_period_split_validation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Simple baseline period split validation", payload)


def strategies_simple_baseline_drawdown_episode_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_drawdown_episode_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Simple baseline drawdown episode review", payload)


def strategies_dynamic_vs_static_edge_significance_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_dynamic_vs_static_edge_significance_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Dynamic vs static edge significance review", payload)


def strategies_tqqq_heavy_pause_rationale_report_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tqqq_heavy_pause_rationale_report(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("TQQQ-heavy pause rationale report", payload)


def strategies_simple_baseline_watchlist_owner_decision_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    docs_path: Annotated[
        Path,
        typer.Option("--docs-path", help="Owner decision Markdown path。"),
    ] = DEFAULT_SIMPLE_BASELINE_WATCHLIST_OWNER_DECISION_DOC_PATH,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_watchlist_owner_decision(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            docs_path=docs_path,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Simple baseline watchlist owner decision", payload)


def strategies_simple_baseline_real_result_reconciliation_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_real_result_reconciliation(output_root=output_root)
    )
    _print_simple_baseline_payload("Simple baseline real result reconciliation", payload)


def strategies_simple_baseline_forward_aging_candidate_freeze_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_forward_aging_candidate_freeze(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline forward aging candidate freeze", payload)


def strategies_simple_baseline_forward_aging_contract_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_forward_aging_contract(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline forward aging contract", payload)


def strategies_simple_baseline_forward_aging_write_observation_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    decision_date: Annotated[
        str | None,
        typer.Option("--decision-date", help="可选 observation decision date。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_forward_aging_write_observation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            decision_date=_parse_optional_date(decision_date),
        )
    )
    _print_simple_baseline_payload("Simple baseline forward aging observation", payload)


def strategies_simple_baseline_forward_aging_update_maturity_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_forward_aging_update_maturity(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Simple baseline forward aging maturity update", payload)


def strategies_simple_baseline_forward_aging_scoreboard_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_forward_aging_scoreboard(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline forward aging scoreboard", payload)


def strategies_equal_risk_qqq_sgov_policy_definition_lock_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_equal_risk_qqq_sgov_policy_definition_lock(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Equal-risk policy definition lock", payload)


def strategies_simple_baseline_comparator_definition_lock_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_comparator_definition_lock(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline comparator definition lock", payload)


def strategies_simple_baseline_forward_aging_data_quality_gate_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_forward_aging_data_quality_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Simple baseline forward aging data quality gate", payload)


def strategies_simple_baseline_paper_shadow_threshold_contract_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_paper_shadow_threshold_contract(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline paper-shadow threshold contract", payload)


def strategies_daily_reader_forward_aging_summary_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_daily_reader_forward_aging_summary(output_root=output_root)
    )
    _print_simple_baseline_payload("Daily reader forward aging summary", payload)


def strategies_simple_baseline_risk_budget_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_risk_budget_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Simple baseline risk budget review", payload)


def strategies_simple_baseline_absolute_return_gap_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_absolute_return_gap_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Simple baseline absolute return gap review", payload)


def strategies_simple_baseline_candidate_role_assignment_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_candidate_role_assignment(
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline candidate role assignment", payload)


def strategies_simple_baseline_forward_aging_owner_review_pack_command(
    docs_path: Annotated[
        Path,
        typer.Option("--docs-path", help="Owner review Markdown path。"),
    ] = DEFAULT_FORWARD_AGING_OWNER_REVIEW_DOC_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_forward_aging_owner_review_pack(
            output_root=output_root,
            docs_path=docs_path,
        )
    )
    _print_simple_baseline_payload("Simple baseline forward aging owner review pack", payload)


def strategies_simple_baseline_forward_aging_automation_readiness_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_forward_aging_automation_readiness(output_root=output_root)
    )
    _print_simple_baseline_payload("Simple baseline forward aging automation readiness", payload)


def strategies_simple_baseline_forward_aging_master_review_command(
    docs_path: Annotated[
        Path,
        typer.Option("--docs-path", help="Master review Markdown path。"),
    ] = DEFAULT_FORWARD_AGING_MASTER_REVIEW_DOC_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_forward_aging_master_review(
            output_root=output_root,
            docs_path=docs_path,
        )
    )
    _print_simple_baseline_payload("Simple baseline forward aging master review", payload)


def strategies_simple_baseline_data_source_inventory_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option("--manifest-path", help="下载/repair manifest CSV。"),
    ] = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_data_source_inventory(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            manifest_path=manifest_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Simple baseline data source inventory", payload)


def strategies_tqqq_cache_rebuild_and_validation_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option("--manifest-path", help="下载/repair manifest CSV。"),
    ] = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    execute_repair: Annotated[
        bool,
        typer.Option(
            "--execute-repair/--no-execute-repair",
            help="是否调用既有 FMP repair 路径补 TQQQ。",
        ),
    ] = True,
) -> None:
    payload = _build_research_payload(
        lambda: run_tqqq_cache_rebuild_validation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            manifest_path=manifest_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            execute_repair=execute_repair,
        )
    )
    _print_simple_baseline_payload("TQQQ cache rebuild validation", payload)


def strategies_sgov_total_return_data_contract_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option("--manifest-path", help="下载/repair manifest CSV。"),
    ] = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_sgov_total_return_data_contract(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            manifest_path=manifest_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("SGOV total-return data contract", payload)


def strategies_market_data_repair_manifest_audit_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option("--manifest-path", help="下载/repair manifest CSV。"),
    ] = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_market_data_repair_manifest_audit(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            manifest_path=manifest_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Market data repair manifest audit", payload)


def strategies_simple_baseline_validate_data_hardening_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option("--manifest-path", help="下载/repair manifest CSV。"),
    ] = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_validate_data_hardening(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            manifest_path=manifest_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Simple baseline validate-data hardening", payload)


def strategies_rerun_simple_baseline_real_cli_after_data_repair_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option("--manifest-path", help="下载/repair manifest CSV。"),
    ] = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_simple_baseline_post_data_repair_real_run(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            manifest_path=manifest_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Simple baseline post-data-repair real run", payload)


def strategies_equal_risk_result_recompute_after_data_repair_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_equal_risk_result_recompute_after_data_repair(
            prices_path=prices_path,
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("Equal-risk result recompute after data repair", payload)


def strategies_tqqq_challenger_revalidation_after_cache_fix_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_tqqq_challenger_revalidation_after_cache_fix(
            prices_path=prices_path,
            config_path=config_path,
            output_root=output_root,
        )
    )
    _print_simple_baseline_payload("TQQQ challenger revalidation", payload)


def strategies_forward_aging_unblock_readiness_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option("--manifest-path", help="下载/repair manifest CSV。"),
    ] = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_forward_aging_unblock_readiness_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            manifest_path=manifest_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_simple_baseline_payload("Forward-aging unblock readiness review", payload)


def strategies_first_forward_aging_observation_dry_run_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="主价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="第二行情源价格缓存 CSV。"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="rates cache for validate-data gate。"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    manifest_path: Annotated[
        Path,
        typer.Option("--manifest-path", help="下载/repair manifest CSV。"),
    ] = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    decision_date: Annotated[
        str | None,
        typer.Option("--decision-date", help="Dry-run observation decision date。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", help="Simple baseline strategy registry YAML。"),
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_first_forward_aging_observation_dry_run(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            manifest_path=manifest_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            decision_date=_parse_optional_date(decision_date),
        )
    )
    _print_simple_baseline_payload("First forward-aging observation dry-run", payload)


def strategies_reader_brief_forward_aging_safe_preview_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_reader_brief_forward_aging_safe_preview(output_root=output_root)
    )
    _print_simple_baseline_payload("Reader Brief forward-aging safe preview", payload)


def strategies_data_repair_owner_decision_pack_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Simple baseline 输出目录。"),
    ] = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: run_data_repair_owner_decision_pack(output_root=output_root)
    )
    _print_simple_baseline_payload("Data repair owner decision pack", payload)


_SIMPLE_BASELINE_STRATEGY_COMMANDS = (
    ("simple-baseline-registry-review", strategies_simple_baseline_registry_review_command),
    ("qqq-sgov-baseline-backtest", strategies_qqq_sgov_baseline_backtest_command),
    ("tqqq-sgov-risk-controlled-baseline", strategies_tqqq_sgov_risk_controlled_baseline_command),
    ("trend-vol-allocation-policy-search", strategies_trend_vol_allocation_policy_search_command),
    ("simple-baseline-dominance-ranking", strategies_simple_baseline_dominance_ranking_command),
    ("simple-baseline-pit-boundary-audit", strategies_simple_baseline_pit_boundary_audit_command),
    ("simple-baseline-cost-sensitivity", strategies_simple_baseline_cost_sensitivity_command),
    ("simple-baseline-regime-review", strategies_simple_baseline_regime_review_command),
    (
        "simple-baseline-forward-aging-tracker",
        strategies_simple_baseline_forward_aging_tracker_command,
    ),
    (
        "simple-baseline-paper-shadow-readiness",
        strategies_simple_baseline_paper_shadow_readiness_command,
    ),
    (
        "daily-reader-portfolio-control-safety-summary",
        strategies_daily_reader_portfolio_control_safety_summary_command,
    ),
    (
        "simple-baseline-portfolio-dry-run-mapper",
        strategies_simple_baseline_portfolio_dry_run_mapper_command,
    ),
    ("simple-baseline-master-review", strategies_simple_baseline_master_review_command),
    ("options-next-stage-gate", strategies_options_next_stage_gate_command),
    ("equal-risk-qqq-sgov-deep-dive", strategies_equal_risk_qqq_sgov_deep_dive_command),
    (
        "simple-baseline-period-split-validation",
        strategies_simple_baseline_period_split_validation_command,
    ),
    (
        "simple-baseline-drawdown-episode-review",
        strategies_simple_baseline_drawdown_episode_review_command,
    ),
    (
        "dynamic-vs-static-edge-significance-review",
        strategies_dynamic_vs_static_edge_significance_review_command,
    ),
    ("tqqq-heavy-pause-rationale-report", strategies_tqqq_heavy_pause_rationale_report_command),
    (
        "simple-baseline-watchlist-owner-decision",
        strategies_simple_baseline_watchlist_owner_decision_command,
    ),
    (
        "simple-baseline-real-result-reconciliation",
        strategies_simple_baseline_real_result_reconciliation_command,
    ),
    (
        "simple-baseline-forward-aging-candidate-freeze",
        strategies_simple_baseline_forward_aging_candidate_freeze_command,
    ),
    (
        "simple-baseline-forward-aging-contract",
        strategies_simple_baseline_forward_aging_contract_command,
    ),
    (
        "simple-baseline-forward-aging-write-observation",
        strategies_simple_baseline_forward_aging_write_observation_command,
    ),
    (
        "simple-baseline-forward-aging-update-maturity",
        strategies_simple_baseline_forward_aging_update_maturity_command,
    ),
    (
        "simple-baseline-forward-aging-scoreboard",
        strategies_simple_baseline_forward_aging_scoreboard_command,
    ),
    (
        "equal-risk-qqq-sgov-policy-definition-lock",
        strategies_equal_risk_qqq_sgov_policy_definition_lock_command,
    ),
    (
        "simple-baseline-comparator-definition-lock",
        strategies_simple_baseline_comparator_definition_lock_command,
    ),
    (
        "simple-baseline-forward-aging-data-quality-gate",
        strategies_simple_baseline_forward_aging_data_quality_gate_command,
    ),
    (
        "simple-baseline-paper-shadow-threshold-contract",
        strategies_simple_baseline_paper_shadow_threshold_contract_command,
    ),
    ("daily-reader-forward-aging-summary", strategies_daily_reader_forward_aging_summary_command),
    ("simple-baseline-risk-budget-review", strategies_simple_baseline_risk_budget_review_command),
    (
        "simple-baseline-absolute-return-gap-review",
        strategies_simple_baseline_absolute_return_gap_review_command,
    ),
    (
        "simple-baseline-candidate-role-assignment",
        strategies_simple_baseline_candidate_role_assignment_command,
    ),
    (
        "simple-baseline-forward-aging-owner-review-pack",
        strategies_simple_baseline_forward_aging_owner_review_pack_command,
    ),
    (
        "simple-baseline-forward-aging-automation-readiness",
        strategies_simple_baseline_forward_aging_automation_readiness_command,
    ),
    (
        "simple-baseline-forward-aging-master-review",
        strategies_simple_baseline_forward_aging_master_review_command,
    ),
    (
        "simple-baseline-data-source-inventory",
        strategies_simple_baseline_data_source_inventory_command,
    ),
    (
        "tqqq-cache-rebuild-and-validation",
        strategies_tqqq_cache_rebuild_and_validation_command,
    ),
    (
        "sgov-total-return-data-contract",
        strategies_sgov_total_return_data_contract_command,
    ),
    (
        "market-data-repair-manifest-audit",
        strategies_market_data_repair_manifest_audit_command,
    ),
    (
        "simple-baseline-validate-data-hardening",
        strategies_simple_baseline_validate_data_hardening_command,
    ),
    (
        "rerun-simple-baseline-real-cli-after-data-repair",
        strategies_rerun_simple_baseline_real_cli_after_data_repair_command,
    ),
    (
        "equal-risk-result-recompute-after-data-repair",
        strategies_equal_risk_result_recompute_after_data_repair_command,
    ),
    (
        "tqqq-challenger-revalidation-after-cache-fix",
        strategies_tqqq_challenger_revalidation_after_cache_fix_command,
    ),
    (
        "forward-aging-unblock-readiness-review",
        strategies_forward_aging_unblock_readiness_review_command,
    ),
    (
        "first-forward-aging-observation-dry-run",
        strategies_first_forward_aging_observation_dry_run_command,
    ),
    (
        "reader-brief-forward-aging-safe-preview",
        strategies_reader_brief_forward_aging_safe_preview_command,
    ),
    (
        "data-repair-owner-decision-pack",
        strategies_data_repair_owner_decision_pack_command,
    ),
)


def _build_research_payload(builder: Callable[[], dict[str, object]]) -> dict[str, object]:
    try:
        return builder()
    except (ResearchGovernanceError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc


def _print_simple_baseline_payload(label: str, payload: dict[str, object]) -> None:
    _print_status(label, str(payload["status"]))
    console.print(f"status={payload.get('status')}")
    _print_summary(payload)
    paths = payload.get("artifact_paths")
    if isinstance(paths, dict):
        console.print(f"JSON：{paths.get('json_path')}")
        console.print(f"Markdown：{paths.get('markdown_path')}")
    for safety_field, expected in (
        ("research_only", True),
        ("observe_only", True),
        ("promotion_allowed", False),
        ("paper_shadow_allowed", False),
        ("production_allowed", False),
        ("broker_action", "none"),
        ("production_effect", "none"),
    ):
        console.print(f"{safety_field}={payload.get(safety_field, expected)}")
    if str(payload["status"]) == "FAIL":
        raise typer.Exit(code=1)


def _parse_optional_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


def _print_summary(payload: dict[str, object]) -> None:
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        return
    compact = "; ".join(f"{key}={value}" for key, value in list(summary.items())[:6])
    if compact:
        console.print(compact)


def _print_status(label: str, status: str) -> None:
    style = "green" if status in {"PASS", "PROMISING"} else "yellow"
    if status in {"FAIL", "BLOCKED", "REJECTED"}:
        style = "red"
    console.print(f"[{style}]{label}：{status}[/{style}]")
