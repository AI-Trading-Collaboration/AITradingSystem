from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.equal_risk_growth_tilt import (
    DEFAULT_BALANCED_CORE_LAUNCH_OWNER_REPORT_DOC_PATH,
    DEFAULT_BALANCED_CORE_OWNER_LAUNCH_PACK_DOC_PATH,
    DEFAULT_DUAL_FORWARD_AGING_MASTER_REVIEW_DOC_PATH,
    DEFAULT_DUAL_FORWARD_AGING_MONTHLY_MONITOR_CONTRACT_DOC_PATH,
    DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
    DEFAULT_EXTERNAL_VALIDATION_BALANCED_CORE_LAUNCH_MASTER_REVIEW_DOC_PATH,
    DEFAULT_GROWTH_EXPLORATION_MASTER_REVIEW_DOC_PATH,
    DEFAULT_GROWTH_TILT_FOCUSED_DIAGNOSIS_MASTER_REVIEW_DOC_PATH,
    DEFAULT_GROWTH_TILT_OWNER_DECISION_DOC_PATH,
    DEFAULT_GROWTH_TILT_OWNER_DECISION_REAL_RUN_DOC_PATH,
    DEFAULT_GROWTH_TILT_OWNER_DIAGNOSIS_PACK_DOC_PATH,
    DEFAULT_GROWTH_TILT_REAL_RESULT_MASTER_REVIEW_DOC_PATH,
    run_balanced_core_definition_lock,
    run_balanced_core_first_observation_write,
    run_balanced_core_first_observation_write_after_validation,
    run_balanced_core_forward_aging_dry_run,
    run_balanced_core_idempotency_duplicate_guard,
    run_balanced_core_launch_owner_report,
    run_balanced_core_launch_preflight,
    run_balanced_core_maturity_scoreboard_safety_gate,
    run_balanced_core_observation_idempotency_proof,
    run_balanced_core_owner_launch_pack,
    run_balanced_core_watchlist_activation_contract,
    run_best_growth_tilt_candidate_deep_dive,
    run_beta_adjusted_edge_methodology_audit,
    run_dual_forward_aging_comparator_panel,
    run_dual_forward_aging_comparator_panel_after_launch,
    run_dual_forward_aging_master_review,
    run_dual_forward_aging_monthly_monitor_contract,
    run_dual_forward_aging_reader_brief_safe_preview,
    run_dual_forward_aging_reader_brief_safe_preview_after_launch,
    run_dual_forward_aging_scoreboard_safety_review,
    run_equal_risk_cap_floor_tilt_search,
    run_equal_risk_growth_tilt_objective_contract,
    run_equal_risk_growth_tilt_ranking_tiering,
    run_equal_risk_growth_tilt_registry_review,
    run_equal_risk_growth_tilt_tradeoff_frontier,
    run_equal_risk_missed_upside_compensation_search,
    run_equal_risk_risk_budget_tilt_search,
    run_equal_risk_small_tqqq_overlay_search,
    run_equal_risk_trend_on_qqq_boost_search,
    run_equal_risk_vol_target_growth_tilt_search,
    run_external_validation_balanced_core_launch_master_review,
    run_growth_exploration_master_review,
    run_growth_research_framing_correction,
    run_growth_tilt_balanced_core_role_review,
    run_growth_tilt_beta_adjusted_edge_review,
    run_growth_tilt_beta_risk_budget_attribution,
    run_growth_tilt_candidate_result_summary,
    run_growth_tilt_cost_turnover_sensitivity,
    run_growth_tilt_definition_lock_versioning,
    run_growth_tilt_focused_diagnosis_master_review,
    run_growth_tilt_forward_aging_readiness_gate,
    run_growth_tilt_forward_aging_watchlist_review,
    run_growth_tilt_owner_decision_pack,
    run_growth_tilt_owner_decision_pack_real_run,
    run_growth_tilt_owner_diagnosis_pack,
    run_growth_tilt_parameter_neighbor_finalist_review,
    run_growth_tilt_period_drawdown_cost_triage,
    run_growth_tilt_period_drawdown_replay,
    run_growth_tilt_reader_brief_safety_preview,
    run_growth_tilt_real_cli_suite,
    run_growth_tilt_real_result_master_review,
    run_growth_tilt_risk_return_frontier_review,
    run_growth_tilt_tier_validation,
    run_growth_tilt_vs_equal_risk_and_qqq_final_gate,
    run_growth_tilt_vs_equal_risk_missed_upside_review,
    run_growth_tilt_watchlist_reconsideration_gate,
    run_roadmap_update_after_growth_tilt_review,
    run_vol_target_growth_tilt_local_sensitivity,
)
from ai_trading_system.research_governance import ResearchGovernanceError
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
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
)

console = Console()
DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT_FOR_GROWTH = (
    DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT.parent / "external_validation"
)


def register_growth_tilt_strategy_commands(strategies_app: typer.Typer) -> None:
    for command_name, command in _GROWTH_TILT_STRATEGY_COMMANDS:
        strategies_app.command(command_name)(command)


def _make_growth_tilt_output_command(
    builder: Callable[..., dict[str, object]],
    label: str,
) -> Callable[..., None]:
    def command(
        output_root: Annotated[
            Path,
            typer.Option("--output-root"),
        ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    ) -> None:
        payload = _build_growth_tilt_payload(lambda: builder(output_root=output_root))
        _print_growth_tilt_payload(label, payload)

    command.__name__ = f"strategies_{label.lower().replace(' ', '_')}_command"
    return command


def _make_growth_tilt_config_command(
    builder: Callable[..., dict[str, object]],
    label: str,
) -> Callable[..., None]:
    def command(
        config_path: Annotated[
            Path,
            typer.Option("--config"),
        ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
        output_root: Annotated[
            Path,
            typer.Option("--output-root"),
        ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    ) -> None:
        payload = _build_growth_tilt_payload(
            lambda: builder(config_path=config_path, output_root=output_root)
        )
        _print_growth_tilt_payload(label, payload)

    command.__name__ = f"strategies_{label.lower().replace(' ', '_')}_command"
    return command


def _make_growth_tilt_data_command(
    builder: Callable[..., dict[str, object]],
    label: str,
) -> Callable[..., None]:
    def command(
        prices_path: Annotated[
            Path,
            typer.Option("--prices-path"),
        ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
        marketstack_prices_path: Annotated[
            Path,
            typer.Option("--marketstack-prices-path"),
        ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
        rates_path: Annotated[
            Path,
            typer.Option("--rates-path"),
        ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
        as_of: Annotated[str | None, typer.Option("--as-of")] = None,
        start_date: Annotated[str | None, typer.Option("--start-date")] = None,
        end_date: Annotated[str | None, typer.Option("--end-date")] = None,
        config_path: Annotated[
            Path,
            typer.Option("--config"),
        ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
        output_root: Annotated[
            Path,
            typer.Option("--output-root"),
        ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    ) -> None:
        payload = _build_growth_tilt_payload(
            lambda: builder(
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
        _print_growth_tilt_payload(label, payload)

    command.__name__ = f"strategies_{label.lower().replace(' ', '_')}_command"
    return command


def _make_growth_tilt_forward_data_command(
    builder: Callable[..., dict[str, object]],
    label: str,
) -> Callable[..., None]:
    def command(
        prices_path: Annotated[
            Path,
            typer.Option("--prices-path"),
        ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
        marketstack_prices_path: Annotated[
            Path,
            typer.Option("--marketstack-prices-path"),
        ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
        rates_path: Annotated[
            Path,
            typer.Option("--rates-path"),
        ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
        as_of: Annotated[str | None, typer.Option("--as-of")] = None,
        start_date: Annotated[str | None, typer.Option("--start-date")] = None,
        end_date: Annotated[str | None, typer.Option("--end-date")] = None,
        decision_date: Annotated[str | None, typer.Option("--decision-date")] = None,
        config_path: Annotated[
            Path,
            typer.Option("--config"),
        ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
        output_root: Annotated[
            Path,
            typer.Option("--output-root"),
        ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    ) -> None:
        payload = _build_growth_tilt_payload(
            lambda: builder(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                config_path=config_path,
                output_root=output_root,
                as_of_date=_parse_optional_date(as_of),
                start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
                end_date=_parse_optional_date(end_date),
                decision_date=_parse_optional_date(decision_date),
            )
        )
        _print_growth_tilt_payload(label, payload)

    command.__name__ = f"strategies_{label.lower().replace(' ', '_')}_command"
    return command


def _make_growth_tilt_doc_command(
    builder: Callable[..., dict[str, object]],
    label: str,
    default_docs_path: Path,
) -> Callable[..., None]:
    def command(
        prices_path: Annotated[
            Path,
            typer.Option("--prices-path"),
        ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
        marketstack_prices_path: Annotated[
            Path,
            typer.Option("--marketstack-prices-path"),
        ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
        rates_path: Annotated[
            Path,
            typer.Option("--rates-path"),
        ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
        as_of: Annotated[str | None, typer.Option("--as-of")] = None,
        start_date: Annotated[str | None, typer.Option("--start-date")] = None,
        end_date: Annotated[str | None, typer.Option("--end-date")] = None,
        config_path: Annotated[
            Path,
            typer.Option("--config"),
        ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
        output_root: Annotated[
            Path,
            typer.Option("--output-root"),
        ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
        docs_path: Annotated[
            Path,
            typer.Option("--docs-path"),
        ] = default_docs_path,
    ) -> None:
        payload = _build_growth_tilt_payload(
            lambda: builder(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                config_path=config_path,
                output_root=output_root,
                docs_path=docs_path,
                as_of_date=_parse_optional_date(as_of),
                start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
                end_date=_parse_optional_date(end_date),
            )
        )
        _print_growth_tilt_payload(label, payload)

    command.__name__ = f"strategies_{label.lower().replace(' ', '_')}_command"
    return command


def strategies_roadmap_update_after_growth_tilt_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config"),
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    growth_output_root: Annotated[
        Path,
        typer.Option("--growth-output-root"),
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
    growth_master_docs_path: Annotated[
        Path,
        typer.Option("--growth-master-docs-path"),
    ] = DEFAULT_GROWTH_EXPLORATION_MASTER_REVIEW_DOC_PATH,
    growth_owner_docs_path: Annotated[
        Path,
        typer.Option("--growth-owner-docs-path"),
    ] = DEFAULT_GROWTH_TILT_OWNER_DECISION_DOC_PATH,
) -> None:
    payload = _build_growth_tilt_payload(
        lambda: run_roadmap_update_after_growth_tilt_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            growth_output_root=growth_output_root,
            output_root=output_root,
            growth_master_docs_path=growth_master_docs_path,
            growth_owner_docs_path=growth_owner_docs_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_growth_tilt_payload("Roadmap update after growth tilt review", payload)


def strategies_growth_tilt_real_cli_suite_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config"),
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    roadmap_output_root: Annotated[
        Path,
        typer.Option("--roadmap-output-root"),
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
    owner_docs_path: Annotated[
        Path,
        typer.Option("--owner-docs-path"),
    ] = DEFAULT_GROWTH_TILT_OWNER_DECISION_DOC_PATH,
    master_docs_path: Annotated[
        Path,
        typer.Option("--master-docs-path"),
    ] = DEFAULT_GROWTH_EXPLORATION_MASTER_REVIEW_DOC_PATH,
) -> None:
    payload = _build_growth_tilt_payload(
        lambda: run_growth_tilt_real_cli_suite(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            roadmap_output_root=roadmap_output_root,
            owner_docs_path=owner_docs_path,
            master_docs_path=master_docs_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_growth_tilt_payload("Growth tilt real CLI suite", payload)


def strategies_growth_tilt_real_result_master_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config"),
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    roadmap_output_root: Annotated[
        Path,
        typer.Option("--roadmap-output-root"),
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
    docs_path: Annotated[
        Path,
        typer.Option("--docs-path"),
    ] = DEFAULT_GROWTH_TILT_REAL_RESULT_MASTER_REVIEW_DOC_PATH,
    owner_docs_path: Annotated[
        Path,
        typer.Option("--owner-docs-path"),
    ] = DEFAULT_GROWTH_TILT_OWNER_DECISION_REAL_RUN_DOC_PATH,
    source_owner_docs_path: Annotated[
        Path,
        typer.Option("--source-owner-docs-path"),
    ] = DEFAULT_GROWTH_TILT_OWNER_DECISION_DOC_PATH,
    source_master_docs_path: Annotated[
        Path,
        typer.Option("--source-master-docs-path"),
    ] = DEFAULT_GROWTH_EXPLORATION_MASTER_REVIEW_DOC_PATH,
) -> None:
    payload = _build_growth_tilt_payload(
        lambda: run_growth_tilt_real_result_master_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            roadmap_output_root=roadmap_output_root,
            docs_path=docs_path,
            owner_docs_path=owner_docs_path,
            source_owner_docs_path=source_owner_docs_path,
            source_master_docs_path=source_master_docs_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_growth_tilt_payload("Growth tilt real result master review", payload)


def strategies_dual_forward_aging_comparator_panel_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config"),
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    growth_output_root: Annotated[
        Path,
        typer.Option("--growth-output-root"),
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
) -> None:
    payload = _build_growth_tilt_payload(
        lambda: run_dual_forward_aging_comparator_panel(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            growth_output_root=growth_output_root,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_growth_tilt_payload("Dual forward-aging comparator panel", payload)


def strategies_dual_forward_aging_reader_brief_safe_preview_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config"),
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    growth_output_root: Annotated[
        Path,
        typer.Option("--growth-output-root"),
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
) -> None:
    payload = _build_growth_tilt_payload(
        lambda: run_dual_forward_aging_reader_brief_safe_preview(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            growth_output_root=growth_output_root,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_growth_tilt_payload("Dual forward-aging Reader Brief safe preview", payload)


def strategies_balanced_core_owner_launch_pack_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config"),
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    growth_output_root: Annotated[
        Path,
        typer.Option("--growth-output-root"),
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
    docs_path: Annotated[
        Path,
        typer.Option("--docs-path"),
    ] = DEFAULT_BALANCED_CORE_OWNER_LAUNCH_PACK_DOC_PATH,
) -> None:
    payload = _build_growth_tilt_payload(
        lambda: run_balanced_core_owner_launch_pack(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            growth_output_root=growth_output_root,
            output_root=output_root,
            docs_path=docs_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_growth_tilt_payload("Balanced core owner launch pack", payload)


def strategies_dual_forward_aging_master_review_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path"),
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path"),
    ] = DEFAULT_SIMPLE_BASELINE_RATES_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config"),
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    growth_output_root: Annotated[
        Path,
        typer.Option("--growth-output-root"),
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root"),
    ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
    docs_path: Annotated[
        Path,
        typer.Option("--docs-path"),
    ] = DEFAULT_DUAL_FORWARD_AGING_MASTER_REVIEW_DOC_PATH,
    owner_docs_path: Annotated[
        Path,
        typer.Option("--owner-docs-path"),
    ] = DEFAULT_BALANCED_CORE_OWNER_LAUNCH_PACK_DOC_PATH,
) -> None:
    payload = _build_growth_tilt_payload(
        lambda: run_dual_forward_aging_master_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            growth_output_root=growth_output_root,
            output_root=output_root,
            docs_path=docs_path,
            owner_docs_path=owner_docs_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_growth_tilt_payload("Dual forward-aging master review", payload)


def strategies_balanced_core_launch_preflight_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = (
        DEFAULT_SIMPLE_BASELINE_PRICES_PATH
    ),
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = (
        DEFAULT_SIMPLE_BASELINE_RATES_PATH
    ),
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    config_path: Annotated[Path, typer.Option("--config")] = (
        DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH
    ),
    external_validation_output_root: Annotated[
        Path, typer.Option("--external-validation-output-root")
    ] = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT_FOR_GROWTH,
    output_root: Annotated[Path, typer.Option("--output-root")] = (
        DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    decision_date: Annotated[str | None, typer.Option("--decision-date")] = None,
) -> None:
    payload = _build_growth_tilt_payload(
        lambda: run_balanced_core_launch_preflight(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            config_path=config_path,
            external_validation_output_root=external_validation_output_root,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
            decision_date=_parse_optional_date(decision_date),
        )
    )
    _print_growth_tilt_payload("Balanced core launch preflight", payload)


def strategies_balanced_core_first_observation_write_after_validation_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = (
        DEFAULT_SIMPLE_BASELINE_PRICES_PATH
    ),
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = (
        DEFAULT_SIMPLE_BASELINE_RATES_PATH
    ),
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    config_path: Annotated[Path, typer.Option("--config")] = (
        DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH
    ),
    external_validation_output_root: Annotated[
        Path, typer.Option("--external-validation-output-root")
    ] = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT_FOR_GROWTH,
    output_root: Annotated[Path, typer.Option("--output-root")] = (
        DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    decision_date: Annotated[str | None, typer.Option("--decision-date")] = None,
) -> None:
    payload = _build_growth_tilt_payload(
        lambda: run_balanced_core_first_observation_write_after_validation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            config_path=config_path,
            external_validation_output_root=external_validation_output_root,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
            decision_date=_parse_optional_date(decision_date),
        )
    )
    _print_growth_tilt_payload(
        "Balanced core first observation write after validation", payload
    )


def strategies_dual_forward_aging_comparator_panel_after_launch_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = (
        DEFAULT_SIMPLE_BASELINE_PRICES_PATH
    ),
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = (
        DEFAULT_SIMPLE_BASELINE_RATES_PATH
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[Path, typer.Option("--config")] = (
        DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH
    ),
    growth_output_root: Annotated[Path, typer.Option("--growth-output-root")] = (
        DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT
    ),
    output_root: Annotated[Path, typer.Option("--output-root")] = (
        DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT
    ),
) -> None:
    payload = _build_growth_tilt_payload(
        lambda: run_dual_forward_aging_comparator_panel_after_launch(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            growth_output_root=growth_output_root,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_growth_tilt_payload("Dual forward-aging comparator panel after launch", payload)


def strategies_dual_forward_aging_scoreboard_safety_review_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = (
        DEFAULT_SIMPLE_BASELINE_PRICES_PATH
    ),
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = (
        DEFAULT_SIMPLE_BASELINE_RATES_PATH
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    config_path: Annotated[Path, typer.Option("--config")] = (
        DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH
    ),
    growth_output_root: Annotated[Path, typer.Option("--growth-output-root")] = (
        DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT
    ),
    output_root: Annotated[Path, typer.Option("--output-root")] = (
        DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT
    ),
) -> None:
    payload = _build_growth_tilt_payload(
        lambda: run_dual_forward_aging_scoreboard_safety_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            growth_output_root=growth_output_root,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_growth_tilt_payload("Dual forward-aging scoreboard safety review", payload)


def strategies_dual_forward_aging_reader_brief_safe_preview_after_launch_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = (
        DEFAULT_SIMPLE_BASELINE_PRICES_PATH
    ),
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = (
        DEFAULT_SIMPLE_BASELINE_RATES_PATH
    ),
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    config_path: Annotated[Path, typer.Option("--config")] = (
        DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH
    ),
    external_validation_output_root: Annotated[
        Path, typer.Option("--external-validation-output-root")
    ] = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT_FOR_GROWTH,
    growth_output_root: Annotated[Path, typer.Option("--growth-output-root")] = (
        DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT
    ),
    output_root: Annotated[Path, typer.Option("--output-root")] = (
        DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = _build_growth_tilt_payload(
        lambda: run_dual_forward_aging_reader_brief_safe_preview_after_launch(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            config_path=config_path,
            external_validation_output_root=external_validation_output_root,
            growth_output_root=growth_output_root,
            output_root=output_root,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_growth_tilt_payload(
        "Dual forward-aging Reader Brief safe preview after launch", payload
    )


def strategies_balanced_core_launch_owner_report_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = (
        DEFAULT_SIMPLE_BASELINE_PRICES_PATH
    ),
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = (
        DEFAULT_SIMPLE_BASELINE_RATES_PATH
    ),
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    config_path: Annotated[Path, typer.Option("--config")] = (
        DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH
    ),
    external_validation_output_root: Annotated[
        Path, typer.Option("--external-validation-output-root")
    ] = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT_FOR_GROWTH,
    growth_output_root: Annotated[Path, typer.Option("--growth-output-root")] = (
        DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT
    ),
    output_root: Annotated[Path, typer.Option("--output-root")] = (
        DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT
    ),
    docs_path: Annotated[Path, typer.Option("--docs-path")] = (
        DEFAULT_BALANCED_CORE_LAUNCH_OWNER_REPORT_DOC_PATH
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = _build_growth_tilt_payload(
        lambda: run_balanced_core_launch_owner_report(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            config_path=config_path,
            external_validation_output_root=external_validation_output_root,
            growth_output_root=growth_output_root,
            output_root=output_root,
            docs_path=docs_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_growth_tilt_payload("Balanced core launch owner report", payload)


def strategies_external_validation_balanced_core_launch_master_review_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = (
        DEFAULT_SIMPLE_BASELINE_PRICES_PATH
    ),
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_SIMPLE_BASELINE_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = (
        DEFAULT_SIMPLE_BASELINE_RATES_PATH
    ),
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    config_path: Annotated[Path, typer.Option("--config")] = (
        DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH
    ),
    external_validation_output_root: Annotated[
        Path, typer.Option("--external-validation-output-root")
    ] = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT_FOR_GROWTH,
    growth_output_root: Annotated[Path, typer.Option("--growth-output-root")] = (
        DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT
    ),
    output_root: Annotated[Path, typer.Option("--output-root")] = (
        DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT
    ),
    docs_path: Annotated[Path, typer.Option("--docs-path")] = (
        DEFAULT_EXTERNAL_VALIDATION_BALANCED_CORE_LAUNCH_MASTER_REVIEW_DOC_PATH
    ),
    owner_docs_path: Annotated[Path, typer.Option("--owner-docs-path")] = (
        DEFAULT_BALANCED_CORE_LAUNCH_OWNER_REPORT_DOC_PATH
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = _build_growth_tilt_payload(
        lambda: run_external_validation_balanced_core_launch_master_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            config_path=config_path,
            external_validation_output_root=external_validation_output_root,
            growth_output_root=growth_output_root,
            output_root=output_root,
            docs_path=docs_path,
            owner_docs_path=owner_docs_path,
            as_of_date=_parse_optional_date(as_of),
            start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
            end_date=_parse_optional_date(end_date),
        )
    )
    _print_growth_tilt_payload(
        "External validation balanced core launch master review", payload
    )


def strategies_dual_forward_aging_monthly_monitor_contract_command(
    output_root: Annotated[Path, typer.Option("--output-root")] = (
        DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT
    ),
    docs_path: Annotated[Path, typer.Option("--docs-path")] = (
        DEFAULT_DUAL_FORWARD_AGING_MONTHLY_MONITOR_CONTRACT_DOC_PATH
    ),
) -> None:
    payload = _build_growth_tilt_payload(
        lambda: run_dual_forward_aging_monthly_monitor_contract(
            output_root=output_root,
            docs_path=docs_path,
        )
    )
    _print_growth_tilt_payload("Dual forward-aging monthly monitor contract", payload)


_GROWTH_TILT_STRATEGY_COMMANDS = (
    (
        "growth-research-framing-correction",
        _make_growth_tilt_output_command(
            run_growth_research_framing_correction,
            "Growth research framing correction",
        ),
    ),
    (
        "equal-risk-growth-tilt-objective-contract",
        _make_growth_tilt_config_command(
            run_equal_risk_growth_tilt_objective_contract,
            "Equal risk growth tilt objective contract",
        ),
    ),
    (
        "equal-risk-growth-tilt-registry-review",
        _make_growth_tilt_config_command(
            run_equal_risk_growth_tilt_registry_review,
            "Equal risk growth tilt registry review",
        ),
    ),
    (
        "equal-risk-cap-floor-tilt-search",
        _make_growth_tilt_data_command(
            run_equal_risk_cap_floor_tilt_search,
            "Equal risk cap floor tilt search",
        ),
    ),
    (
        "equal-risk-risk-budget-tilt-search",
        _make_growth_tilt_data_command(
            run_equal_risk_risk_budget_tilt_search,
            "Equal risk risk budget tilt search",
        ),
    ),
    (
        "equal-risk-trend-on-qqq-boost-search",
        _make_growth_tilt_data_command(
            run_equal_risk_trend_on_qqq_boost_search,
            "Equal risk trend on QQQ boost search",
        ),
    ),
    (
        "equal-risk-missed-upside-compensation-search",
        _make_growth_tilt_data_command(
            run_equal_risk_missed_upside_compensation_search,
            "Equal risk missed upside compensation search",
        ),
    ),
    (
        "equal-risk-small-tqqq-overlay-search",
        _make_growth_tilt_data_command(
            run_equal_risk_small_tqqq_overlay_search,
            "Equal risk small TQQQ overlay search",
        ),
    ),
    (
        "equal-risk-vol-target-growth-tilt-search",
        _make_growth_tilt_data_command(
            run_equal_risk_vol_target_growth_tilt_search,
            "Equal risk vol target growth tilt search",
        ),
    ),
    (
        "equal-risk-growth-tilt-ranking-tiering",
        _make_growth_tilt_data_command(
            run_equal_risk_growth_tilt_ranking_tiering,
            "Equal risk growth tilt ranking tiering",
        ),
    ),
    (
        "growth-tilt-beta-risk-budget-attribution",
        _make_growth_tilt_data_command(
            run_growth_tilt_beta_risk_budget_attribution,
            "Growth tilt beta risk budget attribution",
        ),
    ),
    (
        "growth-tilt-period-drawdown-replay",
        _make_growth_tilt_data_command(
            run_growth_tilt_period_drawdown_replay,
            "Growth tilt period drawdown replay",
        ),
    ),
    (
        "growth-tilt-cost-turnover-sensitivity",
        _make_growth_tilt_data_command(
            run_growth_tilt_cost_turnover_sensitivity,
            "Growth tilt cost turnover sensitivity",
        ),
    ),
    (
        "equal-risk-growth-tilt-tradeoff-frontier",
        _make_growth_tilt_data_command(
            run_equal_risk_growth_tilt_tradeoff_frontier,
            "Equal risk growth tilt tradeoff frontier",
        ),
    ),
    (
        "growth-tilt-definition-lock-versioning",
        _make_growth_tilt_data_command(
            run_growth_tilt_definition_lock_versioning,
            "Growth tilt definition lock versioning",
        ),
    ),
    (
        "growth-tilt-forward-aging-readiness-gate",
        _make_growth_tilt_data_command(
            run_growth_tilt_forward_aging_readiness_gate,
            "Growth tilt forward aging readiness gate",
        ),
    ),
    (
        "growth-tilt-owner-decision-pack",
        _make_growth_tilt_doc_command(
            run_growth_tilt_owner_decision_pack,
            "Growth tilt owner decision pack",
            DEFAULT_GROWTH_TILT_OWNER_DECISION_DOC_PATH,
        ),
    ),
    (
        "growth-exploration-master-review",
        _make_growth_tilt_doc_command(
            run_growth_exploration_master_review,
            "Growth exploration master review",
            DEFAULT_GROWTH_EXPLORATION_MASTER_REVIEW_DOC_PATH,
        ),
    ),
    (
        "roadmap-update-after-growth-tilt-review",
        strategies_roadmap_update_after_growth_tilt_review_command,
    ),
    (
        "growth-tilt-reader-brief-safety-preview",
        _make_growth_tilt_data_command(
            run_growth_tilt_reader_brief_safety_preview,
            "Growth tilt Reader Brief safety preview",
        ),
    ),
    (
        "growth-tilt-real-cli-suite",
        strategies_growth_tilt_real_cli_suite_command,
    ),
    (
        "growth-tilt-candidate-result-summary",
        _make_growth_tilt_data_command(
            run_growth_tilt_candidate_result_summary,
            "Growth tilt candidate result summary",
        ),
    ),
    (
        "growth-tilt-tier-validation",
        _make_growth_tilt_data_command(
            run_growth_tilt_tier_validation,
            "Growth tilt tier validation",
        ),
    ),
    (
        "growth-tilt-beta-adjusted-edge-review",
        _make_growth_tilt_data_command(
            run_growth_tilt_beta_adjusted_edge_review,
            "Growth tilt beta-adjusted edge review",
        ),
    ),
    (
        "growth-tilt-risk-return-frontier-review",
        _make_growth_tilt_data_command(
            run_growth_tilt_risk_return_frontier_review,
            "Growth tilt risk-return frontier review",
        ),
    ),
    (
        "growth-tilt-period-drawdown-cost-triage",
        _make_growth_tilt_data_command(
            run_growth_tilt_period_drawdown_cost_triage,
            "Growth tilt period drawdown cost triage",
        ),
    ),
    (
        "growth-tilt-vs-equal-risk-and-qqq-final-gate",
        _make_growth_tilt_data_command(
            run_growth_tilt_vs_equal_risk_and_qqq_final_gate,
            "Growth tilt final gate",
        ),
    ),
    (
        "growth-tilt-forward-aging-watchlist-review",
        _make_growth_tilt_data_command(
            run_growth_tilt_forward_aging_watchlist_review,
            "Growth tilt forward-aging watchlist review",
        ),
    ),
    (
        "growth-tilt-owner-decision-pack-real-run",
        _make_growth_tilt_doc_command(
            run_growth_tilt_owner_decision_pack_real_run,
            "Growth tilt owner decision pack real run",
            DEFAULT_GROWTH_TILT_OWNER_DECISION_REAL_RUN_DOC_PATH,
        ),
    ),
    (
        "growth-tilt-real-result-master-review",
        strategies_growth_tilt_real_result_master_review_command,
    ),
    (
        "best-growth-tilt-candidate-deep-dive",
        _make_growth_tilt_data_command(
            run_best_growth_tilt_candidate_deep_dive,
            "Best growth tilt candidate deep dive",
        ),
    ),
    (
        "vol-target-growth-tilt-local-sensitivity",
        _make_growth_tilt_data_command(
            run_vol_target_growth_tilt_local_sensitivity,
            "Vol target growth tilt local sensitivity",
        ),
    ),
    (
        "beta-adjusted-edge-methodology-audit",
        _make_growth_tilt_data_command(
            run_beta_adjusted_edge_methodology_audit,
            "Beta adjusted edge methodology audit",
        ),
    ),
    (
        "growth-tilt-balanced-core-role-review",
        _make_growth_tilt_data_command(
            run_growth_tilt_balanced_core_role_review,
            "Growth tilt balanced core role review",
        ),
    ),
    (
        "growth-tilt-vs-equal-risk-missed-upside-review",
        _make_growth_tilt_data_command(
            run_growth_tilt_vs_equal_risk_missed_upside_review,
            "Growth tilt vs equal risk missed upside review",
        ),
    ),
    (
        "growth-tilt-parameter-neighbor-finalist-review",
        _make_growth_tilt_data_command(
            run_growth_tilt_parameter_neighbor_finalist_review,
            "Growth tilt parameter neighbor finalist review",
        ),
    ),
    (
        "growth-tilt-watchlist-reconsideration-gate",
        _make_growth_tilt_data_command(
            run_growth_tilt_watchlist_reconsideration_gate,
            "Growth tilt watchlist reconsideration gate",
        ),
    ),
    (
        "growth-tilt-owner-diagnosis-pack",
        _make_growth_tilt_doc_command(
            run_growth_tilt_owner_diagnosis_pack,
            "Growth tilt owner diagnosis pack",
            DEFAULT_GROWTH_TILT_OWNER_DIAGNOSIS_PACK_DOC_PATH,
        ),
    ),
    (
        "growth-tilt-focused-diagnosis-master-review",
        _make_growth_tilt_doc_command(
            run_growth_tilt_focused_diagnosis_master_review,
            "Growth tilt focused diagnosis master review",
            DEFAULT_GROWTH_TILT_FOCUSED_DIAGNOSIS_MASTER_REVIEW_DOC_PATH,
        ),
    ),
    (
        "balanced-core-watchlist-activation-contract",
        _make_growth_tilt_data_command(
            run_balanced_core_watchlist_activation_contract,
            "Balanced core watchlist activation contract",
        ),
    ),
    (
        "balanced-core-definition-lock",
        _make_growth_tilt_data_command(
            run_balanced_core_definition_lock,
            "Balanced core definition lock",
        ),
    ),
    (
        "balanced-core-forward-aging-dry-run",
        _make_growth_tilt_forward_data_command(
            run_balanced_core_forward_aging_dry_run,
            "Balanced core forward-aging dry run",
        ),
    ),
    (
        "balanced-core-first-observation-write",
        _make_growth_tilt_forward_data_command(
            run_balanced_core_first_observation_write,
            "Balanced core first observation write",
        ),
    ),
    (
        "balanced-core-idempotency-duplicate-guard",
        _make_growth_tilt_forward_data_command(
            run_balanced_core_idempotency_duplicate_guard,
            "Balanced core idempotency duplicate guard",
        ),
    ),
    (
        "balanced-core-maturity-scoreboard-safety-gate",
        _make_growth_tilt_data_command(
            run_balanced_core_maturity_scoreboard_safety_gate,
            "Balanced core maturity scoreboard safety gate",
        ),
    ),
    (
        "dual-forward-aging-comparator-panel",
        strategies_dual_forward_aging_comparator_panel_command,
    ),
    (
        "dual-forward-aging-reader-brief-safe-preview",
        strategies_dual_forward_aging_reader_brief_safe_preview_command,
    ),
    (
        "balanced-core-owner-launch-pack",
        strategies_balanced_core_owner_launch_pack_command,
    ),
    (
        "dual-forward-aging-master-review",
        strategies_dual_forward_aging_master_review_command,
    ),
    (
        "balanced-core-launch-preflight",
        strategies_balanced_core_launch_preflight_command,
    ),
    (
        "balanced-core-first-observation-write-after-validation",
        strategies_balanced_core_first_observation_write_after_validation_command,
    ),
    (
        "balanced-core-observation-idempotency-proof",
        _make_growth_tilt_forward_data_command(
            run_balanced_core_observation_idempotency_proof,
            "Balanced core observation idempotency proof",
        ),
    ),
    (
        "dual-forward-aging-comparator-panel-after-launch",
        strategies_dual_forward_aging_comparator_panel_after_launch_command,
    ),
    (
        "dual-forward-aging-scoreboard-safety-review",
        strategies_dual_forward_aging_scoreboard_safety_review_command,
    ),
    (
        "dual-forward-aging-reader-brief-safe-preview-after-launch",
        strategies_dual_forward_aging_reader_brief_safe_preview_after_launch_command,
    ),
    (
        "balanced-core-launch-owner-report",
        strategies_balanced_core_launch_owner_report_command,
    ),
    (
        "external-validation-balanced-core-launch-master-review",
        strategies_external_validation_balanced_core_launch_master_review_command,
    ),
    (
        "dual-forward-aging-monthly-monitor-contract",
        strategies_dual_forward_aging_monthly_monitor_contract_command,
    ),
)


def _build_growth_tilt_payload(builder: Callable[[], dict[str, object]]) -> dict[str, object]:
    try:
        return builder()
    except (ResearchGovernanceError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc


def _print_growth_tilt_payload(label: str, payload: dict[str, object]) -> None:
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
