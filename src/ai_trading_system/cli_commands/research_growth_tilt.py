from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.equal_risk_growth_tilt import (
    DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    DEFAULT_EQUAL_RISK_GROWTH_TILT_OUTPUT_ROOT,
    DEFAULT_EQUAL_RISK_GROWTH_TILT_ROADMAP_OUTPUT_ROOT,
    DEFAULT_GROWTH_EXPLORATION_MASTER_REVIEW_DOC_PATH,
    DEFAULT_GROWTH_TILT_OWNER_DECISION_DOC_PATH,
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
    run_growth_exploration_master_review,
    run_growth_research_framing_correction,
    run_growth_tilt_beta_risk_budget_attribution,
    run_growth_tilt_cost_turnover_sensitivity,
    run_growth_tilt_definition_lock_versioning,
    run_growth_tilt_forward_aging_readiness_gate,
    run_growth_tilt_owner_decision_pack,
    run_growth_tilt_period_drawdown_replay,
    run_growth_tilt_reader_brief_safety_preview,
    run_roadmap_update_after_growth_tilt_review,
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

console = Console()


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
