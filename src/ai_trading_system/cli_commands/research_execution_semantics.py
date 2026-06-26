from __future__ import annotations

import inspect
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.execution_semantics import (
    DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    DEFAULT_LAYER1_SELECTOR_CONFIG_PATH,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    run_dynamic_backtest_engine_contract_update,
    run_dynamic_strategy_execution_semantics_contract,
    run_dynamic_strategy_latency_execution_lag_review,
    run_dynamic_strategy_validity_period_audit,
    run_equal_risk_balanced_core_execution_policy_selection,
    run_execution_aware_forward_aging_observation_contract,
    run_execution_policy_cost_turnover_normalization,
    run_execution_policy_impact_on_prior_conclusions,
    run_execution_semantics_data_lineage_audit,
    run_execution_semantics_external_validation_update,
    run_execution_semantics_master_review,
    run_execution_semantics_reporting_update,
    run_implicit_monthly_rebalance_assumption_audit,
    run_reader_brief_execution_semantics_safe_preview,
    run_rebalance_assumption_owner_review_pack,
    run_rebalance_frequency_sensitivity_suite,
    run_rebalance_sensitive_candidate_recovery_review,
    run_roadmap_update_after_execution_semantics_review,
    run_signal_staleness_cost_review,
    run_strategy_execution_policy_registry_review,
    run_target_vs_actual_position_path_builder,
    run_threshold_hybrid_rebalance_review,
)

console = Console()


def register_execution_semantics_strategy_commands(strategies_app: typer.Typer) -> None:
    for command_name, builder, label in _EXECUTION_SEMANTICS_COMMANDS:
        strategies_app.command(command_name)(_make_execution_semantics_command(builder, label))


def _make_execution_semantics_command(
    builder: Callable[..., dict[str, object]],
    label: str,
) -> Callable[..., None]:
    def command(
        prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
        marketstack_prices_path: Annotated[
            Path, typer.Option("--marketstack-prices-path")
        ] = DEFAULT_MARKETSTACK_PRICES_PATH,
        rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
        simple_config_path: Annotated[
            Path, typer.Option("--simple-config")
        ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
        growth_config_path: Annotated[
            Path, typer.Option("--growth-config")
        ] = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
        controlled_growth_config_path: Annotated[
            Path, typer.Option("--controlled-growth-config")
        ] = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
        layer1_config_path: Annotated[
            Path, typer.Option("--layer1-config")
        ] = DEFAULT_LAYER1_SELECTOR_CONFIG_PATH,
        qqq_plus_config_path: Annotated[
            Path, typer.Option("--qqq-plus-config")
        ] = DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
        policy_registry_path: Annotated[
            Path, typer.Option("--policy-registry")
        ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
        output_root: Annotated[
            Path, typer.Option("--output-root")
        ] = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
        docs_path: Annotated[Path | None, typer.Option("--docs-path")] = None,
        strategy_id: Annotated[str, typer.Option("--strategy-id")] = "equal_risk_qqq_sgov",
        execution_policy_id: Annotated[
            str, typer.Option("--execution-policy-id")
        ] = "monthly_plus_threshold_5pct_v1",
        as_of: Annotated[str | None, typer.Option("--as-of")] = None,
        start_date: Annotated[str | None, typer.Option("--start-date")] = None,
        end_date: Annotated[str | None, typer.Option("--end-date")] = None,
    ) -> None:
        kwargs: dict[str, object] = {
            "prices_path": prices_path,
            "marketstack_prices_path": marketstack_prices_path,
            "rates_path": rates_path,
            "simple_config_path": simple_config_path,
            "growth_config_path": growth_config_path,
            "controlled_growth_config_path": controlled_growth_config_path,
            "layer1_config_path": layer1_config_path,
            "qqq_plus_config_path": qqq_plus_config_path,
            "policy_registry_path": policy_registry_path,
            "output_root": output_root,
            "strategy_id": strategy_id,
            "execution_policy_id": execution_policy_id,
            "as_of_date": _parse_optional_date(as_of),
            "start_date": _parse_optional_date(start_date) or date(2022, 12, 1),
            "end_date": _parse_optional_date(end_date),
        }
        if docs_path is not None:
            kwargs["docs_path"] = docs_path
        payload = _call_builder(builder, kwargs)
        _print_execution_semantics_payload(label, payload)

    command.__name__ = f"strategies_{label.lower().replace(' ', '_')}_command"
    return command


def _call_builder(
    builder: Callable[..., dict[str, object]],
    kwargs: dict[str, object],
) -> dict[str, object]:
    accepted = set(inspect.signature(builder).parameters)
    return builder(**{key: value for key, value in kwargs.items() if key in accepted})


def _print_execution_semantics_payload(label: str, payload: dict[str, object]) -> None:
    status = str(payload.get("status"))
    style = "green" if "READY" in status or "PASS" in status or "SAFE" in status else "yellow"
    if "BLOCKED" in status or "FAIL" in status:
        style = "red"
    console.print(f"[{style}]{label}：{status}[/{style}]")
    paths = payload.get("artifact_paths")
    if isinstance(paths, dict):
        console.print(f"JSON：{paths.get('json_path')}")
        console.print(f"Markdown：{paths.get('markdown_path')}")
    for field, expected in (
        ("paper_shadow_allowed", False),
        ("production_allowed", False),
        ("broker_action", "none"),
        ("manual_review_required", True),
        ("production_effect", "none"),
    ):
        console.print(f"{field}={payload.get(field, expected)}")


def _parse_optional_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


_EXECUTION_SEMANTICS_COMMANDS: tuple[
    tuple[str, Callable[..., dict[str, object]], str],
    ...,
] = (
    (
        "dynamic-strategy-execution-semantics-contract",
        run_dynamic_strategy_execution_semantics_contract,
        "Dynamic strategy execution semantics contract",
    ),
    (
        "implicit-monthly-rebalance-assumption-audit",
        run_implicit_monthly_rebalance_assumption_audit,
        "Implicit monthly rebalance assumption audit",
    ),
    (
        "strategy-execution-policy-registry-review",
        run_strategy_execution_policy_registry_review,
        "Strategy execution policy registry review",
    ),
    (
        "dynamic-strategy-validity-period-audit",
        run_dynamic_strategy_validity_period_audit,
        "Dynamic strategy validity period audit",
    ),
    (
        "target-vs-actual-position-path-builder",
        run_target_vs_actual_position_path_builder,
        "Target vs actual position path builder",
    ),
    (
        "rebalance-frequency-sensitivity-suite",
        run_rebalance_frequency_sensitivity_suite,
        "Rebalance frequency sensitivity suite",
    ),
    (
        "threshold-hybrid-rebalance-review",
        run_threshold_hybrid_rebalance_review,
        "Threshold hybrid rebalance review",
    ),
    (
        "signal-staleness-cost-review",
        run_signal_staleness_cost_review,
        "Signal staleness cost review",
    ),
    (
        "dynamic-strategy-latency-execution-lag-review",
        run_dynamic_strategy_latency_execution_lag_review,
        "Dynamic strategy latency execution lag review",
    ),
    (
        "execution-policy-impact-on-prior-conclusions",
        run_execution_policy_impact_on_prior_conclusions,
        "Execution policy impact on prior conclusions",
    ),
    (
        "rebalance-sensitive-candidate-recovery-review",
        run_rebalance_sensitive_candidate_recovery_review,
        "Rebalance sensitive candidate recovery review",
    ),
    (
        "execution-semantics-data-lineage-audit",
        run_execution_semantics_data_lineage_audit,
        "Execution semantics data lineage audit",
    ),
    (
        "execution-policy-cost-turnover-normalization",
        run_execution_policy_cost_turnover_normalization,
        "Execution policy cost turnover normalization",
    ),
    (
        "execution-semantics-external-validation-update",
        run_execution_semantics_external_validation_update,
        "Execution semantics external validation update",
    ),
    (
        "execution-aware-forward-aging-observation-contract",
        run_execution_aware_forward_aging_observation_contract,
        "Execution aware forward aging observation contract",
    ),
    (
        "equal-risk-balanced-core-execution-policy-selection",
        run_equal_risk_balanced_core_execution_policy_selection,
        "Equal risk balanced core execution policy selection",
    ),
    (
        "dynamic-backtest-engine-contract-update",
        run_dynamic_backtest_engine_contract_update,
        "Dynamic backtest engine contract update",
    ),
    (
        "execution-semantics-reporting-update",
        run_execution_semantics_reporting_update,
        "Execution semantics reporting update",
    ),
    (
        "rebalance-assumption-owner-review-pack",
        run_rebalance_assumption_owner_review_pack,
        "Rebalance assumption owner review pack",
    ),
    (
        "execution-semantics-master-review",
        run_execution_semantics_master_review,
        "Execution semantics master review",
    ),
    (
        "roadmap-update-after-execution-semantics-review",
        run_roadmap_update_after_execution_semantics_review,
        "Roadmap update after execution semantics review",
    ),
    (
        "reader-brief-execution-semantics-safe-preview",
        run_reader_brief_execution_semantics_safe_preview,
        "Reader Brief execution semantics safe preview",
    ),
)
