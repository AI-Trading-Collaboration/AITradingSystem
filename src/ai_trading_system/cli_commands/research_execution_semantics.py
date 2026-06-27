from __future__ import annotations

import inspect
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.execution_semantics import (
    DEFAULT_ACTUAL_PATH_EDGE_ATTRIBUTION_MATRIX_YAML_PATH,
    DEFAULT_ACTUAL_PATH_EDGE_ATTRIBUTION_REVIEW_PATH,
    DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    DEFAULT_DYNAMIC_OWNER_REVIEW_DECISION_DOC_PATH,
    DEFAULT_DYNAMIC_OWNER_REVIEW_DECISION_YAML_PATH,
    DEFAULT_DYNAMIC_POLICY_SENSITIVITY_DOC_PATH,
    DEFAULT_DYNAMIC_POLICY_SENSITIVITY_YAML_PATH,
    DEFAULT_DYNAMIC_PROMOTION_GATE_V2_PATH,
    DEFAULT_DYNAMIC_STRATEGY_OBJECTIVE_GATE_MATRIX_YAML_PATH,
    DEFAULT_DYNAMIC_STRATEGY_OBJECTIVE_GATE_REVIEW_PATH,
    DEFAULT_DYNAMIC_STRATEGY_OBJECTIVES_PATH,
    DEFAULT_DYNAMIC_STRATEGY_WALK_FORWARD_MATRIX_YAML_PATH,
    DEFAULT_DYNAMIC_STRATEGY_WALK_FORWARD_REVIEW_PATH,
    DEFAULT_DYNAMIC_WALK_FORWARD_POLICY_PATH,
    DEFAULT_EDGE_ATTRIBUTION_OUTPUT_ROOT,
    DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    DEFAULT_EVENT_OVERRIDE_EX_ANTE_TAXONOMY_CONFIG_PATH,
    DEFAULT_EVENT_OVERRIDE_EX_ANTE_TAXONOMY_REVIEW_PATH,
    DEFAULT_EVENT_OVERRIDE_EX_ANTE_TAXONOMY_SNAPSHOT_PATH,
    DEFAULT_EVENT_OVERRIDE_EXECUTION_SEMANTICS_REVIEW_PATH,
    DEFAULT_EVENT_OVERRIDE_POLICY_PATH,
    DEFAULT_EVENT_OVERRIDE_SURVIVAL_MATRIX_YAML_PATH,
    DEFAULT_EVENT_TAXONOMY_OUTPUT_ROOT,
    DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    DEFAULT_EXECUTION_REBACKTEST_STRATEGY_IDS,
    DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    DEFAULT_LAYER1_SELECTOR_CONFIG_PATH,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PIT_AUDIT_OUTPUT_ROOT,
    DEFAULT_PIT_DATA_AVAILABILITY_AUDIT_REVIEW_PATH,
    DEFAULT_PIT_DATA_AVAILABILITY_INVENTORY_PATH,
    DEFAULT_POLICY_SENSITIVITY_OUTPUT_ROOT,
    DEFAULT_PRICES_PATH,
    DEFAULT_QQQ_PLUS_GROWTH_CONFIG_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_RISK_TIMING_QUALITY_MATRIX_YAML_PATH,
    DEFAULT_RISK_TIMING_QUALITY_POLICY_PATH,
    DEFAULT_RISK_TIMING_QUALITY_REVIEW_PATH,
    DEFAULT_SIGNAL_VALIDITY_STALENESS_INPUT_SUMMARY_PATH,
    DEFAULT_SIGNAL_VALIDITY_STALENESS_REPAIR_REVIEW_PATH,
    DEFAULT_SIGNAL_VALIDITY_TAXONOMY_PATH,
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    DEFAULT_STALENESS_REPAIR_MATRIX_YAML_PATH,
    DEFAULT_TIMING_QUALITY_OUTPUT_ROOT,
    DEFAULT_WALK_FORWARD_OUTPUT_ROOT,
    EVENT_OVERRIDE_MODE_T_PLUS_1,
    run_actual_path_edge_attribution_review,
    run_dynamic_actual_path_owner_review_decision,
    run_dynamic_actual_path_policy_sensitivity_review,
    run_dynamic_backtest_engine_contract_update,
    run_dynamic_strategy_execution_semantics_contract,
    run_dynamic_strategy_latency_execution_lag_review,
    run_dynamic_strategy_objective_gate_review,
    run_dynamic_strategy_validity_period_audit,
    run_dynamic_strategy_walk_forward_validation,
    run_equal_risk_balanced_core_execution_policy_selection,
    run_event_override_ex_ante_taxonomy_review,
    run_execution_aware_forward_aging_observation_contract,
    run_execution_policy_cost_turnover_normalization,
    run_execution_policy_impact_on_prior_conclusions,
    run_execution_semantics_data_lineage_audit,
    run_execution_semantics_external_validation_update,
    run_execution_semantics_master_review,
    run_execution_semantics_rebacktest,
    run_execution_semantics_rebacktest_gate,
    run_execution_semantics_reporting_update,
    run_implicit_monthly_rebalance_assumption_audit,
    run_pit_data_availability_audit,
    run_reader_brief_execution_semantics_safe_preview,
    run_rebalance_assumption_owner_review_pack,
    run_rebalance_frequency_sensitivity_suite,
    run_rebalance_sensitive_candidate_recovery_review,
    run_risk_timing_quality_review,
    run_roadmap_update_after_execution_semantics_review,
    run_signal_staleness_cost_review,
    run_strategy_execution_policy_registry_review,
    run_target_vs_actual_position_path_builder,
    run_threshold_hybrid_rebalance_review,
)

console = Console()


def register_execution_semantics_strategy_commands(strategies_app: typer.Typer) -> None:
    strategies_app.command("execution-semantics-rebacktest")(
        _execution_semantics_rebacktest_command
    )
    strategies_app.command("dynamic-actual-path-owner-review-decision")(
        _dynamic_actual_path_owner_review_decision_command
    )
    strategies_app.command("dynamic-actual-path-policy-sensitivity-review")(
        _dynamic_actual_path_policy_sensitivity_review_command
    )
    strategies_app.command("actual-path-edge-attribution")(
        _actual_path_edge_attribution_command
    )
    strategies_app.command("dynamic-strategy-objective-gate-review")(
        _dynamic_strategy_objective_gate_review_command
    )
    strategies_app.command("pit-data-availability-audit")(
        _pit_data_availability_audit_command
    )
    strategies_app.command("dynamic-strategy-walk-forward-validation")(
        _dynamic_strategy_walk_forward_validation_command
    )
    strategies_app.command("event-override-ex-ante-taxonomy-review")(
        _event_override_ex_ante_taxonomy_review_command
    )
    strategies_app.command("risk-timing-quality-review")(
        _risk_timing_quality_review_command
    )
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


def _execution_semantics_rebacktest_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    signal_validity_taxonomy_path: Annotated[
        Path, typer.Option("--signal-validity-taxonomy")
    ] = DEFAULT_SIGNAL_VALIDITY_TAXONOMY_PATH,
    event_override_policy_path: Annotated[
        Path, typer.Option("--event-override-policy")
    ] = DEFAULT_EVENT_OVERRIDE_POLICY_PATH,
    output_root: Annotated[
        Path, typer.Option("--output", "--output-root")
    ] = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    strategy: Annotated[
        list[str] | None,
        typer.Option("--strategy", help="Strategy id; may be repeated."),
    ] = None,
    execution_policy_id: Annotated[
        str | None,
        typer.Option("--execution-policy-id"),
    ] = None,
    enable_staleness_filter: Annotated[
        bool,
        typer.Option("--enable-staleness-filter"),
    ] = False,
    stale_action: Annotated[
        str | None,
        typer.Option(
            "--stale-action",
            help=(
                "Override stale action: suppress_rebalance, hold_previous_position, "
                "fallback_to_static_baseline, or no_trade."
            ),
        ),
    ] = None,
    include_repaired_watch_only: Annotated[
        bool,
        typer.Option("--include-repaired-watch-only"),
    ] = False,
    emit_staleness_decomposition: Annotated[
        bool,
        typer.Option("--emit-staleness-decomposition"),
    ] = False,
    emit_lag_decomposition: Annotated[
        bool,
        typer.Option("--emit-lag-decomposition"),
    ] = False,
    staleness_input_summary_path: Annotated[
        Path,
        typer.Option("--staleness-input-summary-path"),
    ] = DEFAULT_SIGNAL_VALIDITY_STALENESS_INPUT_SUMMARY_PATH,
    staleness_repair_matrix_path: Annotated[
        Path,
        typer.Option("--staleness-repair-matrix-path"),
    ] = DEFAULT_STALENESS_REPAIR_MATRIX_YAML_PATH,
    staleness_repair_review_path: Annotated[
        Path,
        typer.Option("--staleness-repair-review-path"),
    ] = DEFAULT_SIGNAL_VALIDITY_STALENESS_REPAIR_REVIEW_PATH,
    enable_event_override: Annotated[
        bool,
        typer.Option("--enable-event-override"),
    ] = False,
    event_override_mode: Annotated[
        str,
        typer.Option("--event-override-mode"),
    ] = EVENT_OVERRIDE_MODE_T_PLUS_1,
    emit_pending_plan_ledger: Annotated[
        bool,
        typer.Option("--emit-pending-plan-ledger"),
    ] = False,
    emit_supersede_log: Annotated[
        bool,
        typer.Option("--emit-supersede-log"),
    ] = False,
    emit_event_override_trace: Annotated[
        bool,
        typer.Option("--emit-event-override-trace"),
    ] = False,
    event_override_survival_matrix_path: Annotated[
        Path,
        typer.Option("--event-override-survival-matrix-path"),
    ] = DEFAULT_EVENT_OVERRIDE_SURVIVAL_MATRIX_YAML_PATH,
    event_override_review_path: Annotated[
        Path,
        typer.Option("--event-override-review-path"),
    ] = DEFAULT_EVENT_OVERRIDE_EXECUTION_SEMANTICS_REVIEW_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_execution_semantics_rebacktest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        signal_validity_taxonomy_path=signal_validity_taxonomy_path,
        event_override_policy_path=event_override_policy_path,
        output_root=output_root,
        strategy_ids=strategy or list(DEFAULT_EXECUTION_REBACKTEST_STRATEGY_IDS),
        execution_policy_id=execution_policy_id,
        as_of_date=_parse_optional_date(as_of),
        start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
        end_date=_parse_optional_date(end_date),
        enable_staleness_filter=enable_staleness_filter,
        stale_action=stale_action,
        include_repaired_watch_only=include_repaired_watch_only,
        emit_staleness_decomposition=emit_staleness_decomposition,
        emit_lag_decomposition=emit_lag_decomposition,
        staleness_input_summary_path=staleness_input_summary_path,
        staleness_repair_matrix_path=staleness_repair_matrix_path,
        staleness_repair_review_path=staleness_repair_review_path,
        enable_event_override=enable_event_override,
        event_override_mode=event_override_mode,
        emit_pending_plan_ledger=emit_pending_plan_ledger,
        emit_supersede_log=emit_supersede_log,
        emit_event_override_trace=emit_event_override_trace,
        event_override_survival_matrix_path=event_override_survival_matrix_path,
        event_override_review_path=event_override_review_path,
    )
    _print_execution_semantics_payload("Execution semantics rebacktest", payload)


def _dynamic_actual_path_owner_review_decision_command(
    output_root: Annotated[
        Path, typer.Option("--source-root", "--output-root")
    ] = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    docs_path: Annotated[
        Path, typer.Option("--docs-path")
    ] = DEFAULT_DYNAMIC_OWNER_REVIEW_DECISION_DOC_PATH,
    yaml_path: Annotated[
        Path, typer.Option("--yaml-path")
    ] = DEFAULT_DYNAMIC_OWNER_REVIEW_DECISION_YAML_PATH,
) -> None:
    payload = run_dynamic_actual_path_owner_review_decision(
        output_root=output_root,
        docs_path=docs_path,
        yaml_path=yaml_path,
    )
    _print_execution_semantics_payload("Dynamic actual-path owner review decision", payload)


def _dynamic_actual_path_policy_sensitivity_review_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_POLICY_SENSITIVITY_OUTPUT_ROOT,
    docs_path: Annotated[
        Path, typer.Option("--docs-path")
    ] = DEFAULT_DYNAMIC_POLICY_SENSITIVITY_DOC_PATH,
    yaml_path: Annotated[
        Path, typer.Option("--yaml-path")
    ] = DEFAULT_DYNAMIC_POLICY_SENSITIVITY_YAML_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_dynamic_actual_path_policy_sensitivity_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        output_root=output_root,
        docs_path=docs_path,
        yaml_path=yaml_path,
        as_of_date=_parse_optional_date(as_of),
        start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
        end_date=_parse_optional_date(end_date),
    )
    _print_execution_semantics_payload("Dynamic actual-path policy sensitivity", payload)


def _actual_path_edge_attribution_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    objective_config_path: Annotated[
        Path, typer.Option("--objective-config", "--objectives-path")
    ] = DEFAULT_DYNAMIC_STRATEGY_OBJECTIVES_PATH,
    source_root: Annotated[
        Path, typer.Option("--source-root", "--execution-root")
    ] = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_EDGE_ATTRIBUTION_OUTPUT_ROOT,
    run_id: Annotated[
        str | None, typer.Option("--run-id")
    ] = None,
    docs_path: Annotated[
        Path, typer.Option("--docs-path", "--review-path")
    ] = DEFAULT_ACTUAL_PATH_EDGE_ATTRIBUTION_REVIEW_PATH,
    yaml_path: Annotated[
        Path, typer.Option("--yaml-path", "--matrix-path")
    ] = DEFAULT_ACTUAL_PATH_EDGE_ATTRIBUTION_MATRIX_YAML_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_actual_path_edge_attribution_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        output_root=output_root,
        run_id=run_id,
        source_root=source_root,
        objective_config_path=objective_config_path,
        docs_path=docs_path,
        yaml_path=yaml_path,
        as_of_date=_parse_optional_date(as_of),
        start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
        end_date=_parse_optional_date(end_date),
    )
    _print_execution_semantics_payload("Actual-path edge attribution", payload)


def _dynamic_strategy_objective_gate_review_command(
    edge_matrix_path: Annotated[
        Path, typer.Option("--edge-matrix", "--edge-matrix-path")
    ] = DEFAULT_ACTUAL_PATH_EDGE_ATTRIBUTION_MATRIX_YAML_PATH,
    objectives_path: Annotated[
        Path, typer.Option("--objective-config", "--objectives-path")
    ] = DEFAULT_DYNAMIC_STRATEGY_OBJECTIVES_PATH,
    promotion_gate_path: Annotated[
        Path, typer.Option("--gate-config", "--promotion-gate-path")
    ] = DEFAULT_DYNAMIC_PROMOTION_GATE_V2_PATH,
    docs_path: Annotated[
        Path, typer.Option("--review-path", "--docs-path")
    ] = DEFAULT_DYNAMIC_STRATEGY_OBJECTIVE_GATE_REVIEW_PATH,
    yaml_path: Annotated[
        Path, typer.Option("--matrix-path", "--yaml-path")
    ] = DEFAULT_DYNAMIC_STRATEGY_OBJECTIVE_GATE_MATRIX_YAML_PATH,
) -> None:
    payload = run_dynamic_strategy_objective_gate_review(
        edge_matrix_path=edge_matrix_path,
        objectives_path=objectives_path,
        promotion_gate_path=promotion_gate_path,
        docs_path=docs_path,
        yaml_path=yaml_path,
    )
    _print_execution_semantics_payload("Dynamic strategy objective gate review", payload)


def _pit_data_availability_audit_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    signal_validity_taxonomy_path: Annotated[
        Path, typer.Option("--signal-validity-taxonomy")
    ] = DEFAULT_SIGNAL_VALIDITY_TAXONOMY_PATH,
    event_override_policy_path: Annotated[
        Path, typer.Option("--event-override-policy")
    ] = DEFAULT_EVENT_OVERRIDE_POLICY_PATH,
    source_root: Annotated[
        Path, typer.Option("--source-root", "--execution-root")
    ] = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_PIT_AUDIT_OUTPUT_ROOT,
    run_id: Annotated[str | None, typer.Option("--run-id")] = None,
    docs_path: Annotated[
        Path, typer.Option("--docs-path", "--review-path")
    ] = DEFAULT_PIT_DATA_AVAILABILITY_AUDIT_REVIEW_PATH,
    inventory_path: Annotated[
        Path, typer.Option("--inventory-path", "--yaml-path")
    ] = DEFAULT_PIT_DATA_AVAILABILITY_INVENTORY_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_pit_data_availability_audit(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        signal_validity_taxonomy_path=signal_validity_taxonomy_path,
        event_override_policy_path=event_override_policy_path,
        source_root=source_root,
        output_root=output_root,
        run_id=run_id,
        docs_path=docs_path,
        inventory_path=inventory_path,
        as_of_date=_parse_optional_date(as_of),
        start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
        end_date=_parse_optional_date(end_date),
    )
    _print_execution_semantics_payload("PIT data availability audit", payload)


def _dynamic_strategy_walk_forward_validation_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    walk_forward_policy_path: Annotated[
        Path, typer.Option("--walk-forward-policy")
    ] = DEFAULT_DYNAMIC_WALK_FORWARD_POLICY_PATH,
    edge_matrix_path: Annotated[
        Path, typer.Option("--edge-matrix", "--edge-matrix-path")
    ] = DEFAULT_ACTUAL_PATH_EDGE_ATTRIBUTION_MATRIX_YAML_PATH,
    source_root: Annotated[
        Path, typer.Option("--source-root", "--execution-root")
    ] = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_WALK_FORWARD_OUTPUT_ROOT,
    run_id: Annotated[str | None, typer.Option("--run-id")] = None,
    docs_path: Annotated[
        Path, typer.Option("--docs-path", "--review-path")
    ] = DEFAULT_DYNAMIC_STRATEGY_WALK_FORWARD_REVIEW_PATH,
    yaml_path: Annotated[
        Path, typer.Option("--yaml-path", "--matrix-path")
    ] = DEFAULT_DYNAMIC_STRATEGY_WALK_FORWARD_MATRIX_YAML_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_dynamic_strategy_walk_forward_validation(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        walk_forward_policy_path=walk_forward_policy_path,
        edge_matrix_path=edge_matrix_path,
        source_root=source_root,
        output_root=output_root,
        run_id=run_id,
        docs_path=docs_path,
        yaml_path=yaml_path,
        as_of_date=_parse_optional_date(as_of),
        start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
        end_date=_parse_optional_date(end_date),
    )
    _print_execution_semantics_payload("Dynamic strategy walk-forward validation", payload)


def _event_override_ex_ante_taxonomy_review_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    event_override_policy_path: Annotated[
        Path, typer.Option("--event-override-policy")
    ] = DEFAULT_EVENT_OVERRIDE_POLICY_PATH,
    taxonomy_config_path: Annotated[
        Path, typer.Option("--taxonomy-config", "--taxonomy-policy")
    ] = DEFAULT_EVENT_OVERRIDE_EX_ANTE_TAXONOMY_CONFIG_PATH,
    source_root: Annotated[
        Path, typer.Option("--source-root", "--execution-root")
    ] = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_EVENT_TAXONOMY_OUTPUT_ROOT,
    run_id: Annotated[str | None, typer.Option("--run-id")] = None,
    docs_path: Annotated[
        Path, typer.Option("--docs-path", "--review-path")
    ] = DEFAULT_EVENT_OVERRIDE_EX_ANTE_TAXONOMY_REVIEW_PATH,
    yaml_path: Annotated[
        Path, typer.Option("--yaml-path", "--snapshot-path")
    ] = DEFAULT_EVENT_OVERRIDE_EX_ANTE_TAXONOMY_SNAPSHOT_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_event_override_ex_ante_taxonomy_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        event_override_policy_path=event_override_policy_path,
        taxonomy_config_path=taxonomy_config_path,
        source_root=source_root,
        output_root=output_root,
        run_id=run_id,
        docs_path=docs_path,
        yaml_path=yaml_path,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload("Event override ex-ante taxonomy review", payload)


def _risk_timing_quality_review_command(
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    simple_config_path: Annotated[
        Path, typer.Option("--simple-config")
    ] = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    policy_registry_path: Annotated[
        Path, typer.Option("--policy-registry")
    ] = DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    timing_policy_path: Annotated[
        Path, typer.Option("--timing-policy")
    ] = DEFAULT_RISK_TIMING_QUALITY_POLICY_PATH,
    source_root: Annotated[
        Path, typer.Option("--source-root", "--execution-root")
    ] = DEFAULT_EXECUTION_SEMANTICS_OUTPUT_ROOT,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_TIMING_QUALITY_OUTPUT_ROOT,
    run_id: Annotated[str | None, typer.Option("--run-id")] = None,
    docs_path: Annotated[
        Path, typer.Option("--docs-path", "--review-path")
    ] = DEFAULT_RISK_TIMING_QUALITY_REVIEW_PATH,
    yaml_path: Annotated[
        Path, typer.Option("--yaml-path", "--matrix-path")
    ] = DEFAULT_RISK_TIMING_QUALITY_MATRIX_YAML_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
    start_date: Annotated[str | None, typer.Option("--start-date")] = None,
    end_date: Annotated[str | None, typer.Option("--end-date")] = None,
) -> None:
    payload = run_risk_timing_quality_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        policy_registry_path=policy_registry_path,
        timing_policy_path=timing_policy_path,
        source_root=source_root,
        output_root=output_root,
        run_id=run_id,
        docs_path=docs_path,
        yaml_path=yaml_path,
        as_of_date=_parse_optional_date(as_of),
        start_date=_parse_optional_date(start_date) or date(2022, 12, 1),
        end_date=_parse_optional_date(end_date),
    )
    _print_execution_semantics_payload("Risk-off risk-on timing quality review", payload)


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
        if paths.get("json_path"):
            console.print(f"JSON：{paths.get('json_path')}")
        if paths.get("index"):
            console.print(f"Index：{paths.get('index')}")
        if paths.get("markdown_path"):
            console.print(f"Markdown：{paths.get('markdown_path')}")
        if paths.get("review_markdown"):
            console.print(f"Markdown：{paths.get('review_markdown')}")
        if paths.get("yaml_path"):
            console.print(f"YAML：{paths.get('yaml_path')}")
        if paths.get("review_yaml"):
            console.print(f"YAML：{paths.get('review_yaml')}")
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
        "execution-semantics-rebacktest-gate",
        run_execution_semantics_rebacktest_gate,
        "Execution semantics rebacktest gate",
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
