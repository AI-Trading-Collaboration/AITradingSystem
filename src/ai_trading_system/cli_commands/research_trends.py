from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.channel_specific_first_layer_v3 import (
    DEFAULT_ACTION_VALUE_MATRIX_PATH as DEFAULT_CHANNEL_V3_ACTION_VALUE_MATRIX_PATH,
)
from ai_trading_system.channel_specific_first_layer_v3 import (
    DEFAULT_CHANNEL_V3_CONFIG_PATH,
    DEFAULT_CHANNEL_V3_OUTPUT_ROOT,
    DEFAULT_DO_NOT_DERISK_SELECTION_RULE_PATH,
    DEFAULT_FEATURE_SET_LOCKED_PATH,
    DEFAULT_FEATURE_SET_PATH,
    DEFAULT_RISK_ON_VETO_SELECTION_RULE_PATH,
    run_channel_specific_first_layer_v3_pack,
)
from ai_trading_system.channel_specific_first_layer_v3 import (
    DEFAULT_LABELS_PATH as DEFAULT_CHANNEL_V3_LABELS_PATH,
)
from ai_trading_system.channel_specific_first_layer_v3 import (
    DEFAULT_PIT_FEATURE_MATRIX_PATH as DEFAULT_CHANNEL_V3_PIT_FEATURE_MATRIX_PATH,
)
from ai_trading_system.defensive_preservation_lane import (
    DEFAULT_DEFENSIVE_ACTION_VALUE_POLICY_PATH,
    DEFAULT_DEFENSIVE_LABEL_TAXONOMY_PATH,
    DEFAULT_DEFENSIVE_LANE_POLICY_PATH,
    DEFAULT_LIMITED_ADJUSTMENT_REFERENCE_PATH,
    run_defensive_preservation_lane_pack,
)
from ai_trading_system.first_layer_defensive_regression_diagnosis import (
    run_first_layer_defensive_regression_diagnosis_pack,
)
from ai_trading_system.first_layer_policy_calibration import (
    DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_PROBE_REGISTRY_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
    DEFAULT_SCOPE_CONFIG_PATH,
    DEFAULT_SCORE_POLICY_PATH,
    DEFAULT_SCORECARD_CONFIG_PATH,
    run_first_layer_policy_aware_calibration_pack,
)
from ai_trading_system.first_layer_up_state_learning import (
    DEFAULT_HIERARCHICAL_CONFIG_PATH,
    DEFAULT_THRESHOLD_POLICY_PATH,
    run_first_layer_up_state_learning_repair_pack,
)
from ai_trading_system.first_layer_walk_forward_coverage import (
    DEFAULT_2022_SLICE_YAML_PATH,
    DEFAULT_ACTUAL_PATH_YAML_PATH,
    DEFAULT_COVERAGE_MODEL_ROOT,
    DEFAULT_COVERAGE_POLICY_PATH,
    DEFAULT_COVERAGE_SELECTION_RULE_PATH,
    DEFAULT_COVERAGE_SIMULATION_YAML_PATH,
    DEFAULT_FAILURE_YAML_PATH,
    DEFAULT_FEATURE_OPTIONALIZATION_POLICY_PATH,
    DEFAULT_FINAL_MATRIX_YAML_PATH,
    DEFAULT_MODEL_MATRIX_YAML_PATH,
    run_first_layer_walk_forward_coverage_rebuild_pack,
)
from ai_trading_system.indicator_family_ablation import (
    DEFAULT_ACTION_VALUE_MATRIX_PATH,
    DEFAULT_ACTION_VALUE_SUMMARY_PATH,
    DEFAULT_INDICATOR_FAMILY_ABLATION_MATRIX_PATH,
    DEFAULT_INDICATOR_FAMILY_ABLATION_OUTPUT_ROOT,
    DEFAULT_INDICATOR_FAMILY_ABLATION_REVIEW_PATH,
    DEFAULT_INDICATOR_FAMILY_REGISTRY_PATH,
    DEFAULT_INDICATOR_FAMILY_SELECTION_RULE_PATH,
    DEFAULT_LABELS_PATH,
    DEFAULT_PIT_FEATURE_MATRIX_PATH,
    run_indicator_family_ablation,
)
from ai_trading_system.research_window_extension import (
    DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    DEFAULT_WINDOW_AWARE_WALK_FORWARD_POLICY_PATH,
    DEFAULT_WINDOWED_ACTUAL_PATH_ROOT,
    DEFAULT_WINDOWED_STATIC_FRONTIER_ROOT,
    run_research_window_extension_validation_pack,
)
from ai_trading_system.return_seeking_diagnostic_lane import (
    DEFAULT_RETURN_SEEKING_POLICY_PATH,
    run_return_seeking_diagnostic_lane_pack,
)
from ai_trading_system.second_layer_probe_library_freeze import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_SECOND_LAYER_PROBE_OUTPUT_ROOT,
)
from ai_trading_system.second_layer_probe_library_freeze import (
    DEFAULT_PREDICTIONS_PATH as DEFAULT_SECOND_LAYER_FROZEN_PREDICTIONS_PATH,
)
from ai_trading_system.second_layer_probe_library_freeze import (
    DEFAULT_PROBE_REGISTRY_V2_PATH,
    run_second_layer_probe_library_freeze_pack,
)
from ai_trading_system.two_layer_policy_compiler import (
    DEFAULT_POLICY_SCHEMA_PATH,
    DEFAULT_SIGNAL_USAGE_MATRIX_V2_PATH,
)
from ai_trading_system.upper_state_label_feature_reset import (
    DEFAULT_ACTION_VALUE_SCORE_POLICY_V2_PATH,
    DEFAULT_ALTERNATING_PROTOCOL_PATH,
    DEFAULT_COMPOSER_PREDICTIONS_PATH,
    DEFAULT_FIRST_LAYER_COMPOSER_V2_PATH,
    DEFAULT_FIRST_LAYER_THRESHOLD_POLICY_V2_PATH,
    DEFAULT_FIRST_LAYER_V2_PROBE_REGISTRY_PATH,
    DEFAULT_UPPER_STATE_TAXONOMY_V2_PATH,
    run_first_layer_v2_label_feature_model_reset_pack,
    run_upper_state_label_feature_reset_pack,
)

console = Console()
trends_app = typer.Typer(
    help="Policy-aware first-layer trend calibration research.",
    no_args_is_help=True,
)


@trends_app.command("full-pack")
def first_layer_policy_aware_full_pack_command(
    scope_config_path: Annotated[Path, typer.Option("--scope-config")] = DEFAULT_SCOPE_CONFIG_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_PROBE_REGISTRY_PATH,
    score_policy_path: Annotated[Path, typer.Option("--score-policy")] = DEFAULT_SCORE_POLICY_PATH,
    scorecard_config_path: Annotated[
        Path, typer.Option("--scorecard-config")
    ] = DEFAULT_SCORECARD_CONFIG_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_first_layer_policy_aware_calibration_pack(
        scope_config_path=scope_config_path,
        probe_registry_path=probe_registry_path,
        score_policy_path=score_policy_path,
        scorecard_config_path=scorecard_config_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_payload("First-layer policy-aware calibration", payload)


@trends_app.command("up-state-repair")
def first_layer_up_state_repair_command(
    scope_config_path: Annotated[Path, typer.Option("--scope-config")] = DEFAULT_SCOPE_CONFIG_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_PROBE_REGISTRY_PATH,
    score_policy_path: Annotated[Path, typer.Option("--score-policy")] = DEFAULT_SCORE_POLICY_PATH,
    scorecard_config_path: Annotated[
        Path, typer.Option("--scorecard-config")
    ] = DEFAULT_SCORECARD_CONFIG_PATH,
    threshold_policy_path: Annotated[
        Path, typer.Option("--threshold-policy")
    ] = DEFAULT_THRESHOLD_POLICY_PATH,
    hierarchical_config_path: Annotated[
        Path, typer.Option("--hierarchical-config")
    ] = DEFAULT_HIERARCHICAL_CONFIG_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
    refresh_prerequisites: Annotated[
        bool, typer.Option("--refresh-prerequisites/--no-refresh-prerequisites")
    ] = True,
) -> None:
    payload = run_first_layer_up_state_learning_repair_pack(
        scope_config_path=scope_config_path,
        probe_registry_path=probe_registry_path,
        score_policy_path=score_policy_path,
        scorecard_config_path=scorecard_config_path,
        threshold_policy_path=threshold_policy_path,
        hierarchical_config_path=hierarchical_config_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
        refresh_prerequisites=refresh_prerequisites,
    )
    _print_payload("First-layer up-state learning repair", payload)


@trends_app.command("window-extension")
def research_window_extension_command(
    registry_path: Annotated[
        Path, typer.Option("--registry")
    ] = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    walk_forward_policy_path: Annotated[
        Path, typer.Option("--walk-forward-policy")
    ] = DEFAULT_WINDOW_AWARE_WALK_FORWARD_POLICY_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_PROBE_REGISTRY_PATH,
    score_policy_path: Annotated[Path, typer.Option("--score-policy")] = DEFAULT_SCORE_POLICY_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    static_output_root: Annotated[
        Path, typer.Option("--static-output-root")
    ] = DEFAULT_WINDOWED_STATIC_FRONTIER_ROOT,
    actual_path_output_root: Annotated[
        Path, typer.Option("--actual-path-output-root")
    ] = DEFAULT_WINDOWED_ACTUAL_PATH_ROOT,
) -> None:
    payload = run_research_window_extension_validation_pack(
        registry_path=registry_path,
        walk_forward_policy_path=walk_forward_policy_path,
        probe_registry_path=probe_registry_path,
        score_policy_path=score_policy_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        static_output_root=static_output_root,
        actual_path_output_root=actual_path_output_root,
    )
    _print_payload("Research window extension validation", payload)


@trends_app.command("second-layer-probe-freeze")
def second_layer_probe_freeze_command(
    registry_path: Annotated[
        Path, typer.Option("--registry")
    ] = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_PROBE_REGISTRY_V2_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    predictions_path: Annotated[
        Path, typer.Option("--predictions-path")
    ] = DEFAULT_SECOND_LAYER_FROZEN_PREDICTIONS_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_SECOND_LAYER_PROBE_OUTPUT_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_second_layer_probe_library_freeze_pack(
        registry_path=registry_path,
        probe_registry_path=probe_registry_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        predictions_path=predictions_path,
        output_root=output_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_payload("Second-layer probe library freeze", payload)


@trends_app.command("upper-state-reset")
def upper_state_label_feature_reset_command(
    registry_path: Annotated[
        Path, typer.Option("--registry")
    ] = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    alternating_protocol_path: Annotated[
        Path, typer.Option("--alternating-protocol")
    ] = DEFAULT_ALTERNATING_PROTOCOL_PATH,
    upper_state_taxonomy_path: Annotated[
        Path, typer.Option("--upper-state-taxonomy")
    ] = DEFAULT_UPPER_STATE_TAXONOMY_V2_PATH,
    action_value_policy_path: Annotated[
        Path, typer.Option("--action-value-policy")
    ] = DEFAULT_ACTION_VALUE_SCORE_POLICY_V2_PATH,
    threshold_policy_path: Annotated[
        Path, typer.Option("--threshold-policy")
    ] = DEFAULT_FIRST_LAYER_THRESHOLD_POLICY_V2_PATH,
    composer_config_path: Annotated[
        Path, typer.Option("--composer-config")
    ] = DEFAULT_FIRST_LAYER_COMPOSER_V2_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_PROBE_REGISTRY_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
) -> None:
    payload = run_upper_state_label_feature_reset_pack(
        registry_path=registry_path,
        alternating_protocol_path=alternating_protocol_path,
        upper_state_taxonomy_path=upper_state_taxonomy_path,
        action_value_policy_path=action_value_policy_path,
        threshold_policy_path=threshold_policy_path,
        composer_config_path=composer_config_path,
        probe_registry_path=probe_registry_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
    )
    _print_payload("Upper-state label feature reset", payload)


@trends_app.command("first-layer-v2-reset")
def first_layer_v2_label_feature_model_reset_command(
    registry_path: Annotated[
        Path, typer.Option("--registry")
    ] = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    alternating_protocol_path: Annotated[
        Path, typer.Option("--alternating-protocol")
    ] = DEFAULT_ALTERNATING_PROTOCOL_PATH,
    upper_state_taxonomy_path: Annotated[
        Path, typer.Option("--upper-state-taxonomy")
    ] = DEFAULT_UPPER_STATE_TAXONOMY_V2_PATH,
    action_value_policy_path: Annotated[
        Path, typer.Option("--action-value-policy")
    ] = DEFAULT_ACTION_VALUE_SCORE_POLICY_V2_PATH,
    threshold_policy_path: Annotated[
        Path, typer.Option("--threshold-policy")
    ] = DEFAULT_FIRST_LAYER_THRESHOLD_POLICY_V2_PATH,
    composer_config_path: Annotated[
        Path, typer.Option("--composer-config")
    ] = DEFAULT_FIRST_LAYER_COMPOSER_V2_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_FIRST_LAYER_V2_PROBE_REGISTRY_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
) -> None:
    payload = run_first_layer_v2_label_feature_model_reset_pack(
        registry_path=registry_path,
        alternating_protocol_path=alternating_protocol_path,
        upper_state_taxonomy_path=upper_state_taxonomy_path,
        action_value_policy_path=action_value_policy_path,
        threshold_policy_path=threshold_policy_path,
        composer_config_path=composer_config_path,
        probe_registry_path=probe_registry_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
    )
    _print_payload("First-layer v2 label feature model reset", payload)


@trends_app.command("first-layer-coverage-rebuild")
def first_layer_walk_forward_coverage_rebuild_command(
    registry_path: Annotated[
        Path, typer.Option("--registry")
    ] = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    coverage_policy_path: Annotated[
        Path, typer.Option("--coverage-policy")
    ] = DEFAULT_COVERAGE_POLICY_PATH,
    feature_optionalization_policy_path: Annotated[
        Path, typer.Option("--feature-optionalization-policy")
    ] = DEFAULT_FEATURE_OPTIONALIZATION_POLICY_PATH,
    coverage_selection_rule_path: Annotated[
        Path, typer.Option("--coverage-selection-rule")
    ] = DEFAULT_COVERAGE_SELECTION_RULE_PATH,
    upper_state_taxonomy_path: Annotated[
        Path, typer.Option("--upper-state-taxonomy")
    ] = DEFAULT_UPPER_STATE_TAXONOMY_V2_PATH,
    action_value_policy_path: Annotated[
        Path, typer.Option("--action-value-policy")
    ] = DEFAULT_ACTION_VALUE_SCORE_POLICY_V2_PATH,
    threshold_policy_path: Annotated[
        Path, typer.Option("--threshold-policy")
    ] = DEFAULT_FIRST_LAYER_THRESHOLD_POLICY_V2_PATH,
    composer_config_path: Annotated[
        Path, typer.Option("--composer-config")
    ] = DEFAULT_FIRST_LAYER_COMPOSER_V2_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_FIRST_LAYER_V2_PROBE_REGISTRY_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
) -> None:
    payload = run_first_layer_walk_forward_coverage_rebuild_pack(
        registry_path=registry_path,
        coverage_policy_path=coverage_policy_path,
        feature_optionalization_policy_path=feature_optionalization_policy_path,
        coverage_selection_rule_path=coverage_selection_rule_path,
        upper_state_taxonomy_path=upper_state_taxonomy_path,
        action_value_policy_path=action_value_policy_path,
        threshold_policy_path=threshold_policy_path,
        composer_config_path=composer_config_path,
        probe_registry_path=probe_registry_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
    )
    _print_payload("First-layer walk-forward coverage rebuild", payload)


@trends_app.command("first-layer-defensive-regression-diagnosis")
def first_layer_defensive_regression_diagnosis_command(
    actual_path_path: Annotated[
        Path, typer.Option("--actual-path")
    ] = DEFAULT_ACTUAL_PATH_YAML_PATH,
    prior_slice_path: Annotated[
        Path, typer.Option("--prior-slice")
    ] = DEFAULT_2022_SLICE_YAML_PATH,
    coverage_final_path: Annotated[
        Path, typer.Option("--coverage-final")
    ] = DEFAULT_FINAL_MATRIX_YAML_PATH,
    coverage_failure_path: Annotated[
        Path, typer.Option("--coverage-failure")
    ] = DEFAULT_FAILURE_YAML_PATH,
    coverage_simulation_path: Annotated[
        Path, typer.Option("--coverage-simulation")
    ] = DEFAULT_COVERAGE_SIMULATION_YAML_PATH,
    coverage_model_matrix_path: Annotated[
        Path, typer.Option("--coverage-model-matrix")
    ] = DEFAULT_MODEL_MATRIX_YAML_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_FIRST_LAYER_V2_PROBE_REGISTRY_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    coverage_model_root: Annotated[
        Path, typer.Option("--coverage-model-root")
    ] = DEFAULT_COVERAGE_MODEL_ROOT,
) -> None:
    payload = run_first_layer_defensive_regression_diagnosis_pack(
        actual_path_path=actual_path_path,
        prior_slice_path=prior_slice_path,
        coverage_final_path=coverage_final_path,
        coverage_failure_path=coverage_failure_path,
        coverage_simulation_path=coverage_simulation_path,
        coverage_model_matrix_path=coverage_model_matrix_path,
        probe_registry_path=probe_registry_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        coverage_model_root=coverage_model_root,
    )
    _print_payload("First-layer defensive regression diagnosis", payload)


@trends_app.command("defensive-lane")
def defensive_preservation_lane_command(
    registry_path: Annotated[
        Path, typer.Option("--registry")
    ] = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    lane_policy_path: Annotated[
        Path, typer.Option("--lane-policy")
    ] = DEFAULT_DEFENSIVE_LANE_POLICY_PATH,
    label_taxonomy_path: Annotated[
        Path, typer.Option("--label-taxonomy")
    ] = DEFAULT_DEFENSIVE_LABEL_TAXONOMY_PATH,
    action_value_policy_path: Annotated[
        Path, typer.Option("--action-value-policy")
    ] = DEFAULT_DEFENSIVE_ACTION_VALUE_POLICY_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_FIRST_LAYER_V2_PROBE_REGISTRY_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_RESEARCH_TRENDS_OUTPUT_ROOT,
    limited_adjustment_reference_path: Annotated[
        Path, typer.Option("--limited-adjustment-reference")
    ] = DEFAULT_LIMITED_ADJUSTMENT_REFERENCE_PATH,
) -> None:
    payload = run_defensive_preservation_lane_pack(
        registry_path=registry_path,
        lane_policy_path=lane_policy_path,
        label_taxonomy_path=label_taxonomy_path,
        action_value_policy_path=action_value_policy_path,
        probe_registry_path=probe_registry_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
        limited_adjustment_reference_path=limited_adjustment_reference_path,
    )
    _print_payload("Defensive preservation lane", payload)


@trends_app.command("return-seeking-diagnostic-lane")
def return_seeking_diagnostic_lane_command(
    registry_path: Annotated[
        Path, typer.Option("--registry")
    ] = DEFAULT_RESEARCH_WINDOW_REGISTRY_PATH,
    lane_policy_path: Annotated[
        Path, typer.Option("--lane-policy")
    ] = DEFAULT_RETURN_SEEKING_POLICY_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_FIRST_LAYER_V2_PROBE_REGISTRY_PATH,
    composer_predictions_path: Annotated[
        Path, typer.Option("--composer-predictions")
    ] = DEFAULT_COMPOSER_PREDICTIONS_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
) -> None:
    payload = run_return_seeking_diagnostic_lane_pack(
        registry_path=registry_path,
        lane_policy_path=lane_policy_path,
        probe_registry_path=probe_registry_path,
        composer_predictions_path=composer_predictions_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
    )
    _print_payload("Return-seeking diagnostic lane", payload)


@trends_app.command("indicator-family-ablation")
def indicator_family_ablation_command(
    registry_path: Annotated[
        Path, typer.Option("--registry")
    ] = DEFAULT_INDICATOR_FAMILY_REGISTRY_PATH,
    selection_rule_path: Annotated[
        Path, typer.Option("--selection-rule")
    ] = DEFAULT_INDICATOR_FAMILY_SELECTION_RULE_PATH,
    pit_feature_matrix_path: Annotated[
        Path, typer.Option("--pit-feature-matrix")
    ] = DEFAULT_PIT_FEATURE_MATRIX_PATH,
    labels_path: Annotated[Path, typer.Option("--labels-path")] = DEFAULT_LABELS_PATH,
    action_value_matrix_path: Annotated[
        Path, typer.Option("--action-value-matrix")
    ] = DEFAULT_ACTION_VALUE_MATRIX_PATH,
    action_value_summary_path: Annotated[
        Path, typer.Option("--action-value-summary")
    ] = DEFAULT_ACTION_VALUE_SUMMARY_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_INDICATOR_FAMILY_ABLATION_OUTPUT_ROOT,
    matrix_path: Annotated[
        Path, typer.Option("--matrix-path")
    ] = DEFAULT_INDICATOR_FAMILY_ABLATION_MATRIX_PATH,
    review_path: Annotated[
        Path, typer.Option("--review-path")
    ] = DEFAULT_INDICATOR_FAMILY_ABLATION_REVIEW_PATH,
) -> None:
    payload = run_indicator_family_ablation(
        registry_path=registry_path,
        selection_rule_path=selection_rule_path,
        pit_feature_matrix_path=pit_feature_matrix_path,
        labels_path=labels_path,
        action_value_matrix_path=action_value_matrix_path,
        action_value_summary_path=action_value_summary_path,
        output_root=output_root,
        matrix_path=matrix_path,
        review_path=review_path,
    )
    _print_payload("Indicator family ablation", payload)


@trends_app.command("channel-specific-v3")
def channel_specific_first_layer_v3_command(
    feature_set_path: Annotated[
        Path, typer.Option("--feature-set")
    ] = DEFAULT_FEATURE_SET_PATH,
    feature_set_locked_path: Annotated[
        Path, typer.Option("--feature-set-locked")
    ] = DEFAULT_FEATURE_SET_LOCKED_PATH,
    do_not_selection_rule_path: Annotated[
        Path, typer.Option("--do-not-selection-rule")
    ] = DEFAULT_DO_NOT_DERISK_SELECTION_RULE_PATH,
    risk_veto_selection_rule_path: Annotated[
        Path, typer.Option("--risk-veto-selection-rule")
    ] = DEFAULT_RISK_ON_VETO_SELECTION_RULE_PATH,
    channel_config_path: Annotated[
        Path, typer.Option("--channel-config")
    ] = DEFAULT_CHANNEL_V3_CONFIG_PATH,
    pit_feature_matrix_path: Annotated[
        Path, typer.Option("--pit-feature-matrix")
    ] = DEFAULT_CHANNEL_V3_PIT_FEATURE_MATRIX_PATH,
    labels_path: Annotated[
        Path, typer.Option("--labels-path")
    ] = DEFAULT_CHANNEL_V3_LABELS_PATH,
    action_value_matrix_path: Annotated[
        Path, typer.Option("--action-value-matrix")
    ] = DEFAULT_CHANNEL_V3_ACTION_VALUE_MATRIX_PATH,
    probe_registry_path: Annotated[
        Path, typer.Option("--probe-registry")
    ] = DEFAULT_PROBE_REGISTRY_V2_PATH,
    composer_predictions_path: Annotated[
        Path, typer.Option("--composer-predictions")
    ] = DEFAULT_SECOND_LAYER_FROZEN_PREDICTIONS_PATH,
    policy_schema_path: Annotated[
        Path, typer.Option("--policy-schema")
    ] = DEFAULT_POLICY_SCHEMA_PATH,
    signal_usage_matrix_path: Annotated[
        Path, typer.Option("--signal-usage-matrix")
    ] = DEFAULT_SIGNAL_USAGE_MATRIX_V2_PATH,
    expanded_config_path: Annotated[
        Path, typer.Option("--expanded-config")
    ] = DEFAULT_EXPANDED_UNIVERSE_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path, typer.Option("--marketstack-prices-path")
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = DEFAULT_RATES_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = DEFAULT_CHANNEL_V3_OUTPUT_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = run_channel_specific_first_layer_v3_pack(
        feature_set_path=feature_set_path,
        feature_set_locked_path=feature_set_locked_path,
        do_not_selection_rule_path=do_not_selection_rule_path,
        risk_veto_selection_rule_path=risk_veto_selection_rule_path,
        channel_config_path=channel_config_path,
        pit_feature_matrix_path=pit_feature_matrix_path,
        labels_path=labels_path,
        action_value_matrix_path=action_value_matrix_path,
        probe_registry_path=probe_registry_path,
        composer_predictions_path=composer_predictions_path,
        policy_schema_path=policy_schema_path,
        signal_usage_matrix_path=signal_usage_matrix_path,
        expanded_config_path=expanded_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_payload("Channel-specific first-layer v3", payload)


def _print_payload(label: str, payload: dict[str, object]) -> None:
    status = str(payload.get("status"))
    style = "green" if "READY" in status or "CANDIDATE" in status else "yellow"
    if "BLOCKED" in status and "PROMOTION_BLOCKED" not in status:
        style = "red"
    console.print(f"[{style}]{label}: {status}[/{style}]")
    summary = payload.get("summary")
    if isinstance(summary, dict):
        compact = "; ".join(f"{key}={value}" for key, value in list(summary.items())[:8])
        if compact:
            console.print(compact)
    paths = payload.get("artifact_paths")
    if isinstance(paths, dict):
        for key, value in paths.items():
            console.print(f"{key}={value}")
    for field, expected in (
        ("promotion_allowed", False),
        ("paper_shadow_allowed", False),
        ("production_allowed", False),
        ("broker_action", "none"),
        ("dynamic_promotion_status", "BLOCKED"),
    ):
        console.print(f"{field}={payload.get(field, expected)}")
    if "BLOCKED" in status and "PROMOTION_BLOCKED" not in status:
        raise typer.Exit(code=1)


def _parse_optional_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("Date must use YYYY-MM-DD.") from exc
