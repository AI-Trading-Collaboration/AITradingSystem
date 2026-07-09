from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

import ai_trading_system.dynamic_strategy_blocking_gap_remediation_implementation_plan as m2408
import ai_trading_system.dynamic_strategy_growth_tilt_engine_contract_gap_remediation_plan as m2411
import ai_trading_system.dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan as m2406
import ai_trading_system.dynamic_strategy_signal_as_of_validity_contract_schema as m2409
import ai_trading_system.dynamic_strategy_valid_until_window_stale_signal_remediation_plan as m2407
from ai_trading_system import (
    dynamic_strategy_growth_tilt_candidate_gauntlet_harness as m2432,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_defensive_limited_adjustment_component_validation as m2434,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_as_of_semantics_remediation as m2412,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_candidate_promotion_evidence_review as m2430,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_contract_readiness_snapshot as m2422,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_forward_outcome_binding_boundary as m2429,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_manual_review_packet_dry_run as m2427,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_observe_only_signal_artifact_boundary as m2428,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_paper_shadow_dry_run_wiring as m2425,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_paper_shadow_enablement_plan as m2424,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_paper_shadow_preflight as m2423,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_paper_shadow_schedule_dry_run as m2426,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_readiness_recheck as m2419,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_remediation as m2421,  # noqa: E501
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_readiness_snapshot as m2415,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_remaining_blocker_closure_plan as m2416,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_signal_artifact_source_traceability_remediation as m2420,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_signal_validity_dependency_remediation as m2414,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_feature_contract_mapping as m2410,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_traceability_remediation as m2413,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_traceability_upstream_artifact_closure as m2417,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_valid_until_dependency_evidence_closure as m2418,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_existing_candidate_evidence_matrix as m2431,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_false_risk_off_missed_upside_batch_screen as m2433,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_forward_aging_candidate_pack as m2439,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_paper_shadow_candidate_promotion_review as m2440,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_persistent_candidate_pit_replay_blocker_escalation as m2438j,  # noqa: E501
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_pit_replay_engine_blocker_closure as m2438b,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_regime_slice_attribution_review as m2437,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_remaining_candidate_pit_replay_blocker_closure as m2438h,  # noqa: E501
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_level_pit_replay_blocker_closure as m2438f,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay as m2438,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_engine_remediation as m2438a,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck as m2438c,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_after_candidate_blocker_closure as m2438g,  # noqa: E501
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure as m2438e,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure as m2438i,  # noqa: E501
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure as m2438d,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_turnover_cooldown_parameter_plateau_study as m2436,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_valid_until_outcome_hit_rate_study as m2435,
)
from ai_trading_system.cli_commands.research_execution_common import (
    cli_scalar as _cli_scalar,
)
from ai_trading_system.cli_commands.research_execution_common import (
    console,
)
from ai_trading_system.cli_commands.research_execution_common import (
    parse_optional_date as _parse_optional_date,
)
from ai_trading_system.cli_commands.research_execution_common import (
    print_execution_semantics_payload as _print_execution_semantics_payload,
)


def register_growth_tilt_execution_strategy_commands(strategies_app: typer.Typer) -> None:
    strategies_app.command(
        "dynamic-strategy-growth-tilt-engine-pit-signal-remediation-plan"
    )(_dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan_command)
    strategies_app.command(
        "dynamic-strategy-valid-until-window-stale-signal-remediation-plan"
    )(_dynamic_strategy_valid_until_window_stale_signal_remediation_plan_command)
    strategies_app.command(
        "dynamic-strategy-blocking-gap-remediation-implementation-plan"
    )(_dynamic_strategy_blocking_gap_remediation_implementation_plan_command)
    strategies_app.command(
        "dynamic-strategy-signal-as-of-validity-contract-schema"
    )(_dynamic_strategy_signal_as_of_validity_contract_schema_command)
    strategies_app.command("growth-tilt-engine-source-feature-contract-mapping")(
        _growth_tilt_engine_source_feature_contract_mapping_command
    )
    strategies_app.command("growth-tilt-engine-contract-gap-remediation-plan")(
        _growth_tilt_engine_contract_gap_remediation_plan_command
    )
    strategies_app.command("growth-tilt-engine-as-of-semantics-remediation")(
        _growth_tilt_engine_as_of_semantics_remediation_command
    )
    strategies_app.command("growth-tilt-engine-source-traceability-remediation")(
        _growth_tilt_engine_source_traceability_remediation_command
    )
    strategies_app.command("growth-tilt-engine-signal-validity-dependency-remediation")(
        _growth_tilt_engine_signal_validity_dependency_remediation_command
    )
    strategies_app.command("growth-tilt-engine-pit-gate-readiness-snapshot")(
        _growth_tilt_engine_pit_gate_readiness_snapshot_command
    )
    strategies_app.command(
        "growth-tilt-engine-pit-gate-remaining-blocker-closure-plan"
    )(_growth_tilt_engine_pit_gate_remaining_blocker_closure_plan_command)
    strategies_app.command(
        "growth-tilt-engine-source-traceability-upstream-artifact-closure"
    )(_growth_tilt_engine_source_traceability_upstream_artifact_closure_command)
    strategies_app.command(
        "growth-tilt-engine-valid-until-dependency-evidence-closure"
    )(_growth_tilt_engine_valid_until_dependency_evidence_closure_command)
    strategies_app.command("growth-tilt-engine-pit-gate-readiness-recheck")(
        _growth_tilt_engine_pit_gate_readiness_recheck_command
    )
    strategies_app.command(
        "growth-tilt-engine-signal-artifact-source-traceability-remediation"
    )(_growth_tilt_engine_signal_artifact_source_traceability_remediation_command)
    strategies_app.command(
        "growth-tilt-engine-pit-gate-readiness-recheck-after-source-traceability-"
        "remediation"
    )(
        _growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_remediation_command
    )
    strategies_app.command("growth-tilt-engine-contract-readiness-snapshot")(
        _growth_tilt_engine_contract_readiness_snapshot_command
    )
    strategies_app.command("growth-tilt-engine-paper-shadow-preflight")(
        _growth_tilt_engine_paper_shadow_preflight_command
    )
    strategies_app.command("growth-tilt-engine-paper-shadow-enablement-plan")(
        _growth_tilt_engine_paper_shadow_enablement_plan_command
    )
    strategies_app.command("growth-tilt-engine-paper-shadow-dry-run-wiring")(
        _growth_tilt_engine_paper_shadow_dry_run_wiring_command
    )
    strategies_app.command("growth-tilt-engine-paper-shadow-schedule-dry-run")(
        _growth_tilt_engine_paper_shadow_schedule_dry_run_command
    )
    strategies_app.command("growth-tilt-engine-manual-review-packet-dry-run")(
        _growth_tilt_engine_manual_review_packet_dry_run_command
    )
    strategies_app.command("growth-tilt-engine-observe-only-signal-artifact-boundary")(
        _growth_tilt_engine_observe_only_signal_artifact_boundary_command
    )
    strategies_app.command("growth-tilt-engine-forward-outcome-binding-boundary")(
        _growth_tilt_engine_forward_outcome_binding_boundary_command
    )
    strategies_app.command("growth-tilt-engine-candidate-promotion-evidence-review")(
        _growth_tilt_engine_candidate_promotion_evidence_review_command
    )
    strategies_app.command("growth-tilt-existing-candidate-evidence-matrix")(
        _growth_tilt_existing_candidate_evidence_matrix_command
    )
    strategies_app.command("growth-tilt-candidate-gauntlet")(
        _growth_tilt_candidate_gauntlet_command
    )
    strategies_app.command("growth-tilt-false-risk-off-missed-upside-batch-screen")(
        _growth_tilt_false_risk_off_missed_upside_batch_screen_command
    )
    strategies_app.command(
        "growth-tilt-defensive-limited-adjustment-component-validation"
    )(_growth_tilt_defensive_limited_adjustment_component_validation_command)
    strategies_app.command("growth-tilt-valid-until-outcome-hit-rate-study")(
        _growth_tilt_valid_until_outcome_hit_rate_study_command
    )
    strategies_app.command("growth-tilt-turnover-cooldown-parameter-plateau-study")(
        _growth_tilt_turnover_cooldown_parameter_plateau_study_command
    )
    strategies_app.command("growth-tilt-regime-slice-attribution-review")(
        _growth_tilt_regime_slice_attribution_review_command
    )
    strategies_app.command("growth-tilt-top3-candidate-pit-replay")(
        _growth_tilt_top3_candidate_pit_replay_command
    )
    strategies_app.command("growth-tilt-top3-candidate-pit-replay-engine-remediation")(
        _growth_tilt_top3_candidate_pit_replay_engine_remediation_command
    )
    strategies_app.command("growth-tilt-pit-replay-engine-blocker-closure")(
        _growth_tilt_pit_replay_engine_blocker_closure_command
    )
    strategies_app.command("growth-tilt-top3-candidate-pit-replay-recheck")(
        _growth_tilt_top3_candidate_pit_replay_recheck_command
    )
    strategies_app.command(
        "growth-tilt-top3-candidate-pit-replay-recheck-blocker-closure"
    )(_growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure_command)
    strategies_app.command(
        "growth-tilt-top3-candidate-pit-replay-recheck-after-output-closure"
    )(_growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure_command)
    strategies_app.command(
        "growth-tilt-top3-candidate-level-pit-replay-blocker-closure"
    )(_growth_tilt_top3_candidate_level_pit_replay_blocker_closure_command)
    strategies_app.command(
        "growth-tilt-top3-candidate-pit-replay-recheck-after-candidate-blocker-closure"
    )(
        _growth_tilt_top3_candidate_pit_replay_recheck_after_candidate_blocker_closure_command
    )
    strategies_app.command(
        "growth-tilt-remaining-candidate-pit-replay-blocker-closure"
    )(_growth_tilt_remaining_candidate_pit_replay_blocker_closure_command)
    strategies_app.command(
        "growth-tilt-top3-candidate-pit-replay-recheck-after-remaining-blocker-closure"
    )(
        _growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure_command
    )
    strategies_app.command(
        "growth-tilt-persistent-candidate-pit-replay-blocker-escalation"
    )(_growth_tilt_persistent_candidate_pit_replay_blocker_escalation_command)
    strategies_app.command("growth-tilt-forward-aging-candidate-pack")(
        _growth_tilt_forward_aging_candidate_pack_command
    )
    strategies_app.command("growth-tilt-paper-shadow-candidate-promotion-review")(
        _growth_tilt_paper_shadow_candidate_promotion_review_command
    )


def _dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan_command(
    source_2405_implementation_path: Annotated[
        Path, typer.Option("--source-2405-implementation")
    ] = m2406.DEFAULT_SOURCE_2405_IMPLEMENTATION_PATH,
    source_2405_registry_snapshot_path: Annotated[
        Path, typer.Option("--source-2405-registry-snapshot")
    ] = m2406.DEFAULT_SOURCE_2405_REGISTRY_SNAPSHOT_PATH,
    source_2405_pit_coverage_matrix_path: Annotated[
        Path, typer.Option("--source-2405-pit-matrix")
    ] = m2406.DEFAULT_SOURCE_2405_PIT_COVERAGE_MATRIX_PATH,
    source_2405_pit_gate_result_path: Annotated[
        Path, typer.Option("--source-2405-pit-gate-result")
    ] = m2406.DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH,
    source_2405_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2405-blocker-summary")
    ] = m2406.DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH,
    source_2405_remediation_routes_path: Annotated[
        Path, typer.Option("--source-2405-remediation-routes")
    ] = m2406.DEFAULT_SOURCE_2405_REMEDIATION_ROUTES_PATH,
    source_2403_pit_matrix_path: Annotated[
        Path, typer.Option("--source-2403-pit-matrix")
    ] = m2406.DEFAULT_SOURCE_2403_PIT_MATRIX_PATH,
    source_2403_signal_construction_review_path: Annotated[
        Path, typer.Option("--source-2403-signal-construction-review")
    ] = m2406.DEFAULT_SOURCE_2403_SIGNAL_CONSTRUCTION_REVIEW_PATH,
    source_2403_remediation_matrix_path: Annotated[
        Path, typer.Option("--source-2403-remediation-matrix")
    ] = m2406.DEFAULT_SOURCE_2403_REMEDIATION_MATRIX_PATH,
    pit_input_registry_path: Annotated[
        Path, typer.Option("--pit-input-registry")
    ] = m2406.DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH,
    growth_tilt_config_path: Annotated[
        Path, typer.Option("--growth-tilt-config")
    ] = m2406.DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    execution_policy_registry_path: Annotated[
        Path, typer.Option("--execution-policy-registry")
    ] = m2406.DEFAULT_STRATEGY_EXECUTION_POLICY_REGISTRY_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2406.DEFAULT_DYNAMIC_STRATEGY_GROWTH_TILT_ENGINE_PIT_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2406.DEFAULT_DYNAMIC_STRATEGY_GROWTH_TILT_ENGINE_PIT_SIGNAL_REMEDIATION_PLAN_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = (
        m2406.run_dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan(
            source_2405_implementation_path=source_2405_implementation_path,
            source_2405_registry_snapshot_path=source_2405_registry_snapshot_path,
            source_2405_pit_coverage_matrix_path=source_2405_pit_coverage_matrix_path,
            source_2405_pit_gate_result_path=source_2405_pit_gate_result_path,
            source_2405_blocker_summary_path=source_2405_blocker_summary_path,
            source_2405_remediation_routes_path=source_2405_remediation_routes_path,
            source_2403_pit_matrix_path=source_2403_pit_matrix_path,
            source_2403_signal_construction_review_path=(
                source_2403_signal_construction_review_path
            ),
            source_2403_remediation_matrix_path=source_2403_remediation_matrix_path,
            pit_input_registry_path=pit_input_registry_path,
            growth_tilt_config_path=growth_tilt_config_path,
            execution_policy_registry_path=execution_policy_registry_path,
            output_root=output_root,
            docs_root=docs_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_execution_semantics_payload(
        "Dynamic strategy growth tilt engine PIT remediation plan",
        payload,
    )


def _dynamic_strategy_valid_until_window_stale_signal_remediation_plan_command(
    source_2405_implementation_path: Annotated[
        Path, typer.Option("--source-2405-implementation")
    ] = m2407.DEFAULT_SOURCE_2405_IMPLEMENTATION_PATH,
    source_2405_registry_snapshot_path: Annotated[
        Path, typer.Option("--source-2405-registry-snapshot")
    ] = m2407.DEFAULT_SOURCE_2405_REGISTRY_SNAPSHOT_PATH,
    source_2405_pit_coverage_matrix_path: Annotated[
        Path, typer.Option("--source-2405-pit-matrix")
    ] = m2407.DEFAULT_SOURCE_2405_PIT_COVERAGE_MATRIX_PATH,
    source_2405_pit_gate_result_path: Annotated[
        Path, typer.Option("--source-2405-pit-gate-result")
    ] = m2407.DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH,
    source_2405_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2405-blocker-summary")
    ] = m2407.DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH,
    source_2405_remediation_routes_path: Annotated[
        Path, typer.Option("--source-2405-remediation-routes")
    ] = m2407.DEFAULT_SOURCE_2405_REMEDIATION_ROUTES_PATH,
    source_2406_remediation_plan_path: Annotated[
        Path, typer.Option("--source-2406-remediation-plan")
    ] = m2407.DEFAULT_SOURCE_2406_REMEDIATION_PLAN_PATH,
    source_2406_source_feature_inventory_path: Annotated[
        Path, typer.Option("--source-2406-source-feature-inventory")
    ] = m2407.DEFAULT_SOURCE_2406_SOURCE_FEATURE_INVENTORY_PATH,
    source_2406_pit_risk_audit_path: Annotated[
        Path, typer.Option("--source-2406-pit-risk-audit")
    ] = m2407.DEFAULT_SOURCE_2406_PIT_RISK_AUDIT_PATH,
    source_2406_signal_construction_gap_analysis_path: Annotated[
        Path, typer.Option("--source-2406-signal-construction-gap-analysis")
    ] = m2407.DEFAULT_SOURCE_2406_SIGNAL_CONSTRUCTION_GAP_ANALYSIS_PATH,
    source_2406_severity_downgrade_conditions_path: Annotated[
        Path, typer.Option("--source-2406-severity-downgrade-conditions")
    ] = m2407.DEFAULT_SOURCE_2406_SEVERITY_DOWNGRADE_CONDITIONS_PATH,
    source_2406_validation_plan_path: Annotated[
        Path, typer.Option("--source-2406-validation-plan")
    ] = m2407.DEFAULT_SOURCE_2406_VALIDATION_PLAN_PATH,
    source_2403_pit_matrix_path: Annotated[
        Path, typer.Option("--source-2403-pit-matrix")
    ] = m2407.DEFAULT_SOURCE_2403_PIT_MATRIX_PATH,
    source_2403_signal_construction_review_path: Annotated[
        Path, typer.Option("--source-2403-signal-construction-review")
    ] = m2407.DEFAULT_SOURCE_2403_SIGNAL_CONSTRUCTION_REVIEW_PATH,
    source_2403_remediation_matrix_path: Annotated[
        Path, typer.Option("--source-2403-remediation-matrix")
    ] = m2407.DEFAULT_SOURCE_2403_REMEDIATION_MATRIX_PATH,
    pit_input_registry_path: Annotated[
        Path, typer.Option("--pit-input-registry")
    ] = m2407.DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH,
    execution_policy_registry_path: Annotated[
        Path, typer.Option("--execution-policy-registry")
    ] = m2407.DEFAULT_STRATEGY_EXECUTION_POLICY_REGISTRY_PATH,
    signal_validity_taxonomy_path: Annotated[
        Path, typer.Option("--signal-validity-taxonomy")
    ] = m2407.DEFAULT_SIGNAL_VALIDITY_TAXONOMY_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2407.DEFAULT_DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_STALE_SIGNAL_REMEDIATION_PLAN_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2407.DEFAULT_DYNAMIC_STRATEGY_VALID_UNTIL_WINDOW_STALE_SIGNAL_REMEDIATION_PLAN_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = (
        m2407.run_dynamic_strategy_valid_until_window_stale_signal_remediation_plan(
            source_2405_implementation_path=source_2405_implementation_path,
            source_2405_registry_snapshot_path=source_2405_registry_snapshot_path,
            source_2405_pit_coverage_matrix_path=source_2405_pit_coverage_matrix_path,
            source_2405_pit_gate_result_path=source_2405_pit_gate_result_path,
            source_2405_blocker_summary_path=source_2405_blocker_summary_path,
            source_2405_remediation_routes_path=source_2405_remediation_routes_path,
            source_2406_remediation_plan_path=source_2406_remediation_plan_path,
            source_2406_source_feature_inventory_path=(
                source_2406_source_feature_inventory_path
            ),
            source_2406_pit_risk_audit_path=source_2406_pit_risk_audit_path,
            source_2406_signal_construction_gap_analysis_path=(
                source_2406_signal_construction_gap_analysis_path
            ),
            source_2406_severity_downgrade_conditions_path=(
                source_2406_severity_downgrade_conditions_path
            ),
            source_2406_validation_plan_path=source_2406_validation_plan_path,
            source_2403_pit_matrix_path=source_2403_pit_matrix_path,
            source_2403_signal_construction_review_path=(
                source_2403_signal_construction_review_path
            ),
            source_2403_remediation_matrix_path=source_2403_remediation_matrix_path,
            pit_input_registry_path=pit_input_registry_path,
            execution_policy_registry_path=execution_policy_registry_path,
            signal_validity_taxonomy_path=signal_validity_taxonomy_path,
            output_root=output_root,
            docs_root=docs_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_execution_semantics_payload(
        "Dynamic strategy valid-until window stale signal remediation plan",
        payload,
    )


def _dynamic_strategy_blocking_gap_remediation_implementation_plan_command(
    source_2405_implementation_path: Annotated[
        Path, typer.Option("--source-2405-implementation")
    ] = m2408.DEFAULT_SOURCE_2405_IMPLEMENTATION_PATH,
    source_2405_registry_snapshot_path: Annotated[
        Path, typer.Option("--source-2405-registry-snapshot")
    ] = m2408.DEFAULT_SOURCE_2405_REGISTRY_SNAPSHOT_PATH,
    source_2405_pit_coverage_matrix_path: Annotated[
        Path, typer.Option("--source-2405-pit-matrix")
    ] = m2408.DEFAULT_SOURCE_2405_PIT_COVERAGE_MATRIX_PATH,
    source_2405_pit_gate_result_path: Annotated[
        Path, typer.Option("--source-2405-pit-gate-result")
    ] = m2408.DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH,
    source_2405_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2405-blocker-summary")
    ] = m2408.DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH,
    source_2405_remediation_routes_path: Annotated[
        Path, typer.Option("--source-2405-remediation-routes")
    ] = m2408.DEFAULT_SOURCE_2405_REMEDIATION_ROUTES_PATH,
    source_2406_remediation_plan_path: Annotated[
        Path, typer.Option("--source-2406-remediation-plan")
    ] = m2408.DEFAULT_SOURCE_2406_REMEDIATION_PLAN_PATH,
    source_2406_source_feature_inventory_path: Annotated[
        Path, typer.Option("--source-2406-source-feature-inventory")
    ] = m2408.DEFAULT_SOURCE_2406_SOURCE_FEATURE_INVENTORY_PATH,
    source_2406_pit_risk_audit_path: Annotated[
        Path, typer.Option("--source-2406-pit-risk-audit")
    ] = m2408.DEFAULT_SOURCE_2406_PIT_RISK_AUDIT_PATH,
    source_2406_signal_construction_gap_analysis_path: Annotated[
        Path, typer.Option("--source-2406-signal-construction-gap-analysis")
    ] = m2408.DEFAULT_SOURCE_2406_SIGNAL_CONSTRUCTION_GAP_ANALYSIS_PATH,
    source_2406_severity_downgrade_conditions_path: Annotated[
        Path, typer.Option("--source-2406-severity-downgrade-conditions")
    ] = m2408.DEFAULT_SOURCE_2406_SEVERITY_DOWNGRADE_CONDITIONS_PATH,
    source_2406_validation_plan_path: Annotated[
        Path, typer.Option("--source-2406-validation-plan")
    ] = m2408.DEFAULT_SOURCE_2406_VALIDATION_PLAN_PATH,
    source_2407_remediation_plan_path: Annotated[
        Path, typer.Option("--source-2407-remediation-plan")
    ] = m2408.DEFAULT_SOURCE_2407_REMEDIATION_PLAN_PATH,
    source_2407_valid_until_semantics_review_path: Annotated[
        Path, typer.Option("--source-2407-valid-until-semantics-review")
    ] = m2408.DEFAULT_SOURCE_2407_VALID_UNTIL_SEMANTICS_REVIEW_PATH,
    source_2407_stale_signal_risk_audit_path: Annotated[
        Path, typer.Option("--source-2407-stale-signal-risk-audit")
    ] = m2408.DEFAULT_SOURCE_2407_STALE_SIGNAL_RISK_AUDIT_PATH,
    source_2407_signal_validity_contract_plan_path: Annotated[
        Path, typer.Option("--source-2407-signal-validity-contract-plan")
    ] = m2408.DEFAULT_SOURCE_2407_SIGNAL_VALIDITY_CONTRACT_PLAN_PATH,
    source_2407_severity_downgrade_conditions_path: Annotated[
        Path, typer.Option("--source-2407-severity-downgrade-conditions")
    ] = m2408.DEFAULT_SOURCE_2407_SEVERITY_DOWNGRADE_CONDITIONS_PATH,
    source_2407_validation_plan_path: Annotated[
        Path, typer.Option("--source-2407-validation-plan")
    ] = m2408.DEFAULT_SOURCE_2407_VALIDATION_PLAN_PATH,
    pit_input_registry_path: Annotated[
        Path, typer.Option("--pit-input-registry")
    ] = m2408.DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2408.DEFAULT_DYNAMIC_STRATEGY_BLOCKING_GAP_REMEDIATION_IMPLEMENTATION_PLAN_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2408.DEFAULT_DYNAMIC_STRATEGY_BLOCKING_GAP_REMEDIATION_IMPLEMENTATION_PLAN_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2408.run_dynamic_strategy_blocking_gap_remediation_implementation_plan(
        source_2405_implementation_path=source_2405_implementation_path,
        source_2405_registry_snapshot_path=source_2405_registry_snapshot_path,
        source_2405_pit_coverage_matrix_path=source_2405_pit_coverage_matrix_path,
        source_2405_pit_gate_result_path=source_2405_pit_gate_result_path,
        source_2405_blocker_summary_path=source_2405_blocker_summary_path,
        source_2405_remediation_routes_path=source_2405_remediation_routes_path,
        source_2406_remediation_plan_path=source_2406_remediation_plan_path,
        source_2406_source_feature_inventory_path=(
            source_2406_source_feature_inventory_path
        ),
        source_2406_pit_risk_audit_path=source_2406_pit_risk_audit_path,
        source_2406_signal_construction_gap_analysis_path=(
            source_2406_signal_construction_gap_analysis_path
        ),
        source_2406_severity_downgrade_conditions_path=(
            source_2406_severity_downgrade_conditions_path
        ),
        source_2406_validation_plan_path=source_2406_validation_plan_path,
        source_2407_remediation_plan_path=source_2407_remediation_plan_path,
        source_2407_valid_until_semantics_review_path=(
            source_2407_valid_until_semantics_review_path
        ),
        source_2407_stale_signal_risk_audit_path=(
            source_2407_stale_signal_risk_audit_path
        ),
        source_2407_signal_validity_contract_plan_path=(
            source_2407_signal_validity_contract_plan_path
        ),
        source_2407_severity_downgrade_conditions_path=(
            source_2407_severity_downgrade_conditions_path
        ),
        source_2407_validation_plan_path=source_2407_validation_plan_path,
        pit_input_registry_path=pit_input_registry_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy blocking gap remediation implementation plan",
        payload,
    )


def _dynamic_strategy_signal_as_of_validity_contract_schema_command(
    source_2408_implementation_plan_path: Annotated[
        Path, typer.Option("--source-2408-implementation-plan")
    ] = m2409.DEFAULT_SOURCE_2408_IMPLEMENTATION_PLAN_PATH,
    source_2408_contract_schema_plan_path: Annotated[
        Path, typer.Option("--source-2408-contract-schema-plan")
    ] = m2409.DEFAULT_SOURCE_2408_CONTRACT_SCHEMA_PLAN_PATH,
    source_2408_candidate_search_gate_policy_path: Annotated[
        Path, typer.Option("--source-2408-candidate-search-gate-policy")
    ] = m2409.DEFAULT_SOURCE_2408_CANDIDATE_SEARCH_GATE_POLICY_PATH,
    source_2405_registry_snapshot_path: Annotated[
        Path, typer.Option("--source-2405-registry-snapshot")
    ] = m2409.DEFAULT_SOURCE_2405_REGISTRY_SNAPSHOT_PATH,
    source_2405_pit_gate_result_path: Annotated[
        Path, typer.Option("--source-2405-pit-gate-result")
    ] = m2409.DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH,
    source_2405_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2405-blocker-summary")
    ] = m2409.DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH,
    pit_input_registry_path: Annotated[
        Path, typer.Option("--pit-input-registry")
    ] = m2409.DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2409.DEFAULT_DYNAMIC_STRATEGY_SIGNAL_AS_OF_VALIDITY_CONTRACT_SCHEMA_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2409.DEFAULT_DYNAMIC_STRATEGY_SIGNAL_AS_OF_VALIDITY_CONTRACT_SCHEMA_DOCS_ROOT,
    research_quality_output_root: Annotated[
        Path, typer.Option("--research-quality-output-root")
    ] = m2409.DEFAULT_SIGNAL_CONTRACT_RESEARCH_QUALITY_OUTPUT_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2409.run_dynamic_strategy_signal_as_of_validity_contract_schema(
        source_2408_implementation_plan_path=source_2408_implementation_plan_path,
        source_2408_contract_schema_plan_path=source_2408_contract_schema_plan_path,
        source_2408_candidate_search_gate_policy_path=(
            source_2408_candidate_search_gate_policy_path
        ),
        source_2405_registry_snapshot_path=source_2405_registry_snapshot_path,
        source_2405_pit_gate_result_path=source_2405_pit_gate_result_path,
        source_2405_blocker_summary_path=source_2405_blocker_summary_path,
        pit_input_registry_path=pit_input_registry_path,
        output_root=output_root,
        docs_root=docs_root,
        research_quality_output_root=research_quality_output_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Dynamic strategy signal as-of and validity contract schema",
        payload,
    )


def _growth_tilt_engine_source_feature_contract_mapping_command(
    source_2409_contract_schema_result_path: Annotated[
        Path, typer.Option("--source-2409-contract-schema-result")
    ] = m2410.DEFAULT_SOURCE_2409_CONTRACT_SCHEMA_RESULT_PATH,
    source_2409_source_feature_contract_schema_path: Annotated[
        Path, typer.Option("--source-2409-source-feature-contract-schema")
    ] = m2410.DEFAULT_SOURCE_2409_SOURCE_FEATURE_CONTRACT_SCHEMA_PATH,
    source_2409_signal_as_of_contract_schema_path: Annotated[
        Path, typer.Option("--source-2409-signal-as-of-contract-schema")
    ] = m2410.DEFAULT_SOURCE_2409_SIGNAL_AS_OF_CONTRACT_SCHEMA_PATH,
    source_2409_signal_validity_contract_schema_path: Annotated[
        Path, typer.Option("--source-2409-signal-validity-contract-schema")
    ] = m2410.DEFAULT_SOURCE_2409_SIGNAL_VALIDITY_CONTRACT_SCHEMA_PATH,
    source_2409_contract_schema_snapshot_path: Annotated[
        Path, typer.Option("--source-2409-contract-schema-snapshot")
    ] = m2410.DEFAULT_SOURCE_2409_CONTRACT_SCHEMA_SNAPSHOT_PATH,
    source_2406_source_feature_inventory_path: Annotated[
        Path, typer.Option("--source-2406-source-feature-inventory")
    ] = m2410.DEFAULT_SOURCE_2406_SOURCE_FEATURE_INVENTORY_PATH,
    source_2406_pit_risk_audit_path: Annotated[
        Path, typer.Option("--source-2406-pit-risk-audit")
    ] = m2410.DEFAULT_SOURCE_2406_PIT_RISK_AUDIT_PATH,
    source_2406_signal_construction_gap_analysis_path: Annotated[
        Path, typer.Option("--source-2406-signal-construction-gap-analysis")
    ] = m2410.DEFAULT_SOURCE_2406_SIGNAL_CONSTRUCTION_GAP_ANALYSIS_PATH,
    source_2406_remediation_plan_path: Annotated[
        Path, typer.Option("--source-2406-remediation-plan")
    ] = m2410.DEFAULT_SOURCE_2406_REMEDIATION_PLAN_PATH,
    source_2405_pit_gate_result_path: Annotated[
        Path, typer.Option("--source-2405-pit-gate-result")
    ] = m2410.DEFAULT_SOURCE_2405_PIT_GATE_RESULT_PATH,
    source_2405_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2405-blocker-summary")
    ] = m2410.DEFAULT_SOURCE_2405_BLOCKER_SUMMARY_PATH,
    pit_input_registry_path: Annotated[
        Path, typer.Option("--pit-input-registry")
    ] = m2410.DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH,
    growth_tilt_candidate_registry_path: Annotated[
        Path, typer.Option("--growth-tilt-candidate-registry")
    ] = m2410.DEFAULT_EQUAL_RISK_GROWTH_TILT_CANDIDATE_REGISTRY_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2410.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2410.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_FEATURE_CONTRACT_MAPPING_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2410.run_growth_tilt_engine_source_feature_contract_mapping(
        source_2409_contract_schema_result_path=source_2409_contract_schema_result_path,
        source_2409_source_feature_contract_schema_path=(
            source_2409_source_feature_contract_schema_path
        ),
        source_2409_signal_as_of_contract_schema_path=(
            source_2409_signal_as_of_contract_schema_path
        ),
        source_2409_signal_validity_contract_schema_path=(
            source_2409_signal_validity_contract_schema_path
        ),
        source_2409_contract_schema_snapshot_path=(
            source_2409_contract_schema_snapshot_path
        ),
        source_2406_source_feature_inventory_path=(
            source_2406_source_feature_inventory_path
        ),
        source_2406_pit_risk_audit_path=source_2406_pit_risk_audit_path,
        source_2406_signal_construction_gap_analysis_path=(
            source_2406_signal_construction_gap_analysis_path
        ),
        source_2406_remediation_plan_path=source_2406_remediation_plan_path,
        source_2405_pit_gate_result_path=source_2405_pit_gate_result_path,
        source_2405_blocker_summary_path=source_2405_blocker_summary_path,
        pit_input_registry_path=pit_input_registry_path,
        growth_tilt_candidate_registry_path=growth_tilt_candidate_registry_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine source feature contract mapping",
        payload,
    )
    for field in (
        "blockers_resolved",
        "blockers_downgraded",
        "candidate_search_enabled",
        "observation_enabled",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_enabled",
    ):
        console.print(f"{field}={payload.get(field)}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_contract_gap_remediation_plan_command(
    source_2410_mapping_result_path: Annotated[
        Path, typer.Option("--source-2410-mapping-result")
    ] = m2411.DEFAULT_SOURCE_2410_MAPPING_RESULT_PATH,
    source_2410_source_feature_contract_mapping_path: Annotated[
        Path, typer.Option("--source-2410-source-feature-contract-mapping")
    ] = m2411.DEFAULT_SOURCE_2410_SOURCE_FEATURE_CONTRACT_MAPPING_PATH,
    source_2410_contract_mapping_validation_path: Annotated[
        Path, typer.Option("--source-2410-contract-mapping-validation")
    ] = m2411.DEFAULT_SOURCE_2410_CONTRACT_MAPPING_VALIDATION_PATH,
    source_2410_unresolved_gap_summary_path: Annotated[
        Path, typer.Option("--source-2410-unresolved-gap-summary")
    ] = m2411.DEFAULT_SOURCE_2410_UNRESOLVED_GAP_SUMMARY_PATH,
    source_2410_research_doc_path: Annotated[
        Path, typer.Option("--source-2410-research-doc")
    ] = m2411.DEFAULT_SOURCE_2410_RESEARCH_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2411.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2411.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2411.DEFAULT_GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2411.DEFAULT_GROWTH_TILT_ENGINE_CONTRACT_GAP_REMEDIATION_PLAN_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2411.run_growth_tilt_engine_contract_gap_remediation_plan(
        source_2410_mapping_result_path=source_2410_mapping_result_path,
        source_2410_source_feature_contract_mapping_path=(
            source_2410_source_feature_contract_mapping_path
        ),
        source_2410_contract_mapping_validation_path=(
            source_2410_contract_mapping_validation_path
        ),
        source_2410_unresolved_gap_summary_path=(
            source_2410_unresolved_gap_summary_path
        ),
        source_2410_research_doc_path=source_2410_research_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine contract gap remediation plan",
        payload,
    )
    for field in (
        "growth_tilt_engine_blocker_resolved",
        "growth_tilt_engine_blocker_downgraded",
        "valid_until_window_blocker_resolved",
        "valid_until_window_blocker_downgraded",
        "candidate_search_enabled",
        "observation_enabled",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_enabled",
    ):
        console.print(f"{field}={payload.get(field)}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_as_of_semantics_remediation_command(
    source_2411_remediation_plan_result_path: Annotated[
        Path, typer.Option("--source-2411-remediation-plan-result")
    ] = m2412.DEFAULT_SOURCE_2411_REMEDIATION_PLAN_RESULT_PATH,
    source_2411_contract_gap_remediation_plan_path: Annotated[
        Path, typer.Option("--source-2411-contract-gap-remediation-plan")
    ] = m2412.DEFAULT_SOURCE_2411_CONTRACT_GAP_REMEDIATION_PLAN_PATH,
    source_2411_ordered_remediation_items_path: Annotated[
        Path, typer.Option("--source-2411-ordered-remediation-items")
    ] = m2412.DEFAULT_SOURCE_2411_ORDERED_REMEDIATION_ITEMS_PATH,
    source_2411_unresolved_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2411-unresolved-blocker-summary")
    ] = m2412.DEFAULT_SOURCE_2411_UNRESOLVED_BLOCKER_SUMMARY_PATH,
    source_2411_research_doc_path: Annotated[
        Path, typer.Option("--source-2411-research-doc")
    ] = m2412.DEFAULT_SOURCE_2411_RESEARCH_DOC_PATH,
    source_2410_mapping_result_path: Annotated[
        Path, typer.Option("--source-2410-mapping-result")
    ] = m2412.DEFAULT_SOURCE_2410_MAPPING_RESULT_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2412.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2412.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2412.DEFAULT_GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2412.DEFAULT_GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2412.run_growth_tilt_engine_as_of_semantics_remediation(
        source_2411_remediation_plan_result_path=source_2411_remediation_plan_result_path,
        source_2411_contract_gap_remediation_plan_path=(
            source_2411_contract_gap_remediation_plan_path
        ),
        source_2411_ordered_remediation_items_path=(
            source_2411_ordered_remediation_items_path
        ),
        source_2411_unresolved_blocker_summary_path=(
            source_2411_unresolved_blocker_summary_path
        ),
        source_2411_research_doc_path=source_2411_research_doc_path,
        source_2410_mapping_result_path=source_2410_mapping_result_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine as-of semantics remediation",
        payload,
    )
    for field in (
        "as_of_remediation_completed",
        "growth_tilt_engine_blocker_resolved",
        "growth_tilt_engine_blocker_downgraded",
        "valid_until_window_blocker_resolved",
        "valid_until_window_blocker_downgraded",
        "candidate_search_enabled",
        "observation_enabled",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_enabled",
        "input_gap_count",
        "as_of_gap_count",
        "as_of_remediated_count",
        "remaining_blocked_or_gap_count",
        "contract_ready_count",
    ):
        console.print(f"{field}={payload.get(field)}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_source_traceability_remediation_command(
    source_2412_as_of_remediation_result_path: Annotated[
        Path, typer.Option("--source-2412-as-of-remediation-result")
    ] = m2413.DEFAULT_SOURCE_2412_AS_OF_REMEDIATION_RESULT_PATH,
    source_2412_before_after_remediation_path: Annotated[
        Path, typer.Option("--source-2412-before-after-remediation")
    ] = m2413.DEFAULT_SOURCE_2412_BEFORE_AFTER_REMEDIATION_PATH,
    source_2412_updated_source_feature_mapping_path: Annotated[
        Path, typer.Option("--source-2412-updated-source-feature-mapping")
    ] = m2413.DEFAULT_SOURCE_2412_UPDATED_SOURCE_FEATURE_MAPPING_PATH,
    source_2412_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2412-remaining-blocker-summary")
    ] = m2413.DEFAULT_SOURCE_2412_REMAINING_BLOCKER_SUMMARY_PATH,
    source_2412_research_doc_path: Annotated[
        Path, typer.Option("--source-2412-research-doc")
    ] = m2413.DEFAULT_SOURCE_2412_RESEARCH_DOC_PATH,
    source_2411_remediation_plan_result_path: Annotated[
        Path, typer.Option("--source-2411-remediation-plan-result")
    ] = m2413.DEFAULT_SOURCE_2411_REMEDIATION_PLAN_RESULT_PATH,
    source_2411_ordered_remediation_items_path: Annotated[
        Path, typer.Option("--source-2411-ordered-remediation-items")
    ] = m2413.DEFAULT_SOURCE_2411_ORDERED_REMEDIATION_ITEMS_PATH,
    source_2411_unresolved_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2411-unresolved-blocker-summary")
    ] = m2413.DEFAULT_SOURCE_2411_UNRESOLVED_BLOCKER_SUMMARY_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2413.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2413.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2413.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_REMEDIATION_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2413.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_REMEDIATION_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2413.run_growth_tilt_engine_source_traceability_remediation(
        source_2412_as_of_remediation_result_path=(
            source_2412_as_of_remediation_result_path
        ),
        source_2412_before_after_remediation_path=source_2412_before_after_remediation_path,
        source_2412_updated_source_feature_mapping_path=(
            source_2412_updated_source_feature_mapping_path
        ),
        source_2412_remaining_blocker_summary_path=(
            source_2412_remaining_blocker_summary_path
        ),
        source_2412_research_doc_path=source_2412_research_doc_path,
        source_2411_remediation_plan_result_path=source_2411_remediation_plan_result_path,
        source_2411_ordered_remediation_items_path=source_2411_ordered_remediation_items_path,
        source_2411_unresolved_blocker_summary_path=(
            source_2411_unresolved_blocker_summary_path
        ),
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine source traceability remediation",
        payload,
    )
    for field in (
        "source_traceability_remediation_completed",
        "growth_tilt_engine_blocker_resolved",
        "growth_tilt_engine_blocker_downgraded",
        "valid_until_window_blocker_resolved",
        "valid_until_window_blocker_downgraded",
        "candidate_search_enabled",
        "observation_enabled",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_enabled",
        "input_gap_count",
        "source_traceability_gap_count",
        "source_traceability_remediated_count",
        "remaining_blocked_or_gap_count",
        "contract_ready_count",
    ):
        console.print(f"{field}={payload.get(field)}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_signal_validity_dependency_remediation_command(
    source_2413_source_traceability_remediation_result_path: Annotated[
        Path, typer.Option("--source-2413-source-traceability-remediation-result")
    ] = m2414.DEFAULT_SOURCE_2413_SOURCE_TRACEABILITY_REMEDIATION_RESULT_PATH,
    source_2413_source_traceability_contract_metadata_path: Annotated[
        Path, typer.Option("--source-2413-source-traceability-contract-metadata")
    ] = m2414.DEFAULT_SOURCE_2413_SOURCE_TRACEABILITY_CONTRACT_METADATA_PATH,
    source_2413_before_after_remediation_path: Annotated[
        Path, typer.Option("--source-2413-before-after-remediation")
    ] = m2414.DEFAULT_SOURCE_2413_BEFORE_AFTER_REMEDIATION_PATH,
    source_2413_updated_source_feature_mapping_path: Annotated[
        Path, typer.Option("--source-2413-updated-source-feature-mapping")
    ] = m2414.DEFAULT_SOURCE_2413_UPDATED_SOURCE_FEATURE_MAPPING_PATH,
    source_2413_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2413-remaining-blocker-summary")
    ] = m2414.DEFAULT_SOURCE_2413_REMAINING_BLOCKER_SUMMARY_PATH,
    source_2413_research_doc_path: Annotated[
        Path, typer.Option("--source-2413-research-doc")
    ] = m2414.DEFAULT_SOURCE_2413_RESEARCH_DOC_PATH,
    source_2412_as_of_remediation_result_path: Annotated[
        Path, typer.Option("--source-2412-as-of-remediation-result")
    ] = m2414.DEFAULT_SOURCE_2412_AS_OF_REMEDIATION_RESULT_PATH,
    source_2412_updated_source_feature_mapping_path: Annotated[
        Path, typer.Option("--source-2412-updated-source-feature-mapping")
    ] = m2414.DEFAULT_SOURCE_2412_UPDATED_SOURCE_FEATURE_MAPPING_PATH,
    source_2412_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2412-remaining-blocker-summary")
    ] = m2414.DEFAULT_SOURCE_2412_REMAINING_BLOCKER_SUMMARY_PATH,
    source_2412_research_doc_path: Annotated[
        Path, typer.Option("--source-2412-research-doc")
    ] = m2414.DEFAULT_SOURCE_2412_RESEARCH_DOC_PATH,
    source_2411_remediation_plan_result_path: Annotated[
        Path, typer.Option("--source-2411-remediation-plan-result")
    ] = m2414.DEFAULT_SOURCE_2411_REMEDIATION_PLAN_RESULT_PATH,
    source_2411_ordered_remediation_items_path: Annotated[
        Path, typer.Option("--source-2411-ordered-remediation-items")
    ] = m2414.DEFAULT_SOURCE_2411_ORDERED_REMEDIATION_ITEMS_PATH,
    source_2411_unresolved_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2411-unresolved-blocker-summary")
    ] = m2414.DEFAULT_SOURCE_2411_UNRESOLVED_BLOCKER_SUMMARY_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2414.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2414.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2414.DEFAULT_GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2414.DEFAULT_GROWTH_TILT_ENGINE_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2414.run_growth_tilt_engine_signal_validity_dependency_remediation(
        source_2413_source_traceability_remediation_result_path=(
            source_2413_source_traceability_remediation_result_path
        ),
        source_2413_source_traceability_contract_metadata_path=(
            source_2413_source_traceability_contract_metadata_path
        ),
        source_2413_before_after_remediation_path=(
            source_2413_before_after_remediation_path
        ),
        source_2413_updated_source_feature_mapping_path=(
            source_2413_updated_source_feature_mapping_path
        ),
        source_2413_remaining_blocker_summary_path=(
            source_2413_remaining_blocker_summary_path
        ),
        source_2413_research_doc_path=source_2413_research_doc_path,
        source_2412_as_of_remediation_result_path=(
            source_2412_as_of_remediation_result_path
        ),
        source_2412_updated_source_feature_mapping_path=(
            source_2412_updated_source_feature_mapping_path
        ),
        source_2412_remaining_blocker_summary_path=(
            source_2412_remaining_blocker_summary_path
        ),
        source_2412_research_doc_path=source_2412_research_doc_path,
        source_2411_remediation_plan_result_path=source_2411_remediation_plan_result_path,
        source_2411_ordered_remediation_items_path=source_2411_ordered_remediation_items_path,
        source_2411_unresolved_blocker_summary_path=(
            source_2411_unresolved_blocker_summary_path
        ),
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine signal validity dependency remediation",
        payload,
    )
    for field in (
        "signal_validity_dependency_remediation_completed",
        "growth_tilt_engine_blocker_resolved",
        "growth_tilt_engine_blocker_downgraded",
        "valid_until_window_blocker_resolved",
        "valid_until_window_blocker_downgraded",
        "candidate_search_enabled",
        "observation_enabled",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_enabled",
        "input_gap_count",
        "validity_dependency_gap_count",
        "validity_dependency_remediated_count",
        "validity_dependency_blocked_by_valid_until_window_count",
        "validity_dependency_blocked_by_source_traceability_count",
        "remaining_blocked_or_gap_count",
        "contract_ready_count",
    ):
        console.print(f"{field}={payload.get(field)}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_pit_gate_readiness_snapshot_command(
    source_2410_mapping_result_path: Annotated[
        Path, typer.Option("--source-2410-mapping-result")
    ] = m2415.DEFAULT_SOURCE_2410_MAPPING_RESULT_PATH,
    source_2410_source_feature_contract_mapping_path: Annotated[
        Path, typer.Option("--source-2410-source-feature-contract-mapping")
    ] = m2415.DEFAULT_SOURCE_2410_SOURCE_FEATURE_CONTRACT_MAPPING_PATH,
    source_2411_remediation_plan_result_path: Annotated[
        Path, typer.Option("--source-2411-remediation-plan-result")
    ] = m2415.DEFAULT_SOURCE_2411_REMEDIATION_PLAN_RESULT_PATH,
    source_2411_ordered_remediation_items_path: Annotated[
        Path, typer.Option("--source-2411-ordered-remediation-items")
    ] = m2415.DEFAULT_SOURCE_2411_ORDERED_REMEDIATION_ITEMS_PATH,
    source_2411_unresolved_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2411-unresolved-blocker-summary")
    ] = m2415.DEFAULT_SOURCE_2411_UNRESOLVED_BLOCKER_SUMMARY_PATH,
    source_2412_as_of_remediation_result_path: Annotated[
        Path, typer.Option("--source-2412-as-of-remediation-result")
    ] = m2415.DEFAULT_SOURCE_2412_AS_OF_REMEDIATION_RESULT_PATH,
    source_2412_updated_source_feature_mapping_path: Annotated[
        Path, typer.Option("--source-2412-updated-source-feature-mapping")
    ] = m2415.DEFAULT_SOURCE_2412_UPDATED_SOURCE_FEATURE_MAPPING_PATH,
    source_2412_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2412-remaining-blocker-summary")
    ] = m2415.DEFAULT_SOURCE_2412_REMAINING_BLOCKER_SUMMARY_PATH,
    source_2413_source_traceability_remediation_result_path: Annotated[
        Path, typer.Option("--source-2413-source-traceability-remediation-result")
    ] = m2415.DEFAULT_SOURCE_2413_SOURCE_TRACEABILITY_REMEDIATION_RESULT_PATH,
    source_2413_updated_source_feature_mapping_path: Annotated[
        Path, typer.Option("--source-2413-updated-source-feature-mapping")
    ] = m2415.DEFAULT_SOURCE_2413_UPDATED_SOURCE_FEATURE_MAPPING_PATH,
    source_2413_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2413-remaining-blocker-summary")
    ] = m2415.DEFAULT_SOURCE_2413_REMAINING_BLOCKER_SUMMARY_PATH,
    source_2414_signal_validity_dependency_remediation_result_path: Annotated[
        Path, typer.Option("--source-2414-signal-validity-dependency-remediation-result")
    ] = m2415.DEFAULT_SOURCE_2414_SIGNAL_VALIDITY_DEPENDENCY_REMEDIATION_RESULT_PATH,
    source_2414_updated_source_feature_mapping_path: Annotated[
        Path, typer.Option("--source-2414-updated-source-feature-mapping")
    ] = m2415.DEFAULT_SOURCE_2414_UPDATED_SOURCE_FEATURE_MAPPING_PATH,
    source_2414_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2414-remaining-blocker-summary")
    ] = m2415.DEFAULT_SOURCE_2414_REMAINING_BLOCKER_SUMMARY_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2415.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2415.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2415.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2415.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2415.run_growth_tilt_engine_pit_gate_readiness_snapshot(
        source_2410_mapping_result_path=source_2410_mapping_result_path,
        source_2410_source_feature_contract_mapping_path=(
            source_2410_source_feature_contract_mapping_path
        ),
        source_2411_remediation_plan_result_path=source_2411_remediation_plan_result_path,
        source_2411_ordered_remediation_items_path=source_2411_ordered_remediation_items_path,
        source_2411_unresolved_blocker_summary_path=(
            source_2411_unresolved_blocker_summary_path
        ),
        source_2412_as_of_remediation_result_path=(
            source_2412_as_of_remediation_result_path
        ),
        source_2412_updated_source_feature_mapping_path=(
            source_2412_updated_source_feature_mapping_path
        ),
        source_2412_remaining_blocker_summary_path=(
            source_2412_remaining_blocker_summary_path
        ),
        source_2413_source_traceability_remediation_result_path=(
            source_2413_source_traceability_remediation_result_path
        ),
        source_2413_updated_source_feature_mapping_path=(
            source_2413_updated_source_feature_mapping_path
        ),
        source_2413_remaining_blocker_summary_path=(
            source_2413_remaining_blocker_summary_path
        ),
        source_2414_signal_validity_dependency_remediation_result_path=(
            source_2414_signal_validity_dependency_remediation_result_path
        ),
        source_2414_updated_source_feature_mapping_path=(
            source_2414_updated_source_feature_mapping_path
        ),
        source_2414_remaining_blocker_summary_path=(
            source_2414_remaining_blocker_summary_path
        ),
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine PIT gate readiness snapshot",
        payload,
    )
    for field in (
        "pit_gate_readiness_snapshot_completed",
        "growth_tilt_engine_blocker_resolved",
        "growth_tilt_engine_blocker_downgraded",
        "valid_until_window_blocker_resolved",
        "valid_until_window_blocker_downgraded",
        "candidate_search_enabled",
        "observation_enabled",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_enabled",
        "source_feature_count",
        "as_of_ready_count",
        "source_traceability_ready_count",
        "validity_dependency_ready_count",
        "pit_gate_ready_count",
        "contract_ready_count",
        "pit_gate_blocked_count",
        "blocked_by_source_traceability_count",
        "blocked_by_valid_until_window_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_pit_gate_remaining_blocker_closure_plan_command(
    source_2415_readiness_snapshot_result_path: Annotated[
        Path, typer.Option("--source-2415-readiness-snapshot-result")
    ] = m2416.DEFAULT_SOURCE_2415_READINESS_SNAPSHOT_RESULT_PATH,
    source_2415_readiness_matrix_path: Annotated[
        Path, typer.Option("--source-2415-readiness-matrix")
    ] = m2416.DEFAULT_SOURCE_2415_READINESS_MATRIX_PATH,
    source_2415_readiness_validation_path: Annotated[
        Path, typer.Option("--source-2415-readiness-validation")
    ] = m2416.DEFAULT_SOURCE_2415_READINESS_VALIDATION_PATH,
    source_2415_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2415-remaining-blocker-summary")
    ] = m2416.DEFAULT_SOURCE_2415_REMAINING_BLOCKER_SUMMARY_PATH,
    pit_input_registry_path: Annotated[
        Path, typer.Option("--pit-input-registry")
    ] = m2416.DEFAULT_PIT_INPUT_REGISTRY_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2416.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2416.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2416.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2416.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2416.run_growth_tilt_engine_pit_gate_remaining_blocker_closure_plan(
        source_2415_readiness_snapshot_result_path=(
            source_2415_readiness_snapshot_result_path
        ),
        source_2415_readiness_matrix_path=source_2415_readiness_matrix_path,
        source_2415_readiness_validation_path=source_2415_readiness_validation_path,
        source_2415_remaining_blocker_summary_path=(
            source_2415_remaining_blocker_summary_path
        ),
        pit_input_registry_path=pit_input_registry_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine PIT gate remaining blocker closure plan",
        payload,
    )
    for field in (
        "remaining_blocker_matrix_ready",
        "source_traceability_closure_plan_ready",
        "as_of_evidence_closure_plan_ready",
        "valid_until_dependency_closure_plan_ready",
        "pit_gate_evidence_requirements_ready",
        "growth_tilt_engine_blocking_gap_resolved",
        "growth_tilt_engine_severity_downgraded",
        "valid_until_window_blocking_gap_resolved",
        "valid_until_window_severity_downgraded",
        "candidate_search_allowed",
        "candidate_search_resumed",
        "research_only_observation_allowed",
        "research_only_observation_approved",
        "paper_shadow_enabled",
        "event_append_enabled",
        "outcome_binding_enabled",
        "scheduler_enabled",
        "production_enabled",
        "broker_action_enabled",
        "daily_report_generated",
        "source_feature_count",
        "pit_gate_ready_count",
        "contract_ready_count",
        "pit_gate_blocked_count",
        "blocked_by_source_traceability_count",
        "blocked_by_valid_until_window_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_source_traceability_upstream_artifact_closure_command(
    source_2416_closure_result_path: Annotated[
        Path, typer.Option("--source-2416-closure-result")
    ] = m2417.DEFAULT_SOURCE_2416_CLOSURE_RESULT_PATH,
    source_2416_remaining_blocker_matrix_path: Annotated[
        Path, typer.Option("--source-2416-remaining-blocker-matrix")
    ] = m2417.DEFAULT_SOURCE_2416_REMAINING_BLOCKER_MATRIX_PATH,
    source_2416_source_traceability_closure_plan_path: Annotated[
        Path, typer.Option("--source-2416-source-traceability-closure-plan")
    ] = m2417.DEFAULT_SOURCE_2416_SOURCE_TRACEABILITY_CLOSURE_PLAN_PATH,
    source_2416_as_of_evidence_closure_plan_path: Annotated[
        Path, typer.Option("--source-2416-as-of-evidence-closure-plan")
    ] = m2417.DEFAULT_SOURCE_2416_AS_OF_EVIDENCE_CLOSURE_PLAN_PATH,
    source_2416_valid_until_dependency_closure_plan_path: Annotated[
        Path, typer.Option("--source-2416-valid-until-dependency-closure-plan")
    ] = m2417.DEFAULT_SOURCE_2416_VALID_UNTIL_DEPENDENCY_CLOSURE_PLAN_PATH,
    source_2416_pit_gate_evidence_requirements_path: Annotated[
        Path, typer.Option("--source-2416-pit-gate-evidence-requirements")
    ] = m2417.DEFAULT_SOURCE_2416_PIT_GATE_EVIDENCE_REQUIREMENTS_PATH,
    source_2415_readiness_snapshot_result_path: Annotated[
        Path, typer.Option("--source-2415-readiness-snapshot-result")
    ] = m2417.DEFAULT_SOURCE_2415_READINESS_SNAPSHOT_RESULT_PATH,
    source_2415_readiness_matrix_path: Annotated[
        Path, typer.Option("--source-2415-readiness-matrix")
    ] = m2417.DEFAULT_SOURCE_2415_READINESS_MATRIX_PATH,
    source_2413_source_traceability_remediation_result_path: Annotated[
        Path, typer.Option("--source-2413-source-traceability-remediation-result")
    ] = m2417.DEFAULT_SOURCE_2413_SOURCE_TRACEABILITY_REMEDIATION_RESULT_PATH,
    source_2413_updated_source_feature_mapping_path: Annotated[
        Path, typer.Option("--source-2413-updated-source-feature-mapping")
    ] = m2417.DEFAULT_SOURCE_2413_UPDATED_SOURCE_FEATURE_MAPPING_PATH,
    source_2413_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2413-remaining-blocker-summary")
    ] = m2417.DEFAULT_SOURCE_2413_REMAINING_BLOCKER_SUMMARY_PATH,
    source_2412_updated_source_feature_mapping_path: Annotated[
        Path, typer.Option("--source-2412-updated-source-feature-mapping")
    ] = m2417.DEFAULT_SOURCE_2412_UPDATED_SOURCE_FEATURE_MAPPING_PATH,
    source_2410_mapping_result_path: Annotated[
        Path, typer.Option("--source-2410-mapping-result")
    ] = m2417.DEFAULT_SOURCE_2410_MAPPING_RESULT_PATH,
    source_2410_source_feature_contract_mapping_path: Annotated[
        Path, typer.Option("--source-2410-source-feature-contract-mapping")
    ] = m2417.DEFAULT_SOURCE_2410_SOURCE_FEATURE_CONTRACT_MAPPING_PATH,
    pit_input_registry_path: Annotated[
        Path, typer.Option("--pit-input-registry")
    ] = m2417.DEFAULT_PIT_INPUT_REGISTRY_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2417.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2417.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2417.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_UPSTREAM_ARTIFACT_CLOSURE_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2417.DEFAULT_GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_UPSTREAM_ARTIFACT_CLOSURE_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = (
        m2417.run_growth_tilt_engine_source_traceability_upstream_artifact_closure(
            source_2416_closure_result_path=source_2416_closure_result_path,
            source_2416_remaining_blocker_matrix_path=(
                source_2416_remaining_blocker_matrix_path
            ),
            source_2416_source_traceability_closure_plan_path=(
                source_2416_source_traceability_closure_plan_path
            ),
            source_2416_as_of_evidence_closure_plan_path=(
                source_2416_as_of_evidence_closure_plan_path
            ),
            source_2416_valid_until_dependency_closure_plan_path=(
                source_2416_valid_until_dependency_closure_plan_path
            ),
            source_2416_pit_gate_evidence_requirements_path=(
                source_2416_pit_gate_evidence_requirements_path
            ),
            source_2415_readiness_snapshot_result_path=(
                source_2415_readiness_snapshot_result_path
            ),
            source_2415_readiness_matrix_path=source_2415_readiness_matrix_path,
            source_2413_source_traceability_remediation_result_path=(
                source_2413_source_traceability_remediation_result_path
            ),
            source_2413_updated_source_feature_mapping_path=(
                source_2413_updated_source_feature_mapping_path
            ),
            source_2413_remaining_blocker_summary_path=(
                source_2413_remaining_blocker_summary_path
            ),
            source_2412_updated_source_feature_mapping_path=(
                source_2412_updated_source_feature_mapping_path
            ),
            source_2410_mapping_result_path=source_2410_mapping_result_path,
            source_2410_source_feature_contract_mapping_path=(
                source_2410_source_feature_contract_mapping_path
            ),
            pit_input_registry_path=pit_input_registry_path,
            report_registry_path=report_registry_path,
            artifact_catalog_path=artifact_catalog_path,
            output_root=output_root,
            docs_root=docs_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_execution_semantics_payload(
        "Growth tilt engine source traceability upstream artifact closure",
        payload,
    )
    for field in (
        "source_traceability_closure_evidence_ready",
        "upstream_artifact_closure_evidence_ready",
        "updated_source_feature_mapping_ready",
        "remaining_blocker_summary_ready",
        "pit_gate_recheck_required",
        "auto_mark_pit_gate_ready",
        "auto_mark_contract_ready",
        "growth_tilt_engine_blocking_gap_resolved",
        "growth_tilt_engine_severity_downgraded",
        "valid_until_window_blocking_gap_resolved",
        "valid_until_window_severity_downgraded",
        "candidate_search_allowed",
        "candidate_search_resumed",
        "research_only_observation_allowed",
        "research_only_observation_approved",
        "paper_shadow_enabled",
        "event_append_enabled",
        "outcome_binding_enabled",
        "scheduler_enabled",
        "production_enabled",
        "broker_action_enabled",
        "daily_report_generated",
        "source_feature_count",
        "pit_gate_ready_count",
        "contract_ready_count",
        "pit_gate_blocked_count",
        "blocked_by_source_traceability_count",
        "blocked_by_valid_until_window_count",
        "source_traceability_evidence_row_count",
        "source_traceability_pre_recheck_evidence_ready_count",
        "source_traceability_still_blocked_count",
        "upstream_artifact_closure_evidence_row_count",
        "upstream_artifact_pre_recheck_evidence_ready_count",
        "upstream_artifact_still_blocked_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_valid_until_dependency_evidence_closure_command(
    source_2417_closure_result_path: Annotated[
        Path, typer.Option("--source-2417-closure-result")
    ] = m2418.DEFAULT_SOURCE_2417_CLOSURE_RESULT_PATH,
    source_2417_source_traceability_closure_evidence_path: Annotated[
        Path, typer.Option("--source-2417-source-traceability-closure-evidence")
    ] = m2418.DEFAULT_SOURCE_2417_SOURCE_TRACEABILITY_CLOSURE_EVIDENCE_PATH,
    source_2417_upstream_artifact_closure_evidence_path: Annotated[
        Path, typer.Option("--source-2417-upstream-artifact-closure-evidence")
    ] = m2418.DEFAULT_SOURCE_2417_UPSTREAM_ARTIFACT_CLOSURE_EVIDENCE_PATH,
    source_2417_updated_source_feature_mapping_path: Annotated[
        Path, typer.Option("--source-2417-updated-source-feature-mapping")
    ] = m2418.DEFAULT_SOURCE_2417_UPDATED_SOURCE_FEATURE_MAPPING_PATH,
    source_2417_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2417-remaining-blocker-summary")
    ] = m2418.DEFAULT_SOURCE_2417_REMAINING_BLOCKER_SUMMARY_PATH,
    source_2416_closure_result_path: Annotated[
        Path, typer.Option("--source-2416-closure-result")
    ] = m2418.DEFAULT_SOURCE_2416_CLOSURE_RESULT_PATH,
    source_2416_remaining_blocker_matrix_path: Annotated[
        Path, typer.Option("--source-2416-remaining-blocker-matrix")
    ] = m2418.DEFAULT_SOURCE_2416_REMAINING_BLOCKER_MATRIX_PATH,
    source_2416_valid_until_dependency_closure_plan_path: Annotated[
        Path, typer.Option("--source-2416-valid-until-dependency-closure-plan")
    ] = m2418.DEFAULT_SOURCE_2416_VALID_UNTIL_DEPENDENCY_CLOSURE_PLAN_PATH,
    source_2416_pit_gate_evidence_requirements_path: Annotated[
        Path, typer.Option("--source-2416-pit-gate-evidence-requirements")
    ] = m2418.DEFAULT_SOURCE_2416_PIT_GATE_EVIDENCE_REQUIREMENTS_PATH,
    source_2415_readiness_snapshot_result_path: Annotated[
        Path, typer.Option("--source-2415-readiness-snapshot-result")
    ] = m2418.DEFAULT_SOURCE_2415_READINESS_SNAPSHOT_RESULT_PATH,
    source_2415_readiness_matrix_path: Annotated[
        Path, typer.Option("--source-2415-readiness-matrix")
    ] = m2418.DEFAULT_SOURCE_2415_READINESS_MATRIX_PATH,
    source_2414_remediation_result_path: Annotated[
        Path, typer.Option("--source-2414-remediation-result")
    ] = m2418.DEFAULT_SOURCE_2414_REMEDIATION_RESULT_PATH,
    source_2414_contract_metadata_path: Annotated[
        Path, typer.Option("--source-2414-contract-metadata")
    ] = m2418.DEFAULT_SOURCE_2414_CONTRACT_METADATA_PATH,
    source_2414_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2414-remaining-blocker-summary")
    ] = m2418.DEFAULT_SOURCE_2414_REMAINING_BLOCKER_SUMMARY_PATH,
    source_2411_remediation_plan_result_path: Annotated[
        Path, typer.Option("--source-2411-remediation-plan-result")
    ] = m2418.DEFAULT_SOURCE_2411_REMEDIATION_PLAN_RESULT_PATH,
    source_2407_remediation_plan_result_path: Annotated[
        Path, typer.Option("--source-2407-remediation-plan-result")
    ] = m2418.DEFAULT_SOURCE_2407_REMEDIATION_PLAN_RESULT_PATH,
    source_2407_valid_until_semantics_review_path: Annotated[
        Path, typer.Option("--source-2407-valid-until-semantics-review")
    ] = m2418.DEFAULT_SOURCE_2407_VALID_UNTIL_SEMANTICS_REVIEW_PATH,
    source_2407_stale_signal_risk_audit_path: Annotated[
        Path, typer.Option("--source-2407-stale-signal-risk-audit")
    ] = m2418.DEFAULT_SOURCE_2407_STALE_SIGNAL_RISK_AUDIT_PATH,
    source_2407_signal_validity_contract_plan_path: Annotated[
        Path, typer.Option("--source-2407-signal-validity-contract-plan")
    ] = m2418.DEFAULT_SOURCE_2407_SIGNAL_VALIDITY_CONTRACT_PLAN_PATH,
    source_2407_validation_plan_path: Annotated[
        Path, typer.Option("--source-2407-validation-plan")
    ] = m2418.DEFAULT_SOURCE_2407_VALIDATION_PLAN_PATH,
    pit_input_registry_path: Annotated[
        Path, typer.Option("--pit-input-registry")
    ] = m2418.DEFAULT_PIT_INPUT_REGISTRY_PATH,
    strategy_execution_policy_registry_path: Annotated[
        Path, typer.Option("--strategy-execution-policy-registry")
    ] = m2418.DEFAULT_STRATEGY_EXECUTION_POLICY_REGISTRY_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2418.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2418.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = (
        m2418.DEFAULT_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_OUTPUT_ROOT
    ),
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = (
        m2418.DEFAULT_GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_DOCS_ROOT
    ),
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2418.run_growth_tilt_engine_valid_until_dependency_evidence_closure(
        source_2417_closure_result_path=source_2417_closure_result_path,
        source_2417_source_traceability_closure_evidence_path=(
            source_2417_source_traceability_closure_evidence_path
        ),
        source_2417_upstream_artifact_closure_evidence_path=(
            source_2417_upstream_artifact_closure_evidence_path
        ),
        source_2417_updated_source_feature_mapping_path=(
            source_2417_updated_source_feature_mapping_path
        ),
        source_2417_remaining_blocker_summary_path=(
            source_2417_remaining_blocker_summary_path
        ),
        source_2416_closure_result_path=source_2416_closure_result_path,
        source_2416_remaining_blocker_matrix_path=(
            source_2416_remaining_blocker_matrix_path
        ),
        source_2416_valid_until_dependency_closure_plan_path=(
            source_2416_valid_until_dependency_closure_plan_path
        ),
        source_2416_pit_gate_evidence_requirements_path=(
            source_2416_pit_gate_evidence_requirements_path
        ),
        source_2415_readiness_snapshot_result_path=(
            source_2415_readiness_snapshot_result_path
        ),
        source_2415_readiness_matrix_path=source_2415_readiness_matrix_path,
        source_2414_remediation_result_path=source_2414_remediation_result_path,
        source_2414_contract_metadata_path=source_2414_contract_metadata_path,
        source_2414_remaining_blocker_summary_path=(
            source_2414_remaining_blocker_summary_path
        ),
        source_2411_remediation_plan_result_path=(
            source_2411_remediation_plan_result_path
        ),
        source_2407_remediation_plan_result_path=(
            source_2407_remediation_plan_result_path
        ),
        source_2407_valid_until_semantics_review_path=(
            source_2407_valid_until_semantics_review_path
        ),
        source_2407_stale_signal_risk_audit_path=(
            source_2407_stale_signal_risk_audit_path
        ),
        source_2407_signal_validity_contract_plan_path=(
            source_2407_signal_validity_contract_plan_path
        ),
        source_2407_validation_plan_path=source_2407_validation_plan_path,
        pit_input_registry_path=pit_input_registry_path,
        strategy_execution_policy_registry_path=(
            strategy_execution_policy_registry_path
        ),
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine valid-until dependency evidence closure",
        payload,
    )
    for field in (
        "valid_until_dependency_evidence_ready",
        "signal_validity_contract_evidence_ready",
        "stale_signal_policy_evidence_ready",
        "growth_tilt_valid_until_alignment_evidence_ready",
        "remaining_blocker_summary_ready",
        "pit_gate_recheck_required",
        "auto_mark_pit_gate_ready",
        "auto_mark_contract_ready",
        "auto_downgrade_blocker",
        "growth_tilt_engine_blocking_gap_resolved",
        "growth_tilt_engine_severity_downgraded",
        "valid_until_window_blocking_gap_resolved",
        "valid_until_window_severity_downgraded",
        "candidate_search_allowed",
        "candidate_search_resumed",
        "research_only_observation_allowed",
        "research_only_observation_approved",
        "paper_shadow_enabled",
        "event_append_enabled",
        "outcome_binding_enabled",
        "scheduler_enabled",
        "production_enabled",
        "broker_action_enabled",
        "daily_report_generated",
        "source_feature_count",
        "pit_gate_ready_count",
        "contract_ready_count",
        "pit_gate_blocked_count",
        "blocked_by_source_traceability_count",
        "blocked_by_valid_until_window_count",
        "valid_until_window_dependency_blocker_count_from_2415",
        "valid_until_dependency_evidence_row_count",
        "valid_until_dependency_pre_recheck_evidence_ready_count",
        "valid_until_dependency_still_blocked_count",
        "source_traceability_still_blocked",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_pit_gate_readiness_recheck_command(
    source_2418_closure_result_path: Annotated[
        Path, typer.Option("--source-2418-closure-result")
    ] = m2419.DEFAULT_SOURCE_2418_CLOSURE_RESULT_PATH,
    source_2418_valid_until_dependency_evidence_path: Annotated[
        Path, typer.Option("--source-2418-valid-until-dependency-evidence")
    ] = m2419.DEFAULT_SOURCE_2418_VALID_UNTIL_DEPENDENCY_EVIDENCE_PATH,
    source_2418_signal_validity_contract_evidence_path: Annotated[
        Path, typer.Option("--source-2418-signal-validity-contract-evidence")
    ] = m2419.DEFAULT_SOURCE_2418_SIGNAL_VALIDITY_CONTRACT_EVIDENCE_PATH,
    source_2418_stale_signal_policy_evidence_path: Annotated[
        Path, typer.Option("--source-2418-stale-signal-policy-evidence")
    ] = m2419.DEFAULT_SOURCE_2418_STALE_SIGNAL_POLICY_EVIDENCE_PATH,
    source_2418_growth_tilt_valid_until_alignment_evidence_path: Annotated[
        Path, typer.Option("--source-2418-growth-tilt-valid-until-alignment-evidence")
    ] = m2419.DEFAULT_SOURCE_2418_GROWTH_TILT_VALID_UNTIL_ALIGNMENT_EVIDENCE_PATH,
    source_2418_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2418-remaining-blocker-summary")
    ] = m2419.DEFAULT_SOURCE_2418_REMAINING_BLOCKER_SUMMARY_PATH,
    source_2418_research_doc_path: Annotated[
        Path, typer.Option("--source-2418-research-doc")
    ] = m2419.DEFAULT_SOURCE_2418_RESEARCH_DOC_PATH,
    source_2418_route_doc_path: Annotated[
        Path, typer.Option("--source-2418-route-doc")
    ] = m2419.DEFAULT_SOURCE_2418_ROUTE_DOC_PATH,
    source_2417_closure_result_path: Annotated[
        Path, typer.Option("--source-2417-closure-result")
    ] = m2419.DEFAULT_SOURCE_2417_CLOSURE_RESULT_PATH,
    source_2417_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2417-remaining-blocker-summary")
    ] = m2419.DEFAULT_SOURCE_2417_REMAINING_BLOCKER_SUMMARY_PATH,
    source_2416_closure_result_path: Annotated[
        Path, typer.Option("--source-2416-closure-result")
    ] = m2419.DEFAULT_SOURCE_2416_CLOSURE_RESULT_PATH,
    source_2416_remaining_blocker_matrix_path: Annotated[
        Path, typer.Option("--source-2416-remaining-blocker-matrix")
    ] = m2419.DEFAULT_SOURCE_2416_REMAINING_BLOCKER_MATRIX_PATH,
    source_2416_pit_gate_evidence_requirements_path: Annotated[
        Path, typer.Option("--source-2416-pit-gate-evidence-requirements")
    ] = m2419.DEFAULT_SOURCE_2416_PIT_GATE_EVIDENCE_REQUIREMENTS_PATH,
    source_2415_readiness_snapshot_result_path: Annotated[
        Path, typer.Option("--source-2415-readiness-snapshot-result")
    ] = m2419.DEFAULT_SOURCE_2415_READINESS_SNAPSHOT_RESULT_PATH,
    source_2415_readiness_matrix_path: Annotated[
        Path, typer.Option("--source-2415-readiness-matrix")
    ] = m2419.DEFAULT_SOURCE_2415_READINESS_MATRIX_PATH,
    source_2415_readiness_validation_path: Annotated[
        Path, typer.Option("--source-2415-readiness-validation")
    ] = m2419.DEFAULT_SOURCE_2415_READINESS_VALIDATION_PATH,
    source_2415_remaining_blocker_summary_path: Annotated[
        Path, typer.Option("--source-2415-remaining-blocker-summary")
    ] = m2419.DEFAULT_SOURCE_2415_REMAINING_BLOCKER_SUMMARY_PATH,
    pit_input_registry_path: Annotated[
        Path, typer.Option("--pit-input-registry")
    ] = m2419.DEFAULT_PIT_INPUT_REGISTRY_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2419.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2419.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2419.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2419.DEFAULT_GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2419.run_growth_tilt_engine_pit_gate_readiness_recheck(
        source_2418_closure_result_path=source_2418_closure_result_path,
        source_2418_valid_until_dependency_evidence_path=(
            source_2418_valid_until_dependency_evidence_path
        ),
        source_2418_signal_validity_contract_evidence_path=(
            source_2418_signal_validity_contract_evidence_path
        ),
        source_2418_stale_signal_policy_evidence_path=(
            source_2418_stale_signal_policy_evidence_path
        ),
        source_2418_growth_tilt_valid_until_alignment_evidence_path=(
            source_2418_growth_tilt_valid_until_alignment_evidence_path
        ),
        source_2418_remaining_blocker_summary_path=(
            source_2418_remaining_blocker_summary_path
        ),
        source_2418_research_doc_path=source_2418_research_doc_path,
        source_2418_route_doc_path=source_2418_route_doc_path,
        source_2417_closure_result_path=source_2417_closure_result_path,
        source_2417_remaining_blocker_summary_path=(
            source_2417_remaining_blocker_summary_path
        ),
        source_2416_closure_result_path=source_2416_closure_result_path,
        source_2416_remaining_blocker_matrix_path=(
            source_2416_remaining_blocker_matrix_path
        ),
        source_2416_pit_gate_evidence_requirements_path=(
            source_2416_pit_gate_evidence_requirements_path
        ),
        source_2415_readiness_snapshot_result_path=(
            source_2415_readiness_snapshot_result_path
        ),
        source_2415_readiness_matrix_path=source_2415_readiness_matrix_path,
        source_2415_readiness_validation_path=(
            source_2415_readiness_validation_path
        ),
        source_2415_remaining_blocker_summary_path=(
            source_2415_remaining_blocker_summary_path
        ),
        pit_input_registry_path=pit_input_registry_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine PIT gate readiness recheck",
        payload,
    )
    for field in (
        "readiness_status",
        "pit_gate_ready_count",
        "contract_ready_count",
        "pit_gate_blocked_count",
        "remaining_blocker_count",
        "remaining_blockers",
        "blocker_classification",
        "valid_until_dependency_evidence_ready_from_2418",
        "valid_until_dependency_still_blocked_count_after_recheck",
        "auto_mark_pit_gate_ready",
        "auto_mark_contract_ready",
        "auto_downgrade_blocker",
        "blockers_resolved",
        "blockers_downgraded",
        "signal_artifact_source_traceability_blocker_resolved",
        "signal_artifact_source_traceability_blocker_downgraded",
        "candidate_search_allowed",
        "candidate_search_resumed",
        "research_only_observation_allowed",
        "research_only_observation_approved",
        "paper_shadow_enabled",
        "event_append_enabled",
        "outcome_binding_enabled",
        "scheduler_enabled",
        "production_enabled",
        "broker_enabled",
        "broker_action_enabled",
        "daily_report_generated",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_signal_artifact_source_traceability_remediation_command(
    source_2419_recheck_result_path: Annotated[
        Path, typer.Option("--source-2419-recheck-result")
    ] = m2420.DEFAULT_SOURCE_2419_RECHECK_RESULT_PATH,
    source_2419_blocker_classification_path: Annotated[
        Path, typer.Option("--source-2419-blocker-classification")
    ] = m2420.DEFAULT_SOURCE_2419_BLOCKER_CLASSIFICATION_PATH,
    source_2419_research_doc_path: Annotated[
        Path, typer.Option("--source-2419-research-doc")
    ] = m2420.DEFAULT_SOURCE_2419_RESEARCH_DOC_PATH,
    source_2419_blocker_doc_path: Annotated[
        Path, typer.Option("--source-2419-blocker-doc")
    ] = m2420.DEFAULT_SOURCE_2419_BLOCKER_DOC_PATH,
    source_2418_valid_until_dependency_evidence_path: Annotated[
        Path, typer.Option("--source-2418-valid-until-dependency-evidence")
    ] = m2420.DEFAULT_SOURCE_2418_VALID_UNTIL_DEPENDENCY_EVIDENCE_PATH,
    source_2418_signal_validity_contract_evidence_path: Annotated[
        Path, typer.Option("--source-2418-signal-validity-contract-evidence")
    ] = m2420.DEFAULT_SOURCE_2418_SIGNAL_VALIDITY_CONTRACT_EVIDENCE_PATH,
    source_2418_stale_signal_policy_evidence_path: Annotated[
        Path, typer.Option("--source-2418-stale-signal-policy-evidence")
    ] = m2420.DEFAULT_SOURCE_2418_STALE_SIGNAL_POLICY_EVIDENCE_PATH,
    source_2418_growth_tilt_valid_until_alignment_evidence_path: Annotated[
        Path, typer.Option("--source-2418-growth-tilt-valid-until-alignment-evidence")
    ] = m2420.DEFAULT_SOURCE_2418_GROWTH_TILT_VALID_UNTIL_ALIGNMENT_EVIDENCE_PATH,
    source_2418_research_doc_path: Annotated[
        Path, typer.Option("--source-2418-research-doc")
    ] = m2420.DEFAULT_SOURCE_2418_RESEARCH_DOC_PATH,
    source_2417_source_traceability_closure_evidence_path: Annotated[
        Path, typer.Option("--source-2417-source-traceability-closure-evidence")
    ] = m2420.DEFAULT_SOURCE_2417_SOURCE_TRACEABILITY_CLOSURE_EVIDENCE_PATH,
    source_2417_upstream_artifact_closure_evidence_path: Annotated[
        Path, typer.Option("--source-2417-upstream-artifact-closure-evidence")
    ] = m2420.DEFAULT_SOURCE_2417_UPSTREAM_ARTIFACT_CLOSURE_EVIDENCE_PATH,
    source_2417_source_traceability_doc_path: Annotated[
        Path, typer.Option("--source-2417-source-traceability-doc")
    ] = m2420.DEFAULT_SOURCE_2417_SOURCE_TRACEABILITY_DOC_PATH,
    source_2417_upstream_artifact_doc_path: Annotated[
        Path, typer.Option("--source-2417-upstream-artifact-doc")
    ] = m2420.DEFAULT_SOURCE_2417_UPSTREAM_ARTIFACT_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2420.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2420.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2420.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2420.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2420.run_growth_tilt_engine_signal_artifact_source_traceability_remediation(
        source_2419_recheck_result_path=source_2419_recheck_result_path,
        source_2419_blocker_classification_path=(
            source_2419_blocker_classification_path
        ),
        source_2419_research_doc_path=source_2419_research_doc_path,
        source_2419_blocker_doc_path=source_2419_blocker_doc_path,
        source_2418_valid_until_dependency_evidence_path=(
            source_2418_valid_until_dependency_evidence_path
        ),
        source_2418_signal_validity_contract_evidence_path=(
            source_2418_signal_validity_contract_evidence_path
        ),
        source_2418_stale_signal_policy_evidence_path=(
            source_2418_stale_signal_policy_evidence_path
        ),
        source_2418_growth_tilt_valid_until_alignment_evidence_path=(
            source_2418_growth_tilt_valid_until_alignment_evidence_path
        ),
        source_2418_research_doc_path=source_2418_research_doc_path,
        source_2417_source_traceability_closure_evidence_path=(
            source_2417_source_traceability_closure_evidence_path
        ),
        source_2417_upstream_artifact_closure_evidence_path=(
            source_2417_upstream_artifact_closure_evidence_path
        ),
        source_2417_source_traceability_doc_path=(
            source_2417_source_traceability_doc_path
        ),
        source_2417_upstream_artifact_doc_path=(
            source_2417_upstream_artifact_doc_path
        ),
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine signal artifact source traceability remediation",
        payload,
    )
    for field in (
        "remediation_status",
        "artifact_id",
        "source_traceability_evidence_complete",
        "source_traceability_blocker_resolved",
        "blocker_resolved",
        "blocker_downgraded",
        "pit_gate_ready",
        "contract_ready",
        "pit_gate_ready_count",
        "contract_ready_count",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_enabled",
        "scheduler_enabled",
        "event_append_enabled",
        "outcome_binding_enabled",
        "daily_report_generated",
        "new_signal_generated",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    missing = payload.get("missing_source_evidence_summary") or {}
    if isinstance(missing, dict):
        console.print(f"missing_field_count={missing.get('missing_field_count')}")
        console.print(f"incomplete_field_count={missing.get('incomplete_field_count')}")
        console.print(f"unresolved_blocker_count={missing.get('unresolved_blocker_count')}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_remediation_command(
    source_2420_remediation_result_path: Annotated[
        Path, typer.Option("--source-2420-remediation-result")
    ] = m2421.DEFAULT_SOURCE_2420_REMEDIATION_RESULT_PATH,
    source_2420_source_traceability_manifest_path: Annotated[
        Path, typer.Option("--source-2420-source-traceability-manifest")
    ] = m2421.DEFAULT_SOURCE_2420_SOURCE_TRACEABILITY_MANIFEST_PATH,
    source_2420_source_lineage_map_path: Annotated[
        Path, typer.Option("--source-2420-source-lineage-map")
    ] = m2421.DEFAULT_SOURCE_2420_SOURCE_LINEAGE_MAP_PATH,
    source_2420_missing_source_evidence_summary_path: Annotated[
        Path, typer.Option("--source-2420-missing-source-evidence-summary")
    ] = m2421.DEFAULT_SOURCE_2420_MISSING_SOURCE_EVIDENCE_SUMMARY_PATH,
    source_2420_research_doc_path: Annotated[
        Path, typer.Option("--source-2420-research-doc")
    ] = m2421.DEFAULT_SOURCE_2420_RESEARCH_DOC_PATH,
    source_2420_manifest_doc_path: Annotated[
        Path, typer.Option("--source-2420-manifest-doc")
    ] = m2421.DEFAULT_SOURCE_2420_MANIFEST_DOC_PATH,
    source_2420_lineage_doc_path: Annotated[
        Path, typer.Option("--source-2420-lineage-doc")
    ] = m2421.DEFAULT_SOURCE_2420_LINEAGE_DOC_PATH,
    source_2420_route_doc_path: Annotated[
        Path, typer.Option("--source-2420-route-doc")
    ] = m2421.DEFAULT_SOURCE_2420_ROUTE_DOC_PATH,
    source_2419_recheck_result_path: Annotated[
        Path, typer.Option("--source-2419-recheck-result")
    ] = m2421.DEFAULT_SOURCE_2419_RECHECK_RESULT_PATH,
    source_2419_research_doc_path: Annotated[
        Path, typer.Option("--source-2419-research-doc")
    ] = m2421.DEFAULT_SOURCE_2419_RESEARCH_DOC_PATH,
    source_2419_blocker_doc_path: Annotated[
        Path, typer.Option("--source-2419-blocker-doc")
    ] = m2421.DEFAULT_SOURCE_2419_BLOCKER_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2421.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2421.DEFAULT_ARTIFACT_CATALOG_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2421.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2421.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = (
        m2421.run_growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_remediation(
            source_2420_remediation_result_path=source_2420_remediation_result_path,
            source_2420_source_traceability_manifest_path=(
                source_2420_source_traceability_manifest_path
            ),
            source_2420_source_lineage_map_path=source_2420_source_lineage_map_path,
            source_2420_missing_source_evidence_summary_path=(
                source_2420_missing_source_evidence_summary_path
            ),
            source_2420_research_doc_path=source_2420_research_doc_path,
            source_2420_manifest_doc_path=source_2420_manifest_doc_path,
            source_2420_lineage_doc_path=source_2420_lineage_doc_path,
            source_2420_route_doc_path=source_2420_route_doc_path,
            source_2419_recheck_result_path=source_2419_recheck_result_path,
            source_2419_research_doc_path=source_2419_research_doc_path,
            source_2419_blocker_doc_path=source_2419_blocker_doc_path,
            report_registry_path=report_registry_path,
            artifact_catalog_path=artifact_catalog_path,
            output_root=output_root,
            docs_root=docs_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_execution_semantics_payload(
        "Growth tilt engine PIT gate readiness recheck after source traceability remediation",
        payload,
    )
    for field in (
        "readiness_status",
        "source_traceability_remediation_status",
        "source_traceability_recheck_status",
        "source_traceability_evidence_complete_after_2420",
        "source_traceability_blocker_resolved",
        "signal_artifact_source_traceability_blocker_resolved",
        "blockers_resolved",
        "blockers_downgraded",
        "resolved_blockers",
        "remaining_blockers",
        "remaining_blocker_count",
        "pit_gate_ready",
        "pit_gate_ready_count",
        "pit_gate_blocked_count",
        "contract_ready",
        "contract_ready_count",
        "contract_readiness_snapshot_required",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_enabled",
        "scheduler_enabled",
        "event_append_enabled",
        "outcome_binding_enabled",
        "daily_report_generated",
        "new_signal_generated",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
        "blocker_resolution_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_contract_readiness_snapshot_command(
    source_2421_readiness_recheck_result_path: Annotated[
        Path, typer.Option("--source-2421-readiness-recheck-result")
    ] = m2422.DEFAULT_SOURCE_2421_READINESS_RECHECK_RESULT_PATH,
    source_2421_pit_gate_recheck_matrix_path: Annotated[
        Path, typer.Option("--source-2421-pit-gate-recheck-matrix")
    ] = m2422.DEFAULT_SOURCE_2421_PIT_GATE_RECHECK_MATRIX_PATH,
    source_2421_blocker_resolution_summary_path: Annotated[
        Path, typer.Option("--source-2421-blocker-resolution-summary")
    ] = m2422.DEFAULT_SOURCE_2421_BLOCKER_RESOLUTION_SUMMARY_PATH,
    source_2421_contract_readiness_snapshot_gate_path: Annotated[
        Path, typer.Option("--source-2421-contract-readiness-snapshot-gate")
    ] = m2422.DEFAULT_SOURCE_2421_CONTRACT_READINESS_SNAPSHOT_GATE_PATH,
    source_2421_research_doc_path: Annotated[
        Path, typer.Option("--source-2421-research-doc")
    ] = m2422.DEFAULT_SOURCE_2421_RESEARCH_DOC_PATH,
    source_2421_matrix_doc_path: Annotated[
        Path, typer.Option("--source-2421-matrix-doc")
    ] = m2422.DEFAULT_SOURCE_2421_MATRIX_DOC_PATH,
    source_2421_blocker_doc_path: Annotated[
        Path, typer.Option("--source-2421-blocker-doc")
    ] = m2422.DEFAULT_SOURCE_2421_BLOCKER_DOC_PATH,
    source_2421_route_doc_path: Annotated[
        Path, typer.Option("--source-2421-route-doc")
    ] = m2422.DEFAULT_SOURCE_2421_ROUTE_DOC_PATH,
    source_2420_remediation_result_path: Annotated[
        Path, typer.Option("--source-2420-remediation-result")
    ] = m2422.DEFAULT_SOURCE_2420_REMEDIATION_RESULT_PATH,
    source_2420_source_traceability_manifest_path: Annotated[
        Path, typer.Option("--source-2420-source-traceability-manifest")
    ] = m2422.DEFAULT_SOURCE_2420_SOURCE_TRACEABILITY_MANIFEST_PATH,
    source_2420_source_lineage_map_path: Annotated[
        Path, typer.Option("--source-2420-source-lineage-map")
    ] = m2422.DEFAULT_SOURCE_2420_SOURCE_LINEAGE_MAP_PATH,
    source_2420_missing_source_evidence_summary_path: Annotated[
        Path, typer.Option("--source-2420-missing-source-evidence-summary")
    ] = m2422.DEFAULT_SOURCE_2420_MISSING_SOURCE_EVIDENCE_SUMMARY_PATH,
    source_2420_research_doc_path: Annotated[
        Path, typer.Option("--source-2420-research-doc")
    ] = m2422.DEFAULT_SOURCE_2420_RESEARCH_DOC_PATH,
    source_2420_manifest_doc_path: Annotated[
        Path, typer.Option("--source-2420-manifest-doc")
    ] = m2422.DEFAULT_SOURCE_2420_MANIFEST_DOC_PATH,
    source_2420_lineage_doc_path: Annotated[
        Path, typer.Option("--source-2420-lineage-doc")
    ] = m2422.DEFAULT_SOURCE_2420_LINEAGE_DOC_PATH,
    source_2420_route_doc_path: Annotated[
        Path, typer.Option("--source-2420-route-doc")
    ] = m2422.DEFAULT_SOURCE_2420_ROUTE_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2422.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2422.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2422.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2422.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2422.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2422.run_growth_tilt_engine_contract_readiness_snapshot(
        source_2421_readiness_recheck_result_path=(
            source_2421_readiness_recheck_result_path
        ),
        source_2421_pit_gate_recheck_matrix_path=(
            source_2421_pit_gate_recheck_matrix_path
        ),
        source_2421_blocker_resolution_summary_path=(
            source_2421_blocker_resolution_summary_path
        ),
        source_2421_contract_readiness_snapshot_gate_path=(
            source_2421_contract_readiness_snapshot_gate_path
        ),
        source_2421_research_doc_path=source_2421_research_doc_path,
        source_2421_matrix_doc_path=source_2421_matrix_doc_path,
        source_2421_blocker_doc_path=source_2421_blocker_doc_path,
        source_2421_route_doc_path=source_2421_route_doc_path,
        source_2420_remediation_result_path=source_2420_remediation_result_path,
        source_2420_source_traceability_manifest_path=(
            source_2420_source_traceability_manifest_path
        ),
        source_2420_source_lineage_map_path=source_2420_source_lineage_map_path,
        source_2420_missing_source_evidence_summary_path=(
            source_2420_missing_source_evidence_summary_path
        ),
        source_2420_research_doc_path=source_2420_research_doc_path,
        source_2420_manifest_doc_path=source_2420_manifest_doc_path,
        source_2420_lineage_doc_path=source_2420_lineage_doc_path,
        source_2420_route_doc_path=source_2420_route_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine contract readiness snapshot",
        payload,
    )
    for field in (
        "readiness_status",
        "pit_gate_ready",
        "pit_gate_ready_count",
        "pit_gate_blocked_count",
        "remaining_blockers",
        "remaining_blocker_count",
        "source_traceability_remediation_status",
        "source_traceability_recheck_status",
        "source_traceability_evidence_complete_after_2420",
        "contract_ready",
        "contract_ready_count",
        "contract_gap_count",
        "missing_contract_evidence_count",
        "incomplete_contract_field_count",
        "contract_requirement_count",
        "contract_requirement_pass_count",
        "contract_requirement_fail_count",
        "paper_shadow_preflight_required",
        "paper_shadow_preflight_started",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_enabled",
        "scheduler_enabled",
        "event_append_enabled",
        "outcome_binding_enabled",
        "daily_report_generated",
        "new_signal_generated",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_paper_shadow_preflight_command(
    source_2422_contract_readiness_snapshot_path: Annotated[
        Path, typer.Option("--source-2422-contract-readiness-snapshot")
    ] = m2423.DEFAULT_SOURCE_2422_CONTRACT_READINESS_SNAPSHOT_PATH,
    source_2422_contract_evidence_map_path: Annotated[
        Path, typer.Option("--source-2422-contract-evidence-map")
    ] = m2423.DEFAULT_SOURCE_2422_CONTRACT_EVIDENCE_MAP_PATH,
    source_2422_contract_gap_summary_path: Annotated[
        Path, typer.Option("--source-2422-contract-gap-summary")
    ] = m2423.DEFAULT_SOURCE_2422_CONTRACT_GAP_SUMMARY_PATH,
    source_2422_contract_requirements_path: Annotated[
        Path, typer.Option("--source-2422-contract-requirements")
    ] = m2423.DEFAULT_SOURCE_2422_CONTRACT_REQUIREMENTS_PATH,
    source_2422_research_doc_path: Annotated[
        Path, typer.Option("--source-2422-research-doc")
    ] = m2423.DEFAULT_SOURCE_2422_RESEARCH_DOC_PATH,
    source_2422_evidence_map_doc_path: Annotated[
        Path, typer.Option("--source-2422-evidence-map-doc")
    ] = m2423.DEFAULT_SOURCE_2422_EVIDENCE_MAP_DOC_PATH,
    source_2422_gap_summary_doc_path: Annotated[
        Path, typer.Option("--source-2422-gap-summary-doc")
    ] = m2423.DEFAULT_SOURCE_2422_GAP_SUMMARY_DOC_PATH,
    source_2422_route_doc_path: Annotated[
        Path, typer.Option("--source-2422-route-doc")
    ] = m2423.DEFAULT_SOURCE_2422_ROUTE_DOC_PATH,
    source_2421_readiness_recheck_result_path: Annotated[
        Path, typer.Option("--source-2421-readiness-recheck-result")
    ] = m2423.DEFAULT_SOURCE_2421_READINESS_RECHECK_RESULT_PATH,
    source_2421_pit_gate_recheck_matrix_path: Annotated[
        Path, typer.Option("--source-2421-pit-gate-recheck-matrix")
    ] = m2423.DEFAULT_SOURCE_2421_PIT_GATE_RECHECK_MATRIX_PATH,
    source_2421_blocker_resolution_summary_path: Annotated[
        Path, typer.Option("--source-2421-blocker-resolution-summary")
    ] = m2423.DEFAULT_SOURCE_2421_BLOCKER_RESOLUTION_SUMMARY_PATH,
    source_2421_contract_readiness_snapshot_gate_path: Annotated[
        Path, typer.Option("--source-2421-contract-readiness-snapshot-gate")
    ] = m2423.DEFAULT_SOURCE_2421_CONTRACT_READINESS_SNAPSHOT_GATE_PATH,
    source_2421_research_doc_path: Annotated[
        Path, typer.Option("--source-2421-research-doc")
    ] = m2423.DEFAULT_SOURCE_2421_RESEARCH_DOC_PATH,
    source_2421_matrix_doc_path: Annotated[
        Path, typer.Option("--source-2421-matrix-doc")
    ] = m2423.DEFAULT_SOURCE_2421_MATRIX_DOC_PATH,
    source_2421_blocker_doc_path: Annotated[
        Path, typer.Option("--source-2421-blocker-doc")
    ] = m2423.DEFAULT_SOURCE_2421_BLOCKER_DOC_PATH,
    source_2421_route_doc_path: Annotated[
        Path, typer.Option("--source-2421-route-doc")
    ] = m2423.DEFAULT_SOURCE_2421_ROUTE_DOC_PATH,
    source_2420_remediation_result_path: Annotated[
        Path, typer.Option("--source-2420-remediation-result")
    ] = m2423.DEFAULT_SOURCE_2420_REMEDIATION_RESULT_PATH,
    source_2420_source_traceability_manifest_path: Annotated[
        Path, typer.Option("--source-2420-source-traceability-manifest")
    ] = m2423.DEFAULT_SOURCE_2420_SOURCE_TRACEABILITY_MANIFEST_PATH,
    source_2420_source_lineage_map_path: Annotated[
        Path, typer.Option("--source-2420-source-lineage-map")
    ] = m2423.DEFAULT_SOURCE_2420_SOURCE_LINEAGE_MAP_PATH,
    source_2420_missing_source_evidence_summary_path: Annotated[
        Path, typer.Option("--source-2420-missing-source-evidence-summary")
    ] = m2423.DEFAULT_SOURCE_2420_MISSING_SOURCE_EVIDENCE_SUMMARY_PATH,
    source_2420_research_doc_path: Annotated[
        Path, typer.Option("--source-2420-research-doc")
    ] = m2423.DEFAULT_SOURCE_2420_RESEARCH_DOC_PATH,
    source_2420_manifest_doc_path: Annotated[
        Path, typer.Option("--source-2420-manifest-doc")
    ] = m2423.DEFAULT_SOURCE_2420_MANIFEST_DOC_PATH,
    source_2420_lineage_doc_path: Annotated[
        Path, typer.Option("--source-2420-lineage-doc")
    ] = m2423.DEFAULT_SOURCE_2420_LINEAGE_DOC_PATH,
    source_2420_route_doc_path: Annotated[
        Path, typer.Option("--source-2420-route-doc")
    ] = m2423.DEFAULT_SOURCE_2420_ROUTE_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2423.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2423.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2423.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2423.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2423.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2423.run_growth_tilt_engine_paper_shadow_preflight(
        source_2422_contract_readiness_snapshot_path=(
            source_2422_contract_readiness_snapshot_path
        ),
        source_2422_contract_evidence_map_path=source_2422_contract_evidence_map_path,
        source_2422_contract_gap_summary_path=source_2422_contract_gap_summary_path,
        source_2422_contract_requirements_path=source_2422_contract_requirements_path,
        source_2422_research_doc_path=source_2422_research_doc_path,
        source_2422_evidence_map_doc_path=source_2422_evidence_map_doc_path,
        source_2422_gap_summary_doc_path=source_2422_gap_summary_doc_path,
        source_2422_route_doc_path=source_2422_route_doc_path,
        source_2421_readiness_recheck_result_path=(
            source_2421_readiness_recheck_result_path
        ),
        source_2421_pit_gate_recheck_matrix_path=(
            source_2421_pit_gate_recheck_matrix_path
        ),
        source_2421_blocker_resolution_summary_path=(
            source_2421_blocker_resolution_summary_path
        ),
        source_2421_contract_readiness_snapshot_gate_path=(
            source_2421_contract_readiness_snapshot_gate_path
        ),
        source_2421_research_doc_path=source_2421_research_doc_path,
        source_2421_matrix_doc_path=source_2421_matrix_doc_path,
        source_2421_blocker_doc_path=source_2421_blocker_doc_path,
        source_2421_route_doc_path=source_2421_route_doc_path,
        source_2420_remediation_result_path=source_2420_remediation_result_path,
        source_2420_source_traceability_manifest_path=(
            source_2420_source_traceability_manifest_path
        ),
        source_2420_source_lineage_map_path=source_2420_source_lineage_map_path,
        source_2420_missing_source_evidence_summary_path=(
            source_2420_missing_source_evidence_summary_path
        ),
        source_2420_research_doc_path=source_2420_research_doc_path,
        source_2420_manifest_doc_path=source_2420_manifest_doc_path,
        source_2420_lineage_doc_path=source_2420_lineage_doc_path,
        source_2420_route_doc_path=source_2420_route_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine paper-shadow preflight",
        payload,
    )
    for field in (
        "readiness_status",
        "pit_gate_ready",
        "pit_gate_ready_count",
        "remaining_pit_blockers",
        "remaining_pit_blocker_count",
        "contract_readiness_status",
        "contract_ready",
        "contract_ready_count",
        "contract_gap_count",
        "source_traceability_remediation_status",
        "source_traceability_recheck_status",
        "source_traceability_accepted",
        "paper_shadow_preflight_started",
        "paper_shadow_preflight_completed",
        "paper_shadow_preflight_ready",
        "preflight_gap_count",
        "missing_preflight_evidence_count",
        "safety_boundary_gap_count",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "production_enabled",
        "broker_enabled",
        "generated_signal",
        "generated_trading_advice",
        "daily_report_generated",
        "new_signal_generated",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_paper_shadow_enablement_plan_command(
    source_2423_preflight_result_path: Annotated[
        Path, typer.Option("--source-2423-preflight-result")
    ] = m2424.DEFAULT_SOURCE_2423_PREFLIGHT_RESULT_PATH,
    source_2423_preflight_checklist_path: Annotated[
        Path, typer.Option("--source-2423-preflight-checklist")
    ] = m2424.DEFAULT_SOURCE_2423_PREFLIGHT_CHECKLIST_PATH,
    source_2423_preflight_gap_summary_path: Annotated[
        Path, typer.Option("--source-2423-preflight-gap-summary")
    ] = m2424.DEFAULT_SOURCE_2423_PREFLIGHT_GAP_SUMMARY_PATH,
    source_2423_research_doc_path: Annotated[
        Path, typer.Option("--source-2423-research-doc")
    ] = m2424.DEFAULT_SOURCE_2423_RESEARCH_DOC_PATH,
    source_2423_checklist_doc_path: Annotated[
        Path, typer.Option("--source-2423-checklist-doc")
    ] = m2424.DEFAULT_SOURCE_2423_CHECKLIST_DOC_PATH,
    source_2423_gap_summary_doc_path: Annotated[
        Path, typer.Option("--source-2423-gap-summary-doc")
    ] = m2424.DEFAULT_SOURCE_2423_GAP_SUMMARY_DOC_PATH,
    source_2423_route_doc_path: Annotated[
        Path, typer.Option("--source-2423-route-doc")
    ] = m2424.DEFAULT_SOURCE_2423_ROUTE_DOC_PATH,
    source_2422_contract_readiness_snapshot_path: Annotated[
        Path, typer.Option("--source-2422-contract-readiness-snapshot")
    ] = m2424.DEFAULT_SOURCE_2422_CONTRACT_READINESS_SNAPSHOT_PATH,
    source_2422_research_doc_path: Annotated[
        Path, typer.Option("--source-2422-research-doc")
    ] = m2424.DEFAULT_SOURCE_2422_RESEARCH_DOC_PATH,
    source_2422_route_doc_path: Annotated[
        Path, typer.Option("--source-2422-route-doc")
    ] = m2424.DEFAULT_SOURCE_2422_ROUTE_DOC_PATH,
    source_2421_readiness_recheck_result_path: Annotated[
        Path, typer.Option("--source-2421-readiness-recheck-result")
    ] = m2424.DEFAULT_SOURCE_2421_READINESS_RECHECK_RESULT_PATH,
    source_2421_research_doc_path: Annotated[
        Path, typer.Option("--source-2421-research-doc")
    ] = m2424.DEFAULT_SOURCE_2421_RESEARCH_DOC_PATH,
    source_2421_route_doc_path: Annotated[
        Path, typer.Option("--source-2421-route-doc")
    ] = m2424.DEFAULT_SOURCE_2421_ROUTE_DOC_PATH,
    source_2420_remediation_result_path: Annotated[
        Path, typer.Option("--source-2420-remediation-result")
    ] = m2424.DEFAULT_SOURCE_2420_REMEDIATION_RESULT_PATH,
    source_2420_source_traceability_manifest_path: Annotated[
        Path, typer.Option("--source-2420-source-traceability-manifest")
    ] = m2424.DEFAULT_SOURCE_2420_SOURCE_TRACEABILITY_MANIFEST_PATH,
    source_2420_source_lineage_map_path: Annotated[
        Path, typer.Option("--source-2420-source-lineage-map")
    ] = m2424.DEFAULT_SOURCE_2420_SOURCE_LINEAGE_MAP_PATH,
    source_2420_missing_source_evidence_summary_path: Annotated[
        Path, typer.Option("--source-2420-missing-source-evidence-summary")
    ] = m2424.DEFAULT_SOURCE_2420_MISSING_SOURCE_EVIDENCE_SUMMARY_PATH,
    source_2420_research_doc_path: Annotated[
        Path, typer.Option("--source-2420-research-doc")
    ] = m2424.DEFAULT_SOURCE_2420_RESEARCH_DOC_PATH,
    source_2420_manifest_doc_path: Annotated[
        Path, typer.Option("--source-2420-manifest-doc")
    ] = m2424.DEFAULT_SOURCE_2420_MANIFEST_DOC_PATH,
    source_2420_lineage_doc_path: Annotated[
        Path, typer.Option("--source-2420-lineage-doc")
    ] = m2424.DEFAULT_SOURCE_2420_LINEAGE_DOC_PATH,
    source_2420_route_doc_path: Annotated[
        Path, typer.Option("--source-2420-route-doc")
    ] = m2424.DEFAULT_SOURCE_2420_ROUTE_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2424.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2424.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2424.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2424.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2424.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2424.run_growth_tilt_engine_paper_shadow_enablement_plan(
        source_2423_preflight_result_path=source_2423_preflight_result_path,
        source_2423_preflight_checklist_path=source_2423_preflight_checklist_path,
        source_2423_preflight_gap_summary_path=(
            source_2423_preflight_gap_summary_path
        ),
        source_2423_research_doc_path=source_2423_research_doc_path,
        source_2423_checklist_doc_path=source_2423_checklist_doc_path,
        source_2423_gap_summary_doc_path=source_2423_gap_summary_doc_path,
        source_2423_route_doc_path=source_2423_route_doc_path,
        source_2422_contract_readiness_snapshot_path=(
            source_2422_contract_readiness_snapshot_path
        ),
        source_2422_research_doc_path=source_2422_research_doc_path,
        source_2422_route_doc_path=source_2422_route_doc_path,
        source_2421_readiness_recheck_result_path=(
            source_2421_readiness_recheck_result_path
        ),
        source_2421_research_doc_path=source_2421_research_doc_path,
        source_2421_route_doc_path=source_2421_route_doc_path,
        source_2420_remediation_result_path=source_2420_remediation_result_path,
        source_2420_source_traceability_manifest_path=(
            source_2420_source_traceability_manifest_path
        ),
        source_2420_source_lineage_map_path=source_2420_source_lineage_map_path,
        source_2420_missing_source_evidence_summary_path=(
            source_2420_missing_source_evidence_summary_path
        ),
        source_2420_research_doc_path=source_2420_research_doc_path,
        source_2420_manifest_doc_path=source_2420_manifest_doc_path,
        source_2420_lineage_doc_path=source_2420_lineage_doc_path,
        source_2420_route_doc_path=source_2420_route_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine paper-shadow enablement plan",
        payload,
    )
    for field in (
        "readiness_status",
        "pit_gate_ready",
        "pit_gate_ready_count",
        "remaining_pit_blockers",
        "remaining_pit_blocker_count",
        "contract_readiness_status",
        "contract_ready",
        "contract_ready_count",
        "contract_gap_count",
        "source_traceability_remediation_status",
        "source_traceability_recheck_status",
        "source_traceability_accepted",
        "paper_shadow_preflight_ready",
        "paper_shadow_enablement_plan_started",
        "paper_shadow_enablement_plan_completed",
        "enablement_plan_ready",
        "enablement_gap_count",
        "missing_enablement_evidence_count",
        "safety_boundary_gap_count",
        "preflight_or_contract_gap_count",
        "dry_run_wiring_allowed",
        "paper_shadow_schedule_dry_run_allowed",
        "manual_review_required",
        "automatic_execution_allowed",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "production_enabled",
        "broker_enabled",
        "generated_signal",
        "generated_trading_advice",
        "daily_report_generated",
        "daily_report_run",
        "new_signal_generated",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_paper_shadow_dry_run_wiring_command(
    source_2424_enablement_plan_result_path: Annotated[
        Path, typer.Option("--source-2424-enablement-plan-result")
    ] = m2425.DEFAULT_SOURCE_2424_ENABLEMENT_PLAN_RESULT_PATH,
    source_2424_enablement_plan_path: Annotated[
        Path, typer.Option("--source-2424-enablement-plan")
    ] = m2425.DEFAULT_SOURCE_2424_ENABLEMENT_PLAN_PATH,
    source_2424_runtime_boundary_checklist_path: Annotated[
        Path, typer.Option("--source-2424-runtime-boundary-checklist")
    ] = m2425.DEFAULT_SOURCE_2424_RUNTIME_BOUNDARY_CHECKLIST_PATH,
    source_2424_schedule_boundary_plan_path: Annotated[
        Path, typer.Option("--source-2424-schedule-boundary-plan")
    ] = m2425.DEFAULT_SOURCE_2424_SCHEDULE_BOUNDARY_PLAN_PATH,
    source_2424_manual_review_checklist_path: Annotated[
        Path, typer.Option("--source-2424-manual-review-checklist")
    ] = m2425.DEFAULT_SOURCE_2424_MANUAL_REVIEW_CHECKLIST_PATH,
    source_2424_rollback_stop_condition_summary_path: Annotated[
        Path, typer.Option("--source-2424-rollback-stop-condition-summary")
    ] = m2425.DEFAULT_SOURCE_2424_ROLLBACK_STOP_CONDITION_SUMMARY_PATH,
    source_2424_research_doc_path: Annotated[
        Path, typer.Option("--source-2424-research-doc")
    ] = m2425.DEFAULT_SOURCE_2424_RESEARCH_DOC_PATH,
    source_2424_runtime_boundary_doc_path: Annotated[
        Path, typer.Option("--source-2424-runtime-boundary-doc")
    ] = m2425.DEFAULT_SOURCE_2424_RUNTIME_BOUNDARY_DOC_PATH,
    source_2424_schedule_boundary_doc_path: Annotated[
        Path, typer.Option("--source-2424-schedule-boundary-doc")
    ] = m2425.DEFAULT_SOURCE_2424_SCHEDULE_BOUNDARY_DOC_PATH,
    source_2424_manual_review_doc_path: Annotated[
        Path, typer.Option("--source-2424-manual-review-doc")
    ] = m2425.DEFAULT_SOURCE_2424_MANUAL_REVIEW_DOC_PATH,
    source_2424_rollback_doc_path: Annotated[
        Path, typer.Option("--source-2424-rollback-doc")
    ] = m2425.DEFAULT_SOURCE_2424_ROLLBACK_DOC_PATH,
    source_2424_route_doc_path: Annotated[
        Path, typer.Option("--source-2424-route-doc")
    ] = m2425.DEFAULT_SOURCE_2424_ROUTE_DOC_PATH,
    source_2423_preflight_result_path: Annotated[
        Path, typer.Option("--source-2423-preflight-result")
    ] = m2425.DEFAULT_SOURCE_2423_PREFLIGHT_RESULT_PATH,
    source_2423_research_doc_path: Annotated[
        Path, typer.Option("--source-2423-research-doc")
    ] = m2425.DEFAULT_SOURCE_2423_RESEARCH_DOC_PATH,
    source_2423_route_doc_path: Annotated[
        Path, typer.Option("--source-2423-route-doc")
    ] = m2425.DEFAULT_SOURCE_2423_ROUTE_DOC_PATH,
    source_2422_contract_readiness_snapshot_path: Annotated[
        Path, typer.Option("--source-2422-contract-readiness-snapshot")
    ] = m2425.DEFAULT_SOURCE_2422_CONTRACT_READINESS_SNAPSHOT_PATH,
    source_2422_research_doc_path: Annotated[
        Path, typer.Option("--source-2422-research-doc")
    ] = m2425.DEFAULT_SOURCE_2422_RESEARCH_DOC_PATH,
    source_2422_route_doc_path: Annotated[
        Path, typer.Option("--source-2422-route-doc")
    ] = m2425.DEFAULT_SOURCE_2422_ROUTE_DOC_PATH,
    source_2421_readiness_recheck_result_path: Annotated[
        Path, typer.Option("--source-2421-readiness-recheck-result")
    ] = m2425.DEFAULT_SOURCE_2421_READINESS_RECHECK_RESULT_PATH,
    source_2421_research_doc_path: Annotated[
        Path, typer.Option("--source-2421-research-doc")
    ] = m2425.DEFAULT_SOURCE_2421_RESEARCH_DOC_PATH,
    source_2421_route_doc_path: Annotated[
        Path, typer.Option("--source-2421-route-doc")
    ] = m2425.DEFAULT_SOURCE_2421_ROUTE_DOC_PATH,
    source_2420_remediation_result_path: Annotated[
        Path, typer.Option("--source-2420-remediation-result")
    ] = m2425.DEFAULT_SOURCE_2420_REMEDIATION_RESULT_PATH,
    source_2420_source_traceability_manifest_path: Annotated[
        Path, typer.Option("--source-2420-source-traceability-manifest")
    ] = m2425.DEFAULT_SOURCE_2420_SOURCE_TRACEABILITY_MANIFEST_PATH,
    source_2420_source_lineage_map_path: Annotated[
        Path, typer.Option("--source-2420-source-lineage-map")
    ] = m2425.DEFAULT_SOURCE_2420_SOURCE_LINEAGE_MAP_PATH,
    source_2420_missing_source_evidence_summary_path: Annotated[
        Path, typer.Option("--source-2420-missing-source-evidence-summary")
    ] = m2425.DEFAULT_SOURCE_2420_MISSING_SOURCE_EVIDENCE_SUMMARY_PATH,
    source_2420_research_doc_path: Annotated[
        Path, typer.Option("--source-2420-research-doc")
    ] = m2425.DEFAULT_SOURCE_2420_RESEARCH_DOC_PATH,
    source_2420_manifest_doc_path: Annotated[
        Path, typer.Option("--source-2420-manifest-doc")
    ] = m2425.DEFAULT_SOURCE_2420_MANIFEST_DOC_PATH,
    source_2420_lineage_doc_path: Annotated[
        Path, typer.Option("--source-2420-lineage-doc")
    ] = m2425.DEFAULT_SOURCE_2420_LINEAGE_DOC_PATH,
    source_2420_route_doc_path: Annotated[
        Path, typer.Option("--source-2420-route-doc")
    ] = m2425.DEFAULT_SOURCE_2420_ROUTE_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2425.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2425.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2425.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2425.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2425.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2425.run_growth_tilt_engine_paper_shadow_dry_run_wiring(
        source_2424_enablement_plan_result_path=(
            source_2424_enablement_plan_result_path
        ),
        source_2424_enablement_plan_path=source_2424_enablement_plan_path,
        source_2424_runtime_boundary_checklist_path=(
            source_2424_runtime_boundary_checklist_path
        ),
        source_2424_schedule_boundary_plan_path=(
            source_2424_schedule_boundary_plan_path
        ),
        source_2424_manual_review_checklist_path=(
            source_2424_manual_review_checklist_path
        ),
        source_2424_rollback_stop_condition_summary_path=(
            source_2424_rollback_stop_condition_summary_path
        ),
        source_2424_research_doc_path=source_2424_research_doc_path,
        source_2424_runtime_boundary_doc_path=source_2424_runtime_boundary_doc_path,
        source_2424_schedule_boundary_doc_path=source_2424_schedule_boundary_doc_path,
        source_2424_manual_review_doc_path=source_2424_manual_review_doc_path,
        source_2424_rollback_doc_path=source_2424_rollback_doc_path,
        source_2424_route_doc_path=source_2424_route_doc_path,
        source_2423_preflight_result_path=source_2423_preflight_result_path,
        source_2423_research_doc_path=source_2423_research_doc_path,
        source_2423_route_doc_path=source_2423_route_doc_path,
        source_2422_contract_readiness_snapshot_path=(
            source_2422_contract_readiness_snapshot_path
        ),
        source_2422_research_doc_path=source_2422_research_doc_path,
        source_2422_route_doc_path=source_2422_route_doc_path,
        source_2421_readiness_recheck_result_path=(
            source_2421_readiness_recheck_result_path
        ),
        source_2421_research_doc_path=source_2421_research_doc_path,
        source_2421_route_doc_path=source_2421_route_doc_path,
        source_2420_remediation_result_path=source_2420_remediation_result_path,
        source_2420_source_traceability_manifest_path=(
            source_2420_source_traceability_manifest_path
        ),
        source_2420_source_lineage_map_path=source_2420_source_lineage_map_path,
        source_2420_missing_source_evidence_summary_path=(
            source_2420_missing_source_evidence_summary_path
        ),
        source_2420_research_doc_path=source_2420_research_doc_path,
        source_2420_manifest_doc_path=source_2420_manifest_doc_path,
        source_2420_lineage_doc_path=source_2420_lineage_doc_path,
        source_2420_route_doc_path=source_2420_route_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine paper-shadow dry-run wiring",
        payload,
    )
    for field in (
        "readiness_status",
        "pit_gate_ready",
        "pit_gate_ready_count",
        "remaining_pit_blockers",
        "remaining_pit_blocker_count",
        "contract_readiness_status",
        "contract_ready",
        "contract_ready_count",
        "contract_gap_count",
        "source_traceability_remediation_status",
        "source_traceability_recheck_status",
        "source_traceability_accepted",
        "paper_shadow_preflight_ready",
        "enablement_plan_ready",
        "enablement_gap_count",
        "paper_shadow_dry_run_wiring_started",
        "paper_shadow_dry_run_wiring_completed",
        "dry_run_wiring_ready",
        "dry_run_wiring_gap_count",
        "missing_dry_run_evidence_count",
        "safety_boundary_gap_count",
        "wiring_contract_gap_count",
        "precondition_gap_count",
        "input_contract_map_ready",
        "output_artifact_contract_map_ready",
        "manual_review_handoff_wired",
        "schedule_hook_verified_disabled",
        "no_effect_audit_ready",
        "manual_review_required",
        "automatic_execution_allowed",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "generated_signal",
        "generated_trading_advice",
        "daily_report_generated",
        "daily_report_run",
        "new_signal_generated",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_paper_shadow_schedule_dry_run_command(
    source_2425_dry_run_wiring_result_path: Annotated[
        Path, typer.Option("--source-2425-dry-run-wiring-result")
    ] = m2426.DEFAULT_SOURCE_2425_DRY_RUN_WIRING_RESULT_PATH,
    source_2425_schedule_hook_disabled_verification_path: Annotated[
        Path, typer.Option("--source-2425-schedule-hook-disabled-verification")
    ] = m2426.DEFAULT_SOURCE_2425_SCHEDULE_HOOK_DISABLED_VERIFICATION_PATH,
    source_2425_runtime_boundary_manifest_path: Annotated[
        Path, typer.Option("--source-2425-runtime-boundary-manifest")
    ] = m2426.DEFAULT_SOURCE_2425_RUNTIME_BOUNDARY_MANIFEST_PATH,
    source_2425_manual_review_handoff_wiring_plan_path: Annotated[
        Path, typer.Option("--source-2425-manual-review-handoff-wiring-plan")
    ] = m2426.DEFAULT_SOURCE_2425_MANUAL_REVIEW_HANDOFF_WIRING_PLAN_PATH,
    source_2425_dry_run_no_effect_audit_summary_path: Annotated[
        Path, typer.Option("--source-2425-dry-run-no-effect-audit-summary")
    ] = m2426.DEFAULT_SOURCE_2425_DRY_RUN_NO_EFFECT_AUDIT_SUMMARY_PATH,
    source_2425_research_doc_path: Annotated[
        Path, typer.Option("--source-2425-research-doc")
    ] = m2426.DEFAULT_SOURCE_2425_RESEARCH_DOC_PATH,
    source_2425_schedule_hook_doc_path: Annotated[
        Path, typer.Option("--source-2425-schedule-hook-doc")
    ] = m2426.DEFAULT_SOURCE_2425_SCHEDULE_HOOK_DOC_PATH,
    source_2425_runtime_boundary_doc_path: Annotated[
        Path, typer.Option("--source-2425-runtime-boundary-doc")
    ] = m2426.DEFAULT_SOURCE_2425_RUNTIME_BOUNDARY_DOC_PATH,
    source_2425_manual_review_doc_path: Annotated[
        Path, typer.Option("--source-2425-manual-review-doc")
    ] = m2426.DEFAULT_SOURCE_2425_MANUAL_REVIEW_DOC_PATH,
    source_2425_no_effect_audit_doc_path: Annotated[
        Path, typer.Option("--source-2425-no-effect-audit-doc")
    ] = m2426.DEFAULT_SOURCE_2425_NO_EFFECT_AUDIT_DOC_PATH,
    source_2425_route_doc_path: Annotated[
        Path, typer.Option("--source-2425-route-doc")
    ] = m2426.DEFAULT_SOURCE_2425_ROUTE_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2426.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2426.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2426.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2426.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2426.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2426.run_growth_tilt_engine_paper_shadow_schedule_dry_run(
        source_2425_dry_run_wiring_result_path=(
            source_2425_dry_run_wiring_result_path
        ),
        source_2425_schedule_hook_disabled_verification_path=(
            source_2425_schedule_hook_disabled_verification_path
        ),
        source_2425_runtime_boundary_manifest_path=(
            source_2425_runtime_boundary_manifest_path
        ),
        source_2425_manual_review_handoff_wiring_plan_path=(
            source_2425_manual_review_handoff_wiring_plan_path
        ),
        source_2425_dry_run_no_effect_audit_summary_path=(
            source_2425_dry_run_no_effect_audit_summary_path
        ),
        source_2425_research_doc_path=source_2425_research_doc_path,
        source_2425_schedule_hook_doc_path=source_2425_schedule_hook_doc_path,
        source_2425_runtime_boundary_doc_path=source_2425_runtime_boundary_doc_path,
        source_2425_manual_review_doc_path=source_2425_manual_review_doc_path,
        source_2425_no_effect_audit_doc_path=source_2425_no_effect_audit_doc_path,
        source_2425_route_doc_path=source_2425_route_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine paper-shadow schedule dry-run",
        payload,
    )
    for field in (
        "readiness_status",
        "pit_gate_ready",
        "pit_gate_ready_count",
        "contract_ready",
        "contract_ready_count",
        "contract_gap_count",
        "paper_shadow_dry_run_wiring_status",
        "paper_shadow_dry_run_wiring_ready",
        "dry_run_wiring_gap_count",
        "schedule_hook_verified_disabled",
        "runtime_boundary_verified",
        "manual_review_handoff_wired",
        "prior_no_effect_audit_ready",
        "paper_shadow_schedule_dry_run_started",
        "paper_shadow_schedule_dry_run_completed",
        "paper_shadow_schedule_dry_run_ready",
        "schedule_dry_run_plan_ready",
        "schedule_boundary_checklist_ready",
        "schedule_no_effect_audit_ready",
        "schedule_dry_run_gap_count",
        "missing_schedule_evidence_count",
        "safety_boundary_gap_count",
        "schedule_contract_gap_count",
        "precondition_gap_count",
        "manual_review_required",
        "automatic_execution_allowed",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "schedule_hook_invoked",
        "schedule_state_mutated",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "generated_signal",
        "generated_trading_advice",
        "daily_report_generated",
        "daily_report_run",
        "new_signal_generated",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_manual_review_packet_dry_run_command(
    source_2426_schedule_dry_run_result_path: Annotated[
        Path, typer.Option("--source-2426-schedule-dry-run-result")
    ] = m2427.DEFAULT_SOURCE_2426_SCHEDULE_DRY_RUN_RESULT_PATH,
    source_2426_schedule_boundary_checklist_path: Annotated[
        Path, typer.Option("--source-2426-schedule-boundary-checklist")
    ] = m2427.DEFAULT_SOURCE_2426_SCHEDULE_BOUNDARY_CHECKLIST_PATH,
    source_2426_schedule_no_effect_audit_summary_path: Annotated[
        Path, typer.Option("--source-2426-schedule-no-effect-audit-summary")
    ] = m2427.DEFAULT_SOURCE_2426_SCHEDULE_NO_EFFECT_AUDIT_SUMMARY_PATH,
    source_2426_research_doc_path: Annotated[
        Path, typer.Option("--source-2426-research-doc")
    ] = m2427.DEFAULT_SOURCE_2426_RESEARCH_DOC_PATH,
    source_2426_boundary_doc_path: Annotated[
        Path, typer.Option("--source-2426-boundary-doc")
    ] = m2427.DEFAULT_SOURCE_2426_BOUNDARY_DOC_PATH,
    source_2426_no_effect_doc_path: Annotated[
        Path, typer.Option("--source-2426-no-effect-doc")
    ] = m2427.DEFAULT_SOURCE_2426_NO_EFFECT_DOC_PATH,
    source_2426_route_doc_path: Annotated[
        Path, typer.Option("--source-2426-route-doc")
    ] = m2427.DEFAULT_SOURCE_2426_ROUTE_DOC_PATH,
    source_2425_dry_run_wiring_result_path: Annotated[
        Path, typer.Option("--source-2425-dry-run-wiring-result")
    ] = m2427.DEFAULT_SOURCE_2425_DRY_RUN_WIRING_RESULT_PATH,
    source_2425_manual_review_handoff_wiring_plan_path: Annotated[
        Path, typer.Option("--source-2425-manual-review-handoff-wiring-plan")
    ] = m2427.DEFAULT_SOURCE_2425_MANUAL_REVIEW_HANDOFF_WIRING_PLAN_PATH,
    source_2425_research_doc_path: Annotated[
        Path, typer.Option("--source-2425-research-doc")
    ] = m2427.DEFAULT_SOURCE_2425_RESEARCH_DOC_PATH,
    source_2425_manual_review_doc_path: Annotated[
        Path, typer.Option("--source-2425-manual-review-doc")
    ] = m2427.DEFAULT_SOURCE_2425_MANUAL_REVIEW_DOC_PATH,
    source_2425_route_doc_path: Annotated[
        Path, typer.Option("--source-2425-route-doc")
    ] = m2427.DEFAULT_SOURCE_2425_ROUTE_DOC_PATH,
    source_2424_enablement_plan_result_path: Annotated[
        Path, typer.Option("--source-2424-enablement-plan-result")
    ] = m2427.DEFAULT_SOURCE_2424_ENABLEMENT_PLAN_RESULT_PATH,
    source_2424_research_doc_path: Annotated[
        Path, typer.Option("--source-2424-research-doc")
    ] = m2427.DEFAULT_SOURCE_2424_RESEARCH_DOC_PATH,
    source_2424_route_doc_path: Annotated[
        Path, typer.Option("--source-2424-route-doc")
    ] = m2427.DEFAULT_SOURCE_2424_ROUTE_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2427.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2427.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2427.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2427.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2427.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2427.run_growth_tilt_engine_manual_review_packet_dry_run(
        source_2426_schedule_dry_run_result_path=(
            source_2426_schedule_dry_run_result_path
        ),
        source_2426_schedule_boundary_checklist_path=(
            source_2426_schedule_boundary_checklist_path
        ),
        source_2426_schedule_no_effect_audit_summary_path=(
            source_2426_schedule_no_effect_audit_summary_path
        ),
        source_2426_research_doc_path=source_2426_research_doc_path,
        source_2426_boundary_doc_path=source_2426_boundary_doc_path,
        source_2426_no_effect_doc_path=source_2426_no_effect_doc_path,
        source_2426_route_doc_path=source_2426_route_doc_path,
        source_2425_dry_run_wiring_result_path=(
            source_2425_dry_run_wiring_result_path
        ),
        source_2425_manual_review_handoff_wiring_plan_path=(
            source_2425_manual_review_handoff_wiring_plan_path
        ),
        source_2425_research_doc_path=source_2425_research_doc_path,
        source_2425_manual_review_doc_path=source_2425_manual_review_doc_path,
        source_2425_route_doc_path=source_2425_route_doc_path,
        source_2424_enablement_plan_result_path=(
            source_2424_enablement_plan_result_path
        ),
        source_2424_research_doc_path=source_2424_research_doc_path,
        source_2424_route_doc_path=source_2424_route_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine manual review packet dry-run",
        payload,
    )
    for field in (
        "readiness_status",
        "pit_gate_ready",
        "pit_gate_ready_count",
        "contract_ready",
        "contract_ready_count",
        "contract_gap_count",
        "paper_shadow_schedule_dry_run_status",
        "paper_shadow_schedule_dry_run_ready",
        "schedule_dry_run_gap_count",
        "paper_shadow_dry_run_wiring_status",
        "paper_shadow_dry_run_wiring_ready",
        "enablement_plan_status",
        "enablement_plan_ready",
        "manual_review_packet_dry_run_started",
        "manual_review_packet_dry_run_completed",
        "manual_review_packet_dry_run_ready",
        "manual_review_packet_ready",
        "manual_review_checklist_ready",
        "no_advice_boundary_ready",
        "reviewer_handoff_manifest_ready",
        "manual_review_packet_gap_count",
        "missing_manual_review_evidence_count",
        "safety_boundary_gap_count",
        "packet_contract_gap_count",
        "precondition_gap_count",
        "manual_review_required",
        "automatic_execution_allowed",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "daily_report_generated",
        "daily_report_run",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_observe_only_signal_artifact_boundary_command(
    source_2427_manual_review_packet_dry_run_result_path: Annotated[
        Path, typer.Option("--source-2427-manual-review-packet-dry-run-result")
    ] = m2428.DEFAULT_SOURCE_2427_MANUAL_REVIEW_PACKET_DRY_RUN_RESULT_PATH,
    source_2427_manual_review_packet_path: Annotated[
        Path, typer.Option("--source-2427-manual-review-packet")
    ] = m2428.DEFAULT_SOURCE_2427_MANUAL_REVIEW_PACKET_PATH,
    source_2427_manual_review_checklist_path: Annotated[
        Path, typer.Option("--source-2427-manual-review-checklist")
    ] = m2428.DEFAULT_SOURCE_2427_MANUAL_REVIEW_CHECKLIST_PATH,
    source_2427_no_advice_boundary_summary_path: Annotated[
        Path, typer.Option("--source-2427-no-advice-boundary-summary")
    ] = m2428.DEFAULT_SOURCE_2427_NO_ADVICE_BOUNDARY_SUMMARY_PATH,
    source_2427_reviewer_handoff_manifest_path: Annotated[
        Path, typer.Option("--source-2427-reviewer-handoff-manifest")
    ] = m2428.DEFAULT_SOURCE_2427_REVIEWER_HANDOFF_MANIFEST_PATH,
    source_2427_research_doc_path: Annotated[
        Path, typer.Option("--source-2427-research-doc")
    ] = m2428.DEFAULT_SOURCE_2427_RESEARCH_DOC_PATH,
    source_2427_packet_doc_path: Annotated[
        Path, typer.Option("--source-2427-packet-doc")
    ] = m2428.DEFAULT_SOURCE_2427_PACKET_DOC_PATH,
    source_2427_checklist_doc_path: Annotated[
        Path, typer.Option("--source-2427-checklist-doc")
    ] = m2428.DEFAULT_SOURCE_2427_CHECKLIST_DOC_PATH,
    source_2427_no_advice_doc_path: Annotated[
        Path, typer.Option("--source-2427-no-advice-doc")
    ] = m2428.DEFAULT_SOURCE_2427_NO_ADVICE_DOC_PATH,
    source_2427_handoff_doc_path: Annotated[
        Path, typer.Option("--source-2427-handoff-doc")
    ] = m2428.DEFAULT_SOURCE_2427_HANDOFF_DOC_PATH,
    source_2427_route_doc_path: Annotated[
        Path, typer.Option("--source-2427-route-doc")
    ] = m2428.DEFAULT_SOURCE_2427_ROUTE_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2428.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2428.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2428.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2428.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2428.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2428.run_growth_tilt_engine_observe_only_signal_artifact_boundary(
        source_2427_manual_review_packet_dry_run_result_path=(
            source_2427_manual_review_packet_dry_run_result_path
        ),
        source_2427_manual_review_packet_path=(
            source_2427_manual_review_packet_path
        ),
        source_2427_manual_review_checklist_path=(
            source_2427_manual_review_checklist_path
        ),
        source_2427_no_advice_boundary_summary_path=(
            source_2427_no_advice_boundary_summary_path
        ),
        source_2427_reviewer_handoff_manifest_path=(
            source_2427_reviewer_handoff_manifest_path
        ),
        source_2427_research_doc_path=source_2427_research_doc_path,
        source_2427_packet_doc_path=source_2427_packet_doc_path,
        source_2427_checklist_doc_path=source_2427_checklist_doc_path,
        source_2427_no_advice_doc_path=source_2427_no_advice_doc_path,
        source_2427_handoff_doc_path=source_2427_handoff_doc_path,
        source_2427_route_doc_path=source_2427_route_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine observe-only signal artifact boundary",
        payload,
    )
    for field in (
        "readiness_status",
        "pit_gate_ready",
        "pit_gate_ready_count",
        "contract_ready",
        "contract_ready_count",
        "contract_gap_count",
        "manual_review_packet_dry_run_status",
        "manual_review_packet_dry_run_ready",
        "manual_review_packet_gap_count",
        "manual_review_packet_ready",
        "manual_review_checklist_ready",
        "prior_no_advice_boundary_ready",
        "reviewer_handoff_manifest_ready",
        "observe_only_signal_artifact_boundary_started",
        "observe_only_signal_artifact_boundary_completed",
        "observe_only_signal_artifact_boundary_ready",
        "signal_artifact_schema_ready",
        "valid_until_required",
        "valid_until_requirements_ready",
        "source_traceability_required",
        "source_traceability_requirements_ready",
        "pit_contract_manual_review_requirements_ready",
        "no_trading_advice_boundary_ready",
        "observe_only_signal_artifact_boundary_gap_count",
        "missing_observe_only_boundary_evidence_count",
        "safety_boundary_gap_count",
        "signal_artifact_contract_gap_count",
        "precondition_gap_count",
        "manual_review_required",
        "automatic_execution_allowed",
        "signal_artifact_instance_generated",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "daily_report_generated",
        "daily_report_run",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_forward_outcome_binding_boundary_command(
    source_2428_observe_only_boundary_result_path: Annotated[
        Path, typer.Option("--source-2428-observe-only-boundary-result")
    ] = m2429.DEFAULT_SOURCE_2428_OBSERVE_ONLY_BOUNDARY_RESULT_PATH,
    source_2428_signal_artifact_schema_path: Annotated[
        Path, typer.Option("--source-2428-signal-artifact-schema")
    ] = m2429.DEFAULT_SOURCE_2428_SIGNAL_ARTIFACT_SCHEMA_PATH,
    source_2428_valid_until_requirements_path: Annotated[
        Path, typer.Option("--source-2428-valid-until-requirements")
    ] = m2429.DEFAULT_SOURCE_2428_VALID_UNTIL_REQUIREMENTS_PATH,
    source_2428_source_traceability_requirements_path: Annotated[
        Path, typer.Option("--source-2428-source-traceability-requirements")
    ] = m2429.DEFAULT_SOURCE_2428_SOURCE_TRACEABILITY_REQUIREMENTS_PATH,
    source_2428_pit_contract_manual_review_requirements_path: Annotated[
        Path, typer.Option("--source-2428-pit-contract-manual-review-requirements")
    ] = m2429.DEFAULT_SOURCE_2428_PIT_CONTRACT_MANUAL_REVIEW_REQUIREMENTS_PATH,
    source_2428_no_trading_advice_boundary_path: Annotated[
        Path, typer.Option("--source-2428-no-trading-advice-boundary")
    ] = m2429.DEFAULT_SOURCE_2428_NO_TRADING_ADVICE_BOUNDARY_PATH,
    source_2428_research_doc_path: Annotated[
        Path, typer.Option("--source-2428-research-doc")
    ] = m2429.DEFAULT_SOURCE_2428_RESEARCH_DOC_PATH,
    source_2428_schema_doc_path: Annotated[
        Path, typer.Option("--source-2428-schema-doc")
    ] = m2429.DEFAULT_SOURCE_2428_SCHEMA_DOC_PATH,
    source_2428_valid_until_doc_path: Annotated[
        Path, typer.Option("--source-2428-valid-until-doc")
    ] = m2429.DEFAULT_SOURCE_2428_VALID_UNTIL_DOC_PATH,
    source_2428_traceability_doc_path: Annotated[
        Path, typer.Option("--source-2428-traceability-doc")
    ] = m2429.DEFAULT_SOURCE_2428_TRACEABILITY_DOC_PATH,
    source_2428_no_advice_doc_path: Annotated[
        Path, typer.Option("--source-2428-no-advice-doc")
    ] = m2429.DEFAULT_SOURCE_2428_NO_ADVICE_DOC_PATH,
    source_2428_route_doc_path: Annotated[
        Path, typer.Option("--source-2428-route-doc")
    ] = m2429.DEFAULT_SOURCE_2428_ROUTE_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2429.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2429.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2429.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2429.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2429.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2429.run_growth_tilt_engine_forward_outcome_binding_boundary(
        source_2428_observe_only_boundary_result_path=(
            source_2428_observe_only_boundary_result_path
        ),
        source_2428_signal_artifact_schema_path=(
            source_2428_signal_artifact_schema_path
        ),
        source_2428_valid_until_requirements_path=(
            source_2428_valid_until_requirements_path
        ),
        source_2428_source_traceability_requirements_path=(
            source_2428_source_traceability_requirements_path
        ),
        source_2428_pit_contract_manual_review_requirements_path=(
            source_2428_pit_contract_manual_review_requirements_path
        ),
        source_2428_no_trading_advice_boundary_path=(
            source_2428_no_trading_advice_boundary_path
        ),
        source_2428_research_doc_path=source_2428_research_doc_path,
        source_2428_schema_doc_path=source_2428_schema_doc_path,
        source_2428_valid_until_doc_path=source_2428_valid_until_doc_path,
        source_2428_traceability_doc_path=source_2428_traceability_doc_path,
        source_2428_no_advice_doc_path=source_2428_no_advice_doc_path,
        source_2428_route_doc_path=source_2428_route_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine forward outcome binding boundary",
        payload,
    )
    for field in (
        "readiness_status",
        "pit_gate_ready",
        "pit_gate_ready_count",
        "contract_ready",
        "contract_ready_count",
        "observe_only_signal_artifact_boundary_status",
        "observe_only_signal_artifact_boundary_ready",
        "prior_signal_artifact_schema_ready",
        "prior_valid_until_requirements_ready",
        "prior_source_traceability_requirements_ready",
        "prior_pit_contract_manual_review_requirements_ready",
        "prior_no_trading_advice_boundary_ready",
        "forward_outcome_binding_boundary_started",
        "forward_outcome_binding_boundary_completed",
        "forward_outcome_binding_boundary_ready",
        "outcome_horizons",
        "outcome_horizon_rules_ready",
        "outcome_schema_ready",
        "valid_until_binding_ready",
        "outcome_decision_rules_ready",
        "baseline_comparison_ready",
        "signal_to_outcome_linkage_ready",
        "no_effect_boundary_ready",
        "forward_outcome_binding_boundary_gap_count",
        "missing_binding_boundary_evidence_count",
        "safety_boundary_gap_count",
        "outcome_contract_gap_count",
        "precondition_gap_count",
        "manual_review_required",
        "automatic_execution_allowed",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "outcome_backfilled",
        "outcome_binding_executed",
        "outcome_store_mutated",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "daily_report_generated",
        "daily_report_run",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_engine_candidate_promotion_evidence_review_command(
    source_2426_schedule_dry_run_result_path: Annotated[
        Path, typer.Option("--source-2426-schedule-dry-run-result")
    ] = m2430.DEFAULT_SOURCE_2426_SCHEDULE_DRY_RUN_RESULT_PATH,
    source_2427_manual_review_packet_dry_run_result_path: Annotated[
        Path, typer.Option("--source-2427-manual-review-packet-dry-run-result")
    ] = m2430.DEFAULT_SOURCE_2427_MANUAL_REVIEW_PACKET_DRY_RUN_RESULT_PATH,
    source_2428_observe_only_boundary_result_path: Annotated[
        Path, typer.Option("--source-2428-observe-only-boundary-result")
    ] = m2430.DEFAULT_SOURCE_2428_OBSERVE_ONLY_BOUNDARY_RESULT_PATH,
    source_2429_forward_outcome_boundary_result_path: Annotated[
        Path, typer.Option("--source-2429-forward-outcome-boundary-result")
    ] = m2430.DEFAULT_SOURCE_2429_FORWARD_OUTCOME_BOUNDARY_RESULT_PATH,
    candidate_registry_path: Annotated[
        Path, typer.Option("--candidate-registry")
    ] = m2430.DEFAULT_CANDIDATE_REGISTRY_PATH,
    prior_candidate_evidence_path: Annotated[
        Path, typer.Option("--prior-candidate-evidence")
    ] = m2430.DEFAULT_PRIOR_CANDIDATE_EVIDENCE_PATH,
    source_2426_research_doc_path: Annotated[
        Path, typer.Option("--source-2426-research-doc")
    ] = m2430.DEFAULT_SOURCE_2426_RESEARCH_DOC_PATH,
    source_2427_research_doc_path: Annotated[
        Path, typer.Option("--source-2427-research-doc")
    ] = m2430.DEFAULT_SOURCE_2427_RESEARCH_DOC_PATH,
    source_2428_research_doc_path: Annotated[
        Path, typer.Option("--source-2428-research-doc")
    ] = m2430.DEFAULT_SOURCE_2428_RESEARCH_DOC_PATH,
    source_2429_research_doc_path: Annotated[
        Path, typer.Option("--source-2429-research-doc")
    ] = m2430.DEFAULT_SOURCE_2429_RESEARCH_DOC_PATH,
    source_2429_route_doc_path: Annotated[
        Path, typer.Option("--source-2429-route-doc")
    ] = m2430.DEFAULT_SOURCE_2429_ROUTE_DOC_PATH,
    prior_candidate_evidence_doc_path: Annotated[
        Path, typer.Option("--prior-candidate-evidence-doc")
    ] = m2430.DEFAULT_PRIOR_CANDIDATE_EVIDENCE_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2430.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2430.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2430.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2430.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2430.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2430.run_growth_tilt_engine_candidate_promotion_evidence_review(
        source_2426_schedule_dry_run_result_path=(
            source_2426_schedule_dry_run_result_path
        ),
        source_2427_manual_review_packet_dry_run_result_path=(
            source_2427_manual_review_packet_dry_run_result_path
        ),
        source_2428_observe_only_boundary_result_path=(
            source_2428_observe_only_boundary_result_path
        ),
        source_2429_forward_outcome_boundary_result_path=(
            source_2429_forward_outcome_boundary_result_path
        ),
        candidate_registry_path=candidate_registry_path,
        prior_candidate_evidence_path=prior_candidate_evidence_path,
        source_2426_research_doc_path=source_2426_research_doc_path,
        source_2427_research_doc_path=source_2427_research_doc_path,
        source_2428_research_doc_path=source_2428_research_doc_path,
        source_2429_research_doc_path=source_2429_research_doc_path,
        source_2429_route_doc_path=source_2429_route_doc_path,
        prior_candidate_evidence_doc_path=prior_candidate_evidence_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt engine candidate promotion evidence review",
        payload,
    )
    for field in (
        "readiness_status",
        "schedule_dry_run_ready",
        "manual_review_packet_dry_run_ready",
        "observe_only_signal_artifact_boundary_ready",
        "forward_outcome_binding_boundary_ready",
        "candidate_registry_ready",
        "prior_candidate_evidence_ready",
        "promotion_evidence_review_started",
        "promotion_evidence_review_completed",
        "promotion_evidence_review_ready",
        "promotion_candidate_found",
        "promotion_candidate_count",
        "candidate_count",
        "candidate_evidence_matrix_ready",
        "candidate_decision_summary_ready",
        "no_promotion_rationale_ready",
        "engineering_readiness_is_alpha_evidence",
        "paper_shadow_promotion_allowed_by_registry",
        "prior_owner_approved_paper_shadow",
        "prior_owner_approved_observation",
        "promotion_evidence_review_gap_count",
        "missing_promotion_review_evidence_count",
        "safety_boundary_gap_count",
        "candidate_evidence_gap_count",
        "precondition_gap_count",
        "manual_review_required",
        "automatic_execution_allowed",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "outcome_backfilled",
        "outcome_binding_executed",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "daily_report_generated",
        "daily_report_run",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_existing_candidate_evidence_matrix_command(
    source_2430_promotion_review_result_path: Annotated[
        Path, typer.Option("--source-2430-promotion-review-result")
    ] = m2431.DEFAULT_SOURCE_2430_PROMOTION_REVIEW_RESULT_PATH,
    candidate_registry_path: Annotated[
        Path, typer.Option("--candidate-registry")
    ] = m2431.DEFAULT_CANDIDATE_REGISTRY_PATH,
    prior_candidate_evidence_path: Annotated[
        Path, typer.Option("--prior-candidate-evidence")
    ] = m2431.DEFAULT_PRIOR_CANDIDATE_EVIDENCE_PATH,
    prior_component_value_matrix_path: Annotated[
        Path, typer.Option("--prior-component-value-matrix")
    ] = m2431.DEFAULT_PRIOR_COMPONENT_VALUE_MATRIX_PATH,
    component_value_doc_path: Annotated[
        Path, typer.Option("--component-value-doc")
    ] = m2431.DEFAULT_COMPONENT_VALUE_DOC_PATH,
    prior_candidate_evidence_doc_path: Annotated[
        Path, typer.Option("--prior-candidate-evidence-doc")
    ] = m2431.DEFAULT_PRIOR_CANDIDATE_EVIDENCE_DOC_PATH,
    candidate_reclassification_doc_path: Annotated[
        Path, typer.Option("--candidate-reclassification-doc")
    ] = m2431.DEFAULT_CANDIDATE_RECLASSIFICATION_DOC_PATH,
    execution_semantics_review_doc_path: Annotated[
        Path, typer.Option("--execution-semantics-review-doc")
    ] = m2431.DEFAULT_EXECUTION_SEMANTICS_REVIEW_DOC_PATH,
    growth_tilt_signal_doc_path: Annotated[
        Path, typer.Option("--growth-tilt-signal-doc")
    ] = m2431.DEFAULT_GROWTH_TILT_SIGNAL_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2431.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2431.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2431.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2431.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2431.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2431.run_growth_tilt_existing_candidate_evidence_matrix(
        source_2430_promotion_review_result_path=(
            source_2430_promotion_review_result_path
        ),
        candidate_registry_path=candidate_registry_path,
        prior_candidate_evidence_path=prior_candidate_evidence_path,
        prior_component_value_matrix_path=prior_component_value_matrix_path,
        component_value_doc_path=component_value_doc_path,
        prior_candidate_evidence_doc_path=prior_candidate_evidence_doc_path,
        candidate_reclassification_doc_path=candidate_reclassification_doc_path,
        execution_semantics_review_doc_path=execution_semantics_review_doc_path,
        growth_tilt_signal_doc_path=growth_tilt_signal_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt existing candidate evidence matrix",
        payload,
    )
    for field in (
        "readiness_status",
        "source_2430_ready",
        "candidate_registry_ready",
        "prior_candidate_evidence_ready",
        "component_value_evidence_ready",
        "existing_candidate_evidence_matrix_ready",
        "candidate_status_summary_ready",
        "candidate_metric_coverage_ready",
        "no_effect_boundary_ready",
        "candidate_count",
        "required_candidate_group_count",
        "rejected_count",
        "component_value_count",
        "needs_pit_count",
        "promotion_candidate_count",
        "promotion_candidate_found",
        "metric_coverage_available_count",
        "metric_coverage_partial_count",
        "metric_coverage_missing_count",
        "evidence_gap_count",
        "engineering_readiness_is_alpha_evidence",
        "market_data_experiment_run",
        "historical_screen_run",
        "pit_replay_run",
        "candidate_gauntlet_run",
        "manual_review_required",
        "automatic_execution_allowed",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "outcome_backfilled",
        "outcome_binding_executed",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "daily_report_generated",
        "daily_report_run",
        "backtest_run",
        "scoring_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_candidate_gauntlet_command(
    source_2431_existing_candidate_evidence_matrix_path: Annotated[
        Path, typer.Option("--source-2431-existing-candidate-evidence-matrix")
    ] = m2432.DEFAULT_SOURCE_2431_EXISTING_CANDIDATE_EVIDENCE_MATRIX_PATH,
    candidate_set_path: Annotated[
        Path, typer.Option("--candidate-set")
    ] = m2432.DEFAULT_CANDIDATE_SET_PATH,
    existing_candidate_evidence_matrix_doc_path: Annotated[
        Path, typer.Option("--existing-candidate-evidence-matrix-doc")
    ] = m2432.DEFAULT_EXISTING_CANDIDATE_EVIDENCE_MATRIX_DOC_PATH,
    existing_candidate_evidence_matrix_table_doc_path: Annotated[
        Path, typer.Option("--existing-candidate-evidence-matrix-table-doc")
    ] = m2432.DEFAULT_EXISTING_CANDIDATE_EVIDENCE_MATRIX_TABLE_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2432.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2432.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2432.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2432.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2432.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2432.run_growth_tilt_candidate_gauntlet_harness(
        source_2431_existing_candidate_evidence_matrix_path=(
            source_2431_existing_candidate_evidence_matrix_path
        ),
        candidate_set_path=candidate_set_path,
        existing_candidate_evidence_matrix_doc_path=(
            existing_candidate_evidence_matrix_doc_path
        ),
        existing_candidate_evidence_matrix_table_doc_path=(
            existing_candidate_evidence_matrix_table_doc_path
        ),
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt candidate gauntlet harness",
        payload,
    )
    for field in (
        "readiness_status",
        "source_2431_ready",
        "candidate_set_ready",
        "candidate_set_id",
        "harness_ready",
        "baseline_ready",
        "metrics_ready",
        "kill_criteria_ready",
        "promotion_criteria_ready",
        "regime_slices_ready",
        "parameter_plateau_check_ready",
        "ablation_output_ready",
        "candidate_group_count",
        "candidates_tested",
        "required_metric_count",
        "configured_metric_count",
        "kill_criteria_count",
        "promotion_criteria_count",
        "regime_slice_count",
        "parameter_plateau_dimension_count",
        "ablation_output_count",
        "new_investment_threshold_values_set",
        "threshold_policy_required_for_execution",
        "criteria_threshold_values_all_null",
        "contract_gap_count",
        "candidate_gauntlet_run",
        "candidate_batch_screen_run",
        "market_data_experiment_run",
        "historical_screen_run",
        "pit_replay_run",
        "backtest_run",
        "scoring_run",
        "manual_review_required",
        "automatic_execution_allowed",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "outcome_backfilled",
        "outcome_binding_executed",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "daily_report_generated",
        "daily_report_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_false_risk_off_missed_upside_batch_screen_command(
    source_2432_candidate_gauntlet_harness_path: Annotated[
        Path, typer.Option("--source-2432-candidate-gauntlet-harness")
    ] = m2433.DEFAULT_SOURCE_2432_CANDIDATE_GAUNTLET_HARNESS_PATH,
    candidate_set_path: Annotated[
        Path, typer.Option("--candidate-set")
    ] = m2433.DEFAULT_CANDIDATE_SET_PATH,
    candidate_gauntlet_harness_doc_path: Annotated[
        Path, typer.Option("--candidate-gauntlet-harness-doc")
    ] = m2433.DEFAULT_CANDIDATE_GAUNTLET_HARNESS_DOC_PATH,
    candidate_set_2432_doc_path: Annotated[
        Path, typer.Option("--candidate-set-2432-doc")
    ] = m2433.DEFAULT_CANDIDATE_SET_2432_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2433.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2433.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2433.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2433.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2433.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2433.run_growth_tilt_false_risk_off_missed_upside_batch_screen(
        source_2432_candidate_gauntlet_harness_path=(
            source_2432_candidate_gauntlet_harness_path
        ),
        candidate_set_path=candidate_set_path,
        candidate_gauntlet_harness_doc_path=candidate_gauntlet_harness_doc_path,
        candidate_set_2432_doc_path=candidate_set_2432_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt false risk-off missed upside batch screen",
        payload,
    )
    for field in (
        "readiness_status",
        "source_2432_ready",
        "candidate_set_ready",
        "candidate_set_id",
        "batch_screen_ready",
        "candidate_screen_matrix_ready",
        "batch_decision_summary_ready",
        "research_question_coverage_ready",
        "no_effect_boundary_ready",
        "candidate_count",
        "candidates_screened",
        "rejected_count",
        "component_value_count",
        "pit_candidate_count",
        "promotion_candidate_count",
        "promotion_candidate_found",
        "research_question_count",
        "research_question_covered_count",
        "new_investment_threshold_values_set",
        "threshold_policy_required_for_pit_or_promotion",
        "criteria_threshold_values_all_null",
        "computed_new_metrics",
        "screen_contract_gap_count",
        "candidate_batch_screen_run",
        "market_data_candidate_screen_run",
        "market_data_experiment_run",
        "historical_screen_run",
        "pit_replay_run",
        "backtest_run",
        "scoring_run",
        "manual_review_required",
        "automatic_execution_allowed",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "outcome_backfilled",
        "outcome_binding_executed",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "daily_report_generated",
        "daily_report_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_defensive_limited_adjustment_component_validation_command(
    source_2433_batch_screen_path: Annotated[
        Path, typer.Option("--source-2433-batch-screen")
    ] = m2434.DEFAULT_SOURCE_2433_BATCH_SCREEN_PATH,
    batch_screen_doc_path: Annotated[
        Path, typer.Option("--batch-screen-doc")
    ] = m2434.DEFAULT_BATCH_SCREEN_DOC_PATH,
    candidate_screen_matrix_doc_path: Annotated[
        Path, typer.Option("--candidate-screen-matrix-doc")
    ] = m2434.DEFAULT_CANDIDATE_SCREEN_MATRIX_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2434.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2434.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2434.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2434.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2434.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = (
        m2434.run_growth_tilt_defensive_limited_adjustment_component_validation(
            source_2433_batch_screen_path=source_2433_batch_screen_path,
            batch_screen_doc_path=batch_screen_doc_path,
            candidate_screen_matrix_doc_path=candidate_screen_matrix_doc_path,
            report_registry_path=report_registry_path,
            artifact_catalog_path=artifact_catalog_path,
            system_flow_path=system_flow_path,
            output_root=output_root,
            docs_root=docs_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_execution_semantics_payload(
        "Growth tilt defensive limited adjustment component validation",
        payload,
    )
    for field in (
        "readiness_status",
        "source_2433_ready",
        "source_candidate_found",
        "component_validation_ready",
        "component_value_assessment_ready",
        "primary_value_matrix_ready",
        "validation_boundary_ready",
        "component_value_found",
        "candidate_status",
        "promotion_candidate_found",
        "promotion_candidate_count",
        "computed_new_metrics",
        "market_data_component_validation_run",
        "evidence_gap_count",
        "market_data_experiment_run",
        "historical_screen_run",
        "pit_replay_run",
        "backtest_run",
        "scoring_run",
        "manual_review_required",
        "automatic_execution_allowed",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "outcome_backfilled",
        "outcome_binding_executed",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "daily_report_generated",
        "daily_report_run",
        "fresh_market_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    primary_value = payload.get("primary_value")
    if not isinstance(primary_value, list):
        primary_value = []
    console.print(f"primary_value={','.join(str(value) for value in primary_value)}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_valid_until_outcome_hit_rate_study_command(
    source_2434_component_validation_path: Annotated[
        Path, typer.Option("--source-2434-component-validation")
    ] = m2435.DEFAULT_SOURCE_2434_COMPONENT_VALIDATION_PATH,
    source_2418_valid_until_alignment_path: Annotated[
        Path, typer.Option("--source-2418-valid-until-alignment")
    ] = m2435.DEFAULT_SOURCE_2418_VALID_UNTIL_ALIGNMENT_PATH,
    source_2418_stale_signal_policy_path: Annotated[
        Path, typer.Option("--source-2418-stale-signal-policy")
    ] = m2435.DEFAULT_SOURCE_2418_STALE_SIGNAL_POLICY_PATH,
    source_2429_forward_outcome_boundary_path: Annotated[
        Path, typer.Option("--source-2429-forward-outcome-boundary")
    ] = m2435.DEFAULT_SOURCE_2429_FORWARD_OUTCOME_BOUNDARY_PATH,
    candidate_set_2432_path: Annotated[
        Path, typer.Option("--candidate-set-2432")
    ] = m2435.DEFAULT_CANDIDATE_SET_2432_PATH,
    component_validation_doc_path: Annotated[
        Path, typer.Option("--component-validation-doc")
    ] = m2435.DEFAULT_COMPONENT_VALIDATION_DOC_PATH,
    valid_until_alignment_doc_path: Annotated[
        Path, typer.Option("--valid-until-alignment-doc")
    ] = m2435.DEFAULT_VALID_UNTIL_ALIGNMENT_DOC_PATH,
    forward_outcome_boundary_doc_path: Annotated[
        Path, typer.Option("--forward-outcome-boundary-doc")
    ] = m2435.DEFAULT_FORWARD_OUTCOME_BOUNDARY_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2435.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2435.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2435.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2435.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2435.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2435.run_growth_tilt_valid_until_outcome_hit_rate_study(
        source_2434_component_validation_path=source_2434_component_validation_path,
        source_2418_valid_until_alignment_path=source_2418_valid_until_alignment_path,
        source_2418_stale_signal_policy_path=source_2418_stale_signal_policy_path,
        source_2429_forward_outcome_boundary_path=(
            source_2429_forward_outcome_boundary_path
        ),
        candidate_set_2432_path=candidate_set_2432_path,
        component_validation_doc_path=component_validation_doc_path,
        valid_until_alignment_doc_path=valid_until_alignment_doc_path,
        forward_outcome_boundary_doc_path=forward_outcome_boundary_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt valid-until outcome hit-rate study",
        payload,
    )
    for field in (
        "readiness_status",
        "source_2434_ready",
        "source_2418_valid_until_evidence_ready",
        "source_2429_forward_outcome_boundary_ready",
        "candidate_set_valid_until_metric_ready",
        "candidate_set_valid_until_candidate_group_ready",
        "hit_rate_study_ready",
        "valid_until_hit_rate_matrix_ready",
        "stale_signal_reduction_summary_ready",
        "expiry_failure_audit_ready",
        "no_effect_boundary_ready",
        "valid_until_component_value_found",
        "valid_until_hit_rate_delta",
        "stale_signal_reduction",
        "expiry_failure_count",
        "candidate_status",
        "outcome_sample_count",
        "observed_outcome_hit_rate_available",
        "computed_new_metrics",
        "market_data_hit_rate_study_run",
        "real_outcome_binding_run",
        "evidence_gap_count",
        "historical_screen_run",
        "pit_replay_run",
        "backtest_run",
        "scoring_run",
        "manual_review_required",
        "automatic_execution_allowed",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "outcome_backfilled",
        "outcome_binding_executed",
        "outcome_store_mutated",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "daily_report_generated",
        "daily_report_run",
        "fresh_market_data_read",
        "fresh_outcome_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_turnover_cooldown_parameter_plateau_study_command(
    source_2435_hit_rate_study_path: Annotated[
        Path, typer.Option("--source-2435-hit-rate-study")
    ] = m2436.DEFAULT_SOURCE_2435_HIT_RATE_STUDY_PATH,
    source_2432_candidate_gauntlet_path: Annotated[
        Path, typer.Option("--source-2432-candidate-gauntlet")
    ] = m2436.DEFAULT_SOURCE_2432_CANDIDATE_GAUNTLET_PATH,
    candidate_set_2432_path: Annotated[
        Path, typer.Option("--candidate-set-2432")
    ] = m2436.DEFAULT_CANDIDATE_SET_2432_PATH,
    hit_rate_study_doc_path: Annotated[
        Path, typer.Option("--hit-rate-study-doc")
    ] = m2436.DEFAULT_HIT_RATE_STUDY_DOC_PATH,
    candidate_gauntlet_doc_path: Annotated[
        Path, typer.Option("--candidate-gauntlet-doc")
    ] = m2436.DEFAULT_CANDIDATE_GAUNTLET_DOC_PATH,
    candidate_set_2432_doc_path: Annotated[
        Path, typer.Option("--candidate-set-2432-doc")
    ] = m2436.DEFAULT_CANDIDATE_SET_2432_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2436.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2436.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2436.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2436.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2436.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2436.run_growth_tilt_turnover_cooldown_parameter_plateau_study(
        source_2435_hit_rate_study_path=source_2435_hit_rate_study_path,
        source_2432_candidate_gauntlet_path=source_2432_candidate_gauntlet_path,
        candidate_set_2432_path=candidate_set_2432_path,
        hit_rate_study_doc_path=hit_rate_study_doc_path,
        candidate_gauntlet_doc_path=candidate_gauntlet_doc_path,
        candidate_set_2432_doc_path=candidate_set_2432_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt turnover cooldown parameter plateau study",
        payload,
    )
    for field in (
        "readiness_status",
        "source_2435_ready",
        "source_2432_gauntlet_ready",
        "candidate_set_parameter_plateau_contract_ready",
        "candidate_set_turnover_cooldown_group_ready",
        "candidate_set_required_metrics_ready",
        "parameter_plateau_study_ready",
        "parameter_plateau_matrix_ready",
        "turnover_cooldown_check_summary_ready",
        "no_effect_boundary_ready",
        "parameter_plateau_found",
        "isolated_winner",
        "robust_region_count",
        "component_value_found",
        "candidate_status",
        "nearby_parameter_pass_count",
        "turnover_delta",
        "whipsaw_delta",
        "missed_upside_delta",
        "return_degradation",
        "drawdown_degradation",
        "computed_new_metrics",
        "parameter_sweep_run",
        "market_data_parameter_plateau_run",
        "evidence_gap_count",
        "historical_screen_run",
        "pit_replay_run",
        "backtest_run",
        "scoring_run",
        "manual_review_required",
        "automatic_execution_allowed",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "outcome_backfilled",
        "outcome_binding_executed",
        "outcome_store_mutated",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "daily_report_generated",
        "daily_report_run",
        "fresh_market_data_read",
        "fresh_outcome_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_regime_slice_attribution_review_command(
    source_2436_parameter_plateau_study_path: Annotated[
        Path, typer.Option("--source-2436-parameter-plateau-study")
    ] = m2437.DEFAULT_SOURCE_2436_PARAMETER_PLATEAU_STUDY_PATH,
    source_2432_candidate_gauntlet_path: Annotated[
        Path, typer.Option("--source-2432-candidate-gauntlet")
    ] = m2437.DEFAULT_SOURCE_2432_CANDIDATE_GAUNTLET_PATH,
    candidate_set_2432_path: Annotated[
        Path, typer.Option("--candidate-set-2432")
    ] = m2437.DEFAULT_CANDIDATE_SET_2432_PATH,
    parameter_plateau_study_doc_path: Annotated[
        Path, typer.Option("--parameter-plateau-study-doc")
    ] = m2437.DEFAULT_PARAMETER_PLATEAU_STUDY_DOC_PATH,
    route_2437_doc_path: Annotated[
        Path, typer.Option("--route-2437-doc")
    ] = m2437.DEFAULT_2437_ROUTE_DOC_PATH,
    candidate_gauntlet_doc_path: Annotated[
        Path, typer.Option("--candidate-gauntlet-doc")
    ] = m2437.DEFAULT_CANDIDATE_GAUNTLET_DOC_PATH,
    candidate_set_2432_doc_path: Annotated[
        Path, typer.Option("--candidate-set-2432-doc")
    ] = m2437.DEFAULT_CANDIDATE_SET_2432_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2437.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2437.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2437.DEFAULT_SYSTEM_FLOW_PATH,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2437.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2437.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2437.run_growth_tilt_regime_slice_attribution_review(
        source_2436_parameter_plateau_study_path=source_2436_parameter_plateau_study_path,
        source_2432_candidate_gauntlet_path=source_2432_candidate_gauntlet_path,
        candidate_set_2432_path=candidate_set_2432_path,
        parameter_plateau_study_doc_path=parameter_plateau_study_doc_path,
        route_2437_doc_path=route_2437_doc_path,
        candidate_gauntlet_doc_path=candidate_gauntlet_doc_path,
        candidate_set_2432_doc_path=candidate_set_2432_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt regime slice attribution review",
        payload,
    )
    for field in (
        "readiness_status",
        "source_2436_ready",
        "source_2432_gauntlet_ready",
        "candidate_set_regime_slice_contract_ready",
        "candidate_set_required_metrics_ready",
        "regime_slice_attribution_review_ready",
        "regime_slice_attribution_matrix_ready",
        "candidate_status_by_regime_ready",
        "no_effect_boundary_ready",
        "recommended_regime_slice_count",
        "candidate_set_regime_slice_count",
        "regime_robustness_score",
        "single_regime_dependency_detected",
        "single_regime_dependency_assessed",
        "regime_pass_count",
        "regime_fail_count",
        "regime_inconclusive_count",
        "all_recommended_regime_status_inconclusive",
        "component_value_found",
        "candidate_status",
        "computed_new_metrics",
        "regime_attribution_run",
        "market_data_regime_attribution_run",
        "evidence_gap_count",
        "historical_screen_run",
        "pit_replay_run",
        "backtest_run",
        "scoring_run",
        "manual_review_required",
        "automatic_execution_allowed",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "outcome_backfilled",
        "outcome_binding_executed",
        "outcome_store_mutated",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "daily_report_generated",
        "daily_report_run",
        "fresh_market_data_read",
        "fresh_outcome_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_top3_candidate_pit_replay_command(
    source_2437_regime_review_path: Annotated[
        Path, typer.Option("--source-2437-regime-review")
    ] = m2438.DEFAULT_SOURCE_2437_REGIME_REVIEW_PATH,
    source_2433_batch_screen_path: Annotated[
        Path, typer.Option("--source-2433-batch-screen")
    ] = m2438.DEFAULT_SOURCE_2433_BATCH_SCREEN_PATH,
    source_2431_existing_candidate_evidence_path: Annotated[
        Path, typer.Option("--source-2431-existing-candidate-evidence")
    ] = m2438.DEFAULT_SOURCE_2431_EXISTING_CANDIDATE_EVIDENCE_PATH,
    candidate_set_2433_path: Annotated[
        Path, typer.Option("--candidate-set-2433")
    ] = m2438.DEFAULT_CANDIDATE_SET_2433_PATH,
    regime_review_doc_path: Annotated[
        Path, typer.Option("--regime-review-doc")
    ] = m2438.DEFAULT_REGIME_REVIEW_DOC_PATH,
    batch_screen_doc_path: Annotated[
        Path, typer.Option("--batch-screen-doc")
    ] = m2438.DEFAULT_BATCH_SCREEN_DOC_PATH,
    existing_candidate_doc_path: Annotated[
        Path, typer.Option("--existing-candidate-doc")
    ] = m2438.DEFAULT_EXISTING_CANDIDATE_DOC_PATH,
    candidate_set_2433_doc_path: Annotated[
        Path, typer.Option("--candidate-set-2433-doc")
    ] = m2438.DEFAULT_CANDIDATE_SET_2433_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2438.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2438.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2438.DEFAULT_SYSTEM_FLOW_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = m2438.DEFAULT_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = m2438.DEFAULT_RATES_PATH,
    data_quality_summary_path: Annotated[
        Path | None, typer.Option("--data-quality-summary")
    ] = None,
    data_quality_output_path: Annotated[
        Path | None, typer.Option("--data-quality-output")
    ] = None,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2438.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2438.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2438.run_growth_tilt_top3_candidate_pit_replay(
        source_2437_regime_review_path=source_2437_regime_review_path,
        source_2433_batch_screen_path=source_2433_batch_screen_path,
        source_2431_existing_candidate_evidence_path=(
            source_2431_existing_candidate_evidence_path
        ),
        candidate_set_2433_path=candidate_set_2433_path,
        regime_review_doc_path=regime_review_doc_path,
        batch_screen_doc_path=batch_screen_doc_path,
        existing_candidate_doc_path=existing_candidate_doc_path,
        candidate_set_2433_doc_path=candidate_set_2433_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        prices_path=prices_path,
        rates_path=rates_path,
        data_quality_summary_path=data_quality_summary_path,
        data_quality_output_path=data_quality_output_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt top-3 candidate PIT replay",
        payload,
    )
    for field in (
        "readiness_status",
        "source_2437_ready",
        "source_2433_batch_screen_ready",
        "source_2431_existing_candidate_evidence_ready",
        "candidate_set_2433_ready",
        "data_quality_gate_executed",
        "data_quality_gate_passed",
        "data_quality_status",
        "top3_candidate_selection_ready",
        "pit_replay_evidence_artifact_ready",
        "pit_replay_blocker_summary_ready",
        "no_effect_boundary_ready",
        "pit_candidates_selected",
        "pit_candidates_tested",
        "pit_replay_pass_count",
        "pit_replay_fail_count",
        "pit_replay_blocked_count",
        "promotion_review_candidate_count",
        "candidate_pit_replay_engine_available",
        "candidate_replay_input_specs_ready",
        "candidate_source_traceability_manifests_ready",
        "candidate_as_of_boundary_specs_ready",
        "candidate_valid_until_boundary_specs_ready",
        "candidate_outcome_linkage_specs_ready",
        "source_traceability_verified_count",
        "as_of_boundary_verified_count",
        "valid_until_boundary_verified_count",
        "outcome_linkage_ready_count",
        "pit_replay_run",
        "pit_replay_executed",
        "computed_new_metrics",
        "evidence_gap_count",
        "historical_screen_run",
        "backtest_run",
        "scoring_run",
        "manual_review_required",
        "automatic_execution_allowed",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "outcome_backfilled",
        "outcome_binding_executed",
        "outcome_store_mutated",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "daily_report_generated",
        "daily_report_run",
        "fresh_market_data_read",
        "fresh_outcome_data_read",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_top3_candidate_pit_replay_engine_remediation_command(
    source_2440_promotion_review_path: Annotated[
        Path, typer.Option("--source-2440-promotion-review")
    ] = m2438a.DEFAULT_SOURCE_2440_PROMOTION_REVIEW_PATH,
    source_2439_forward_pack_path: Annotated[
        Path, typer.Option("--source-2439-forward-pack")
    ] = m2438a.DEFAULT_SOURCE_2439_FORWARD_PACK_PATH,
    source_2438_pit_replay_path: Annotated[
        Path, typer.Option("--source-2438-pit-replay")
    ] = m2438a.DEFAULT_SOURCE_2438_PIT_REPLAY_PATH,
    pit_replay_evidence_path: Annotated[
        Path, typer.Option("--pit-replay-evidence")
    ] = m2438a.DEFAULT_PIT_REPLAY_EVIDENCE_PATH,
    pit_replay_blocker_summary_path: Annotated[
        Path, typer.Option("--pit-replay-blocker-summary")
    ] = m2438a.DEFAULT_PIT_REPLAY_BLOCKER_SUMMARY_PATH,
    source_2440_doc_path: Annotated[
        Path, typer.Option("--source-2440-doc")
    ] = m2438a.DEFAULT_SOURCE_2440_DOC_PATH,
    source_2439_doc_path: Annotated[
        Path, typer.Option("--source-2439-doc")
    ] = m2438a.DEFAULT_SOURCE_2439_DOC_PATH,
    source_2438_doc_path: Annotated[
        Path, typer.Option("--source-2438-doc")
    ] = m2438a.DEFAULT_SOURCE_2438_DOC_PATH,
    pit_replay_evidence_doc_path: Annotated[
        Path, typer.Option("--pit-replay-evidence-doc")
    ] = m2438a.DEFAULT_PIT_REPLAY_EVIDENCE_DOC_PATH,
    pit_replay_blocker_doc_path: Annotated[
        Path, typer.Option("--pit-replay-blocker-doc")
    ] = m2438a.DEFAULT_PIT_REPLAY_BLOCKER_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2438a.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2438a.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2438a.DEFAULT_SYSTEM_FLOW_PATH,
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = m2438a.DEFAULT_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path")
    ] = m2438a.DEFAULT_RATES_PATH,
    data_quality_summary_path: Annotated[
        Path | None, typer.Option("--data-quality-summary")
    ] = None,
    data_quality_output_path: Annotated[
        Path | None, typer.Option("--data-quality-output")
    ] = None,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2438a.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2438a.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2438a.run_growth_tilt_top3_candidate_pit_replay_engine_remediation(
        source_2440_promotion_review_path=source_2440_promotion_review_path,
        source_2439_forward_pack_path=source_2439_forward_pack_path,
        source_2438_pit_replay_path=source_2438_pit_replay_path,
        pit_replay_evidence_path=pit_replay_evidence_path,
        pit_replay_blocker_summary_path=pit_replay_blocker_summary_path,
        source_2440_doc_path=source_2440_doc_path,
        source_2439_doc_path=source_2439_doc_path,
        source_2438_doc_path=source_2438_doc_path,
        pit_replay_evidence_doc_path=pit_replay_evidence_doc_path,
        pit_replay_blocker_doc_path=pit_replay_blocker_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        prices_path=prices_path,
        rates_path=rates_path,
        data_quality_summary_path=data_quality_summary_path,
        data_quality_output_path=data_quality_output_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt top-3 candidate PIT replay engine remediation",
        payload,
    )
    for field in (
        "readiness_status",
        "prior_promotion_review_status",
        "prior_forward_aging_status",
        "prior_pit_replay_status",
        "blocked_by_forward_aging_gate",
        "not_no_candidate_status",
        "data_quality_gate_executed",
        "data_quality_gate_passed",
        "data_quality_status",
        "candidate_selection_resolves",
        "top3_candidate_ids_present",
        "pit_replay_artifacts_present",
        "pit_replay_engine_ready",
        "candidate_pit_replay_engine_available",
        "candidate_replay_input_specs_ready",
        "pit_replay_evidence_ready",
        "pit_replay_evidence_complete",
        "source_traceability_complete",
        "as_of_boundary_explicit",
        "valid_until_boundary_explicit",
        "outcome_linkage_complete",
        "forward_aging_handoff_ready",
        "registry_catalog_docs_alignment",
        "remediation_ready",
        "remediation_gap_count",
        "unresolved_engine_blocker_count",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "production_enabled",
        "broker_enabled",
        "automatic_execution_allowed",
        "generated_trading_advice",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_pit_replay_engine_blocker_closure_command(
    source_2438a_remediation_path: Annotated[
        Path, typer.Option("--source-2438a-remediation")
    ] = m2438b.DEFAULT_SOURCE_2438A_REMEDIATION_PATH,
    source_2438_pit_replay_path: Annotated[
        Path, typer.Option("--source-2438-pit-replay")
    ] = m2438b.DEFAULT_SOURCE_2438_PIT_REPLAY_PATH,
    pit_replay_evidence_path: Annotated[
        Path, typer.Option("--pit-replay-evidence")
    ] = m2438b.DEFAULT_PIT_REPLAY_EVIDENCE_PATH,
    pit_replay_blocker_summary_path: Annotated[
        Path, typer.Option("--pit-replay-blocker-summary")
    ] = m2438b.DEFAULT_PIT_REPLAY_BLOCKER_SUMMARY_PATH,
    source_2438a_doc_path: Annotated[
        Path, typer.Option("--source-2438a-doc")
    ] = m2438b.DEFAULT_SOURCE_2438A_DOC_PATH,
    source_2438_doc_path: Annotated[
        Path, typer.Option("--source-2438-doc")
    ] = m2438b.DEFAULT_SOURCE_2438_DOC_PATH,
    pit_replay_evidence_doc_path: Annotated[
        Path, typer.Option("--pit-replay-evidence-doc")
    ] = m2438b.DEFAULT_PIT_REPLAY_EVIDENCE_DOC_PATH,
    pit_replay_blocker_doc_path: Annotated[
        Path, typer.Option("--pit-replay-blocker-doc")
    ] = m2438b.DEFAULT_PIT_REPLAY_BLOCKER_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2438b.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2438b.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2438b.DEFAULT_SYSTEM_FLOW_PATH,
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = m2438b.DEFAULT_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path")
    ] = m2438b.DEFAULT_RATES_PATH,
    data_quality_summary_path: Annotated[
        Path | None, typer.Option("--data-quality-summary")
    ] = None,
    data_quality_output_path: Annotated[
        Path | None, typer.Option("--data-quality-output")
    ] = None,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2438b.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2438b.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2438b.run_growth_tilt_pit_replay_engine_blocker_closure(
        source_2438a_remediation_path=source_2438a_remediation_path,
        source_2438_pit_replay_path=source_2438_pit_replay_path,
        pit_replay_evidence_path=pit_replay_evidence_path,
        pit_replay_blocker_summary_path=pit_replay_blocker_summary_path,
        source_2438a_doc_path=source_2438a_doc_path,
        source_2438_doc_path=source_2438_doc_path,
        pit_replay_evidence_doc_path=pit_replay_evidence_doc_path,
        pit_replay_blocker_doc_path=pit_replay_blocker_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        prices_path=prices_path,
        rates_path=rates_path,
        data_quality_summary_path=data_quality_summary_path,
        data_quality_output_path=data_quality_output_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt PIT replay engine blocker closure",
        payload,
    )
    for field in (
        "readiness_status",
        "prior_status",
        "prior_pit_replay_status",
        "not_no_candidate_status",
        "source_2438a_remediation_blocked",
        "source_2438_pit_replay_blocked",
        "data_quality_gate_executed",
        "data_quality_gate_passed",
        "data_quality_status",
        "candidate_selection_resolves",
        "blocker_closure_ready",
        "blocker_count_before",
        "blocker_count_after",
        "pit_replay_engine_ready",
        "input_specs_ready",
        "evidence_completeness_ready",
        "source_traceability_ready",
        "as_of_boundary_ready",
        "valid_until_boundary_ready",
        "outcome_linkage_ready",
        "forward_aging_handoff_ready",
        "registry_catalog_docs_alignment",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "production_enabled",
        "broker_enabled",
        "automatic_execution_allowed",
        "generated_trading_advice",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_top3_candidate_pit_replay_recheck_command(
    source_2438b_blocker_closure_path: Annotated[
        Path, typer.Option("--source-2438b-blocker-closure")
    ] = m2438c.DEFAULT_SOURCE_2438B_BLOCKER_CLOSURE_PATH,
    source_2438a_remediation_path: Annotated[
        Path, typer.Option("--source-2438a-remediation")
    ] = m2438c.DEFAULT_SOURCE_2438A_REMEDIATION_PATH,
    source_2438_pit_replay_path: Annotated[
        Path, typer.Option("--source-2438-pit-replay")
    ] = m2438c.DEFAULT_SOURCE_2438_PIT_REPLAY_PATH,
    pit_replay_evidence_path: Annotated[
        Path, typer.Option("--pit-replay-evidence")
    ] = m2438c.DEFAULT_PIT_REPLAY_EVIDENCE_PATH,
    pit_replay_blocker_summary_path: Annotated[
        Path, typer.Option("--pit-replay-blocker-summary")
    ] = m2438c.DEFAULT_PIT_REPLAY_BLOCKER_SUMMARY_PATH,
    source_2438b_doc_path: Annotated[
        Path, typer.Option("--source-2438b-doc")
    ] = m2438c.DEFAULT_SOURCE_2438B_DOC_PATH,
    source_2438a_doc_path: Annotated[
        Path, typer.Option("--source-2438a-doc")
    ] = m2438c.DEFAULT_SOURCE_2438A_DOC_PATH,
    source_2438_doc_path: Annotated[
        Path, typer.Option("--source-2438-doc")
    ] = m2438c.DEFAULT_SOURCE_2438_DOC_PATH,
    pit_replay_evidence_doc_path: Annotated[
        Path, typer.Option("--pit-replay-evidence-doc")
    ] = m2438c.DEFAULT_PIT_REPLAY_EVIDENCE_DOC_PATH,
    pit_replay_blocker_doc_path: Annotated[
        Path, typer.Option("--pit-replay-blocker-doc")
    ] = m2438c.DEFAULT_PIT_REPLAY_BLOCKER_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2438c.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2438c.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2438c.DEFAULT_SYSTEM_FLOW_PATH,
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = m2438c.DEFAULT_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path")
    ] = m2438c.DEFAULT_RATES_PATH,
    data_quality_summary_path: Annotated[
        Path | None, typer.Option("--data-quality-summary")
    ] = None,
    data_quality_output_path: Annotated[
        Path | None, typer.Option("--data-quality-output")
    ] = None,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2438c.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2438c.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2438c.run_growth_tilt_top3_candidate_pit_replay_recheck(
        source_2438b_blocker_closure_path=source_2438b_blocker_closure_path,
        source_2438a_remediation_path=source_2438a_remediation_path,
        source_2438_pit_replay_path=source_2438_pit_replay_path,
        pit_replay_evidence_path=pit_replay_evidence_path,
        pit_replay_blocker_summary_path=pit_replay_blocker_summary_path,
        source_2438b_doc_path=source_2438b_doc_path,
        source_2438a_doc_path=source_2438a_doc_path,
        source_2438_doc_path=source_2438_doc_path,
        pit_replay_evidence_doc_path=pit_replay_evidence_doc_path,
        pit_replay_blocker_doc_path=pit_replay_blocker_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        prices_path=prices_path,
        rates_path=rates_path,
        data_quality_summary_path=data_quality_summary_path,
        data_quality_output_path=data_quality_output_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt top-3 candidate PIT replay recheck",
        payload,
    )
    for field in (
        "readiness_status",
        "prior_blocker_closure_status",
        "prior_remediation_status",
        "prior_pit_replay_status",
        "source_2438b_blocker_closure_ready",
        "source_2438a_remediation_blocked",
        "source_2438_pit_replay_blocked",
        "not_no_candidate_status",
        "data_quality_gate_executed",
        "data_quality_gate_passed",
        "data_quality_status",
        "pit_replay_recheck_ready",
        "pit_replay_engine_ready",
        "input_specs_ready",
        "evidence_completeness_ready",
        "source_traceability_ready",
        "as_of_boundary_ready",
        "valid_until_boundary_ready",
        "outcome_linkage_ready",
        "forward_aging_handoff_ready",
        "top3_candidate_selection_resolves",
        "pit_replay_evidence_exists",
        "candidate_replay_outputs_complete",
        "top3_candidate_count",
        "candidate_replay_pass_count",
        "candidate_replay_fail_count",
        "candidate_replay_blocked_count",
        "registry_catalog_docs_alignment",
        "paper_shadow_candidate_found",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "production_enabled",
        "broker_enabled",
        "automatic_execution_allowed",
        "generated_trading_advice",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure_command(
    source_2438c_recheck_path: Annotated[
        Path, typer.Option("--source-2438c-recheck")
    ] = m2438d.DEFAULT_SOURCE_2438C_RECHECK_PATH,
    source_2438b_blocker_closure_path: Annotated[
        Path, typer.Option("--source-2438b-blocker-closure")
    ] = m2438d.DEFAULT_SOURCE_2438B_BLOCKER_CLOSURE_PATH,
    source_2438_pit_replay_path: Annotated[
        Path, typer.Option("--source-2438-pit-replay")
    ] = m2438d.DEFAULT_SOURCE_2438_PIT_REPLAY_PATH,
    pit_replay_evidence_path: Annotated[
        Path, typer.Option("--pit-replay-evidence")
    ] = m2438d.DEFAULT_PIT_REPLAY_EVIDENCE_PATH,
    pit_replay_blocker_summary_path: Annotated[
        Path, typer.Option("--pit-replay-blocker-summary")
    ] = m2438d.DEFAULT_PIT_REPLAY_BLOCKER_SUMMARY_PATH,
    source_2438c_doc_path: Annotated[
        Path, typer.Option("--source-2438c-doc")
    ] = m2438d.DEFAULT_SOURCE_2438C_DOC_PATH,
    source_2438b_doc_path: Annotated[
        Path, typer.Option("--source-2438b-doc")
    ] = m2438d.DEFAULT_SOURCE_2438B_DOC_PATH,
    source_2438_doc_path: Annotated[
        Path, typer.Option("--source-2438-doc")
    ] = m2438d.DEFAULT_SOURCE_2438_DOC_PATH,
    pit_replay_evidence_doc_path: Annotated[
        Path, typer.Option("--pit-replay-evidence-doc")
    ] = m2438d.DEFAULT_PIT_REPLAY_EVIDENCE_DOC_PATH,
    pit_replay_blocker_doc_path: Annotated[
        Path, typer.Option("--pit-replay-blocker-doc")
    ] = m2438d.DEFAULT_PIT_REPLAY_BLOCKER_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2438d.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2438d.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2438d.DEFAULT_SYSTEM_FLOW_PATH,
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = m2438d.DEFAULT_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path")
    ] = m2438d.DEFAULT_RATES_PATH,
    data_quality_summary_path: Annotated[
        Path | None, typer.Option("--data-quality-summary")
    ] = None,
    data_quality_output_path: Annotated[
        Path | None, typer.Option("--data-quality-output")
    ] = None,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2438d.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2438d.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = (
        m2438d.run_growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure(
            source_2438c_recheck_path=source_2438c_recheck_path,
            source_2438b_blocker_closure_path=source_2438b_blocker_closure_path,
            source_2438_pit_replay_path=source_2438_pit_replay_path,
            pit_replay_evidence_path=pit_replay_evidence_path,
            pit_replay_blocker_summary_path=pit_replay_blocker_summary_path,
            source_2438c_doc_path=source_2438c_doc_path,
            source_2438b_doc_path=source_2438b_doc_path,
            source_2438_doc_path=source_2438_doc_path,
            pit_replay_evidence_doc_path=pit_replay_evidence_doc_path,
            pit_replay_blocker_doc_path=pit_replay_blocker_doc_path,
            report_registry_path=report_registry_path,
            artifact_catalog_path=artifact_catalog_path,
            system_flow_path=system_flow_path,
            prices_path=prices_path,
            rates_path=rates_path,
            data_quality_summary_path=data_quality_summary_path,
            data_quality_output_path=data_quality_output_path,
            output_root=output_root,
            docs_root=docs_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_execution_semantics_payload(
        "Growth tilt top-3 candidate PIT replay recheck blocker closure",
        payload,
    )
    for field in (
        "readiness_status",
        "prior_status",
        "prior_candidate_replay_outputs_complete",
        "source_2438c_recheck_blocked",
        "source_2438b_blocker_closure_ready",
        "source_2438_pit_replay_artifact_resolves",
        "data_quality_gate_executed",
        "data_quality_gate_passed",
        "data_quality_status",
        "blocker_closure_ready",
        "candidate_replay_outputs_complete",
        "candidate_replay_output_record_count",
        "top3_candidate_ids_present",
        "each_candidate_has_replay_status",
        "each_candidate_has_status_reason",
        "each_candidate_has_input_spec_ref",
        "each_candidate_has_source_traceability_ref",
        "each_candidate_has_evidence_ref",
        "each_candidate_has_as_of_boundary",
        "each_candidate_has_valid_until_policy_ref",
        "each_candidate_has_outcome_linkage_key",
        "each_candidate_has_forward_aging_handoff_key",
        "top3_candidate_count",
        "candidate_replay_pass_count",
        "candidate_replay_fail_count",
        "candidate_replay_blocked_count",
        "registry_catalog_docs_alignment",
        "paper_shadow_candidate_found",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "production_enabled",
        "broker_enabled",
        "automatic_execution_allowed",
        "generated_trading_advice",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure_command(
    source_2438d_output_closure_path: Annotated[
        Path, typer.Option("--source-2438d-output-closure")
    ] = m2438e.DEFAULT_SOURCE_2438D_OUTPUT_CLOSURE_PATH,
    candidate_replay_output_records_path: Annotated[
        Path, typer.Option("--candidate-replay-output-records")
    ] = m2438e.DEFAULT_CANDIDATE_REPLAY_OUTPUT_RECORDS_PATH,
    source_2438c_recheck_path: Annotated[
        Path, typer.Option("--source-2438c-recheck")
    ] = m2438e.DEFAULT_SOURCE_2438C_RECHECK_PATH,
    source_2438b_blocker_closure_path: Annotated[
        Path, typer.Option("--source-2438b-blocker-closure")
    ] = m2438e.DEFAULT_SOURCE_2438B_BLOCKER_CLOSURE_PATH,
    source_2438d_doc_path: Annotated[
        Path, typer.Option("--source-2438d-doc")
    ] = m2438e.DEFAULT_SOURCE_2438D_DOC_PATH,
    candidate_output_records_doc_path: Annotated[
        Path, typer.Option("--candidate-output-records-doc")
    ] = m2438e.DEFAULT_CANDIDATE_OUTPUT_RECORDS_DOC_PATH,
    source_2438c_doc_path: Annotated[
        Path, typer.Option("--source-2438c-doc")
    ] = m2438e.DEFAULT_SOURCE_2438C_DOC_PATH,
    source_2438b_doc_path: Annotated[
        Path, typer.Option("--source-2438b-doc")
    ] = m2438e.DEFAULT_SOURCE_2438B_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2438e.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2438e.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2438e.DEFAULT_SYSTEM_FLOW_PATH,
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = m2438e.DEFAULT_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path")
    ] = m2438e.DEFAULT_RATES_PATH,
    data_quality_summary_path: Annotated[
        Path | None, typer.Option("--data-quality-summary")
    ] = None,
    data_quality_output_path: Annotated[
        Path | None, typer.Option("--data-quality-output")
    ] = None,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2438e.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2438e.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = (
        m2438e.run_growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure(
            source_2438d_output_closure_path=source_2438d_output_closure_path,
            candidate_replay_output_records_path=candidate_replay_output_records_path,
            source_2438c_recheck_path=source_2438c_recheck_path,
            source_2438b_blocker_closure_path=source_2438b_blocker_closure_path,
            source_2438d_doc_path=source_2438d_doc_path,
            candidate_output_records_doc_path=candidate_output_records_doc_path,
            source_2438c_doc_path=source_2438c_doc_path,
            source_2438b_doc_path=source_2438b_doc_path,
            report_registry_path=report_registry_path,
            artifact_catalog_path=artifact_catalog_path,
            system_flow_path=system_flow_path,
            prices_path=prices_path,
            rates_path=rates_path,
            data_quality_summary_path=data_quality_summary_path,
            data_quality_output_path=data_quality_output_path,
            output_root=output_root,
            docs_root=docs_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_execution_semantics_payload(
        "Growth tilt top-3 candidate PIT replay recheck after output closure",
        payload,
    )
    for field in (
        "readiness_status",
        "prior_status",
        "source_2438d_output_closure_ready",
        "source_2438c_recheck_blocked",
        "source_2438b_blocker_closure_ready",
        "data_quality_gate_executed",
        "data_quality_gate_passed",
        "data_quality_status",
        "candidate_replay_outputs_complete",
        "candidate_replay_output_record_count",
        "candidate_output_records_recheckable",
        "pit_replay_recheck_after_output_closure_ready",
        "candidate_replay_pass_count",
        "candidate_replay_fail_count",
        "candidate_replay_blocked_count",
        "candidate_level_blocker_count",
        "forward_aging_handoff_ready",
        "forward_aging_candidate_count",
        "top3_candidate_count",
        "each_candidate_has_replay_status",
        "each_candidate_has_status_reason",
        "pass_fail_blocked_counts_consistent",
        "blocked_candidates_have_blocker_reason",
        "forward_aging_handoff_pass_only",
        "registry_catalog_docs_alignment",
        "paper_shadow_candidate_found",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "production_enabled",
        "broker_enabled",
        "automatic_execution_allowed",
        "generated_trading_advice",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_top3_candidate_level_pit_replay_blocker_closure_command(
    source_2438e_recheck_path: Annotated[
        Path, typer.Option("--source-2438e-recheck")
    ] = m2438f.DEFAULT_SOURCE_2438E_RECHECK_PATH,
    candidate_replay_output_records_path: Annotated[
        Path, typer.Option("--candidate-replay-output-records")
    ] = m2438f.DEFAULT_CANDIDATE_REPLAY_OUTPUT_RECORDS_PATH,
    candidate_level_blocker_summary_path: Annotated[
        Path, typer.Option("--candidate-level-blocker-summary")
    ] = m2438f.DEFAULT_CANDIDATE_LEVEL_BLOCKER_SUMMARY_PATH,
    source_2438e_doc_path: Annotated[
        Path, typer.Option("--source-2438e-doc")
    ] = m2438f.DEFAULT_SOURCE_2438E_DOC_PATH,
    candidate_output_records_doc_path: Annotated[
        Path, typer.Option("--candidate-output-records-doc")
    ] = m2438f.DEFAULT_CANDIDATE_OUTPUT_RECORDS_DOC_PATH,
    candidate_level_blocker_doc_path: Annotated[
        Path, typer.Option("--candidate-level-blocker-doc")
    ] = m2438f.DEFAULT_CANDIDATE_LEVEL_BLOCKER_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2438f.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2438f.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2438f.DEFAULT_SYSTEM_FLOW_PATH,
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = m2438f.DEFAULT_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path")
    ] = m2438f.DEFAULT_RATES_PATH,
    data_quality_summary_path: Annotated[
        Path | None, typer.Option("--data-quality-summary")
    ] = None,
    data_quality_output_path: Annotated[
        Path | None, typer.Option("--data-quality-output")
    ] = None,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2438f.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2438f.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = (
        m2438f.run_growth_tilt_top3_candidate_level_pit_replay_blocker_closure(
            source_2438e_recheck_path=source_2438e_recheck_path,
            candidate_replay_output_records_path=candidate_replay_output_records_path,
            candidate_level_blocker_summary_path=candidate_level_blocker_summary_path,
            source_2438e_doc_path=source_2438e_doc_path,
            candidate_output_records_doc_path=candidate_output_records_doc_path,
            candidate_level_blocker_doc_path=candidate_level_blocker_doc_path,
            report_registry_path=report_registry_path,
            artifact_catalog_path=artifact_catalog_path,
            system_flow_path=system_flow_path,
            prices_path=prices_path,
            rates_path=rates_path,
            data_quality_summary_path=data_quality_summary_path,
            data_quality_output_path=data_quality_output_path,
            output_root=output_root,
            docs_root=docs_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_execution_semantics_payload(
        "Growth tilt top-3 candidate-level PIT replay blocker closure",
        payload,
    )
    for field in (
        "readiness_status",
        "prior_status",
        "source_2438e_candidate_level_blocked",
        "data_quality_gate_executed",
        "data_quality_gate_passed",
        "data_quality_status",
        "candidate_replay_outputs_complete",
        "candidate_replay_output_record_count",
        "candidate_level_blocker_closure_records_complete",
        "candidate_level_blocker_closure_ready",
        "candidate_level_blocker_count_before",
        "candidate_level_blocker_count_after",
        "candidate_replayable_after_closure_count",
        "replayability_handoff_ready",
        "forward_aging_handoff_ready",
        "candidate_replay_pass_count",
        "candidate_replay_fail_count",
        "candidate_replay_blocked_count",
        "top3_candidate_count",
        "each_candidate_has_prior_blocker_reason",
        "each_candidate_has_closure_action",
        "each_candidate_has_closure_evidence_ref",
        "each_candidate_has_after_state",
        "all_candidate_blockers_closed",
        "registry_catalog_docs_alignment",
        "paper_shadow_candidate_found",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "production_enabled",
        "broker_enabled",
        "automatic_execution_allowed",
        "generated_trading_advice",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_top3_candidate_pit_replay_recheck_after_candidate_blocker_closure_command(
    source_2438f_blocker_closure_path: Annotated[
        Path, typer.Option("--source-2438f-blocker-closure")
    ] = m2438g.DEFAULT_SOURCE_2438F_BLOCKER_CLOSURE_PATH,
    replayability_handoff_manifest_path: Annotated[
        Path, typer.Option("--replayability-handoff-manifest")
    ] = m2438g.DEFAULT_REPLAYABILITY_HANDOFF_MANIFEST_PATH,
    candidate_replay_output_records_path: Annotated[
        Path, typer.Option("--candidate-replay-output-records")
    ] = m2438g.DEFAULT_CANDIDATE_REPLAY_OUTPUT_RECORDS_PATH,
    candidate_level_closure_records_path: Annotated[
        Path, typer.Option("--candidate-level-closure-records")
    ] = m2438g.DEFAULT_CANDIDATE_LEVEL_CLOSURE_RECORDS_PATH,
    source_2438f_doc_path: Annotated[
        Path, typer.Option("--source-2438f-doc")
    ] = m2438g.DEFAULT_SOURCE_2438F_DOC_PATH,
    replayability_handoff_doc_path: Annotated[
        Path, typer.Option("--replayability-handoff-doc")
    ] = m2438g.DEFAULT_REPLAYABILITY_HANDOFF_DOC_PATH,
    candidate_level_closure_doc_path: Annotated[
        Path, typer.Option("--candidate-level-closure-doc")
    ] = m2438g.DEFAULT_CANDIDATE_LEVEL_CLOSURE_DOC_PATH,
    candidate_output_records_doc_path: Annotated[
        Path, typer.Option("--candidate-output-records-doc")
    ] = m2438g.DEFAULT_CANDIDATE_OUTPUT_RECORDS_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2438g.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2438g.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2438g.DEFAULT_SYSTEM_FLOW_PATH,
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = m2438g.DEFAULT_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path")
    ] = m2438g.DEFAULT_RATES_PATH,
    data_quality_summary_path: Annotated[
        Path | None, typer.Option("--data-quality-summary")
    ] = None,
    data_quality_output_path: Annotated[
        Path | None, typer.Option("--data-quality-output")
    ] = None,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2438g.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2438g.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    run_recheck = (
        m2438g.run_growth_tilt_top3_candidate_pit_replay_recheck_after_candidate_blocker_closure
    )
    payload = run_recheck(
        source_2438f_blocker_closure_path=source_2438f_blocker_closure_path,
        replayability_handoff_manifest_path=replayability_handoff_manifest_path,
        candidate_replay_output_records_path=candidate_replay_output_records_path,
        candidate_level_closure_records_path=candidate_level_closure_records_path,
        source_2438f_doc_path=source_2438f_doc_path,
        replayability_handoff_doc_path=replayability_handoff_doc_path,
        candidate_level_closure_doc_path=candidate_level_closure_doc_path,
        candidate_output_records_doc_path=candidate_output_records_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        prices_path=prices_path,
        rates_path=rates_path,
        data_quality_summary_path=data_quality_summary_path,
        data_quality_output_path=data_quality_output_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt top-3 candidate PIT replay recheck after candidate blocker closure",
        payload,
    )
    for field in (
        "readiness_status",
        "prior_status",
        "source_2438f_blocker_closure_ready",
        "candidate_level_blocker_closure_ready",
        "replayability_handoff_ready",
        "data_quality_gate_executed",
        "data_quality_gate_passed",
        "data_quality_status",
        "candidate_replay_outputs_complete",
        "candidate_replay_output_record_count",
        "candidate_records_recheckable_after_candidate_blocker_closure",
        "pit_replay_recheck_after_candidate_blocker_closure_complete",
        "candidate_replay_pass_count",
        "candidate_replay_fail_count",
        "candidate_replay_blocked_count",
        "remaining_candidate_replay_blocker_count",
        "forward_aging_handoff_ready",
        "forward_aging_candidate_count",
        "top3_candidate_count",
        "handoff_candidate_count",
        "candidate_level_closure_record_count",
        "each_candidate_has_replay_status",
        "each_candidate_has_status_reason",
        "pass_fail_blocked_counts_consistent",
        "blocked_candidates_have_blocker_reason",
        "pass_candidates_have_forward_aging_handoff_key",
        "forward_aging_handoff_pass_only",
        "registry_catalog_docs_alignment",
        "paper_shadow_candidate_found",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "production_enabled",
        "broker_enabled",
        "automatic_execution_allowed",
        "generated_trading_advice",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_remaining_candidate_pit_replay_blocker_closure_command(
    source_2438g_blocked_recheck_path: Annotated[
        Path, typer.Option("--source-2438g-blocked-recheck")
    ] = m2438h.DEFAULT_SOURCE_2438G_BLOCKED_RECHECK_PATH,
    source_2438f_candidate_level_blocker_closure_path: Annotated[
        Path, typer.Option("--source-2438f-candidate-level-blocker-closure")
    ] = m2438h.DEFAULT_SOURCE_2438F_CANDIDATE_LEVEL_BLOCKER_CLOSURE_PATH,
    candidate_replay_output_records_path: Annotated[
        Path, typer.Option("--candidate-replay-output-records")
    ] = m2438h.DEFAULT_CANDIDATE_REPLAY_OUTPUT_RECORDS_PATH,
    remaining_candidate_replay_blocker_summary_path: Annotated[
        Path, typer.Option("--remaining-candidate-replay-blocker-summary")
    ] = m2438h.DEFAULT_REMAINING_CANDIDATE_REPLAY_BLOCKER_SUMMARY_PATH,
    source_2438g_doc_path: Annotated[
        Path, typer.Option("--source-2438g-doc")
    ] = m2438h.DEFAULT_SOURCE_2438G_DOC_PATH,
    source_2438f_doc_path: Annotated[
        Path, typer.Option("--source-2438f-doc")
    ] = m2438h.DEFAULT_SOURCE_2438F_DOC_PATH,
    candidate_output_records_doc_path: Annotated[
        Path, typer.Option("--candidate-output-records-doc")
    ] = m2438h.DEFAULT_CANDIDATE_OUTPUT_RECORDS_DOC_PATH,
    remaining_blocker_summary_doc_path: Annotated[
        Path, typer.Option("--remaining-blocker-summary-doc")
    ] = m2438h.DEFAULT_REMAINING_BLOCKER_SUMMARY_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2438h.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2438h.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2438h.DEFAULT_SYSTEM_FLOW_PATH,
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = m2438h.DEFAULT_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path")
    ] = m2438h.DEFAULT_RATES_PATH,
    data_quality_summary_path: Annotated[
        Path | None, typer.Option("--data-quality-summary")
    ] = None,
    data_quality_output_path: Annotated[
        Path | None, typer.Option("--data-quality-output")
    ] = None,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2438h.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2438h.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2438h.run_growth_tilt_remaining_candidate_pit_replay_blocker_closure(
        source_2438g_blocked_recheck_path=source_2438g_blocked_recheck_path,
        source_2438f_candidate_level_blocker_closure_path=(
            source_2438f_candidate_level_blocker_closure_path
        ),
        candidate_replay_output_records_path=candidate_replay_output_records_path,
        remaining_candidate_replay_blocker_summary_path=(
            remaining_candidate_replay_blocker_summary_path
        ),
        source_2438g_doc_path=source_2438g_doc_path,
        source_2438f_doc_path=source_2438f_doc_path,
        candidate_output_records_doc_path=candidate_output_records_doc_path,
        remaining_blocker_summary_doc_path=remaining_blocker_summary_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        prices_path=prices_path,
        rates_path=rates_path,
        data_quality_summary_path=data_quality_summary_path,
        data_quality_output_path=data_quality_output_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt remaining candidate PIT replay blocker closure",
        payload,
    )
    for field in (
        "readiness_status",
        "prior_status",
        "prior_candidate_replay_pass_count",
        "prior_candidate_replay_fail_count",
        "prior_candidate_replay_blocked_count",
        "source_2438g_blocked_recheck_ready",
        "source_2438f_candidate_level_closure_ready",
        "candidate_output_records_complete",
        "candidate_replay_output_record_count",
        "replayability_handoff_ready",
        "data_quality_gate_executed",
        "data_quality_gate_passed",
        "data_quality_status",
        "remaining_blocker_records_present",
        "remaining_blocker_record_count",
        "remaining_candidate_blocker_closure_records_complete",
        "remaining_candidate_blocker_closure_ready",
        "remaining_candidate_blocker_count_before",
        "remaining_candidate_blocker_count_after",
        "candidate_recheckable_after_closure_count",
        "replay_recheck_handoff_ready",
        "candidate_replay_pass_count",
        "candidate_replay_fail_count",
        "candidate_replay_blocked_count",
        "forward_aging_handoff_ready",
        "forward_aging_candidate_count",
        "top3_candidate_count",
        "candidate_level_closure_record_count",
        "each_blocked_candidate_has_remaining_blocker_reason",
        "each_blocked_candidate_has_closure_action",
        "each_closure_action_has_evidence_ref",
        "each_candidate_has_after_state",
        "registry_catalog_docs_alignment",
        "paper_shadow_candidate_found",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "production_enabled",
        "broker_enabled",
        "automatic_execution_allowed",
        "generated_trading_advice",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure_command(
    source_2438h_remaining_blocker_closure_path: Annotated[
        Path, typer.Option("--source-2438h-remaining-blocker-closure")
    ] = m2438i.DEFAULT_SOURCE_2438H_REMAINING_BLOCKER_CLOSURE_PATH,
    replay_recheck_readiness_handoff_path: Annotated[
        Path, typer.Option("--replay-recheck-readiness-handoff")
    ] = m2438i.DEFAULT_REPLAY_RECHECK_READINESS_HANDOFF_PATH,
    candidate_replay_output_records_path: Annotated[
        Path, typer.Option("--candidate-replay-output-records")
    ] = m2438i.DEFAULT_CANDIDATE_REPLAY_OUTPUT_RECORDS_PATH,
    remaining_blocker_before_after_matrix_path: Annotated[
        Path, typer.Option("--remaining-blocker-before-after-matrix")
    ] = m2438i.DEFAULT_REMAINING_BLOCKER_BEFORE_AFTER_MATRIX_PATH,
    source_2438h_doc_path: Annotated[
        Path, typer.Option("--source-2438h-doc")
    ] = m2438i.DEFAULT_SOURCE_2438H_DOC_PATH,
    replay_recheck_handoff_doc_path: Annotated[
        Path, typer.Option("--replay-recheck-handoff-doc")
    ] = m2438i.DEFAULT_REPLAY_RECHECK_HANDOFF_DOC_PATH,
    candidate_output_records_doc_path: Annotated[
        Path, typer.Option("--candidate-output-records-doc")
    ] = m2438i.DEFAULT_CANDIDATE_OUTPUT_RECORDS_DOC_PATH,
    remaining_blocker_before_after_doc_path: Annotated[
        Path, typer.Option("--remaining-blocker-before-after-doc")
    ] = m2438i.DEFAULT_REMAINING_BLOCKER_BEFORE_AFTER_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2438i.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2438i.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2438i.DEFAULT_SYSTEM_FLOW_PATH,
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = m2438i.DEFAULT_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path")
    ] = m2438i.DEFAULT_RATES_PATH,
    data_quality_summary_path: Annotated[
        Path | None, typer.Option("--data-quality-summary")
    ] = None,
    data_quality_output_path: Annotated[
        Path | None, typer.Option("--data-quality-output")
    ] = None,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2438i.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2438i.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = (
        m2438i.run_growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure(
            source_2438h_remaining_blocker_closure_path=(
                source_2438h_remaining_blocker_closure_path
            ),
            replay_recheck_readiness_handoff_path=(
                replay_recheck_readiness_handoff_path
            ),
            candidate_replay_output_records_path=candidate_replay_output_records_path,
            remaining_blocker_before_after_matrix_path=(
                remaining_blocker_before_after_matrix_path
            ),
            source_2438h_doc_path=source_2438h_doc_path,
            replay_recheck_handoff_doc_path=replay_recheck_handoff_doc_path,
            candidate_output_records_doc_path=candidate_output_records_doc_path,
            remaining_blocker_before_after_doc_path=(
                remaining_blocker_before_after_doc_path
            ),
            report_registry_path=report_registry_path,
            artifact_catalog_path=artifact_catalog_path,
            system_flow_path=system_flow_path,
            prices_path=prices_path,
            rates_path=rates_path,
            data_quality_summary_path=data_quality_summary_path,
            data_quality_output_path=data_quality_output_path,
            output_root=output_root,
            docs_root=docs_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_execution_semantics_payload(
        "Growth tilt top-3 candidate PIT replay recheck after remaining blocker closure",
        payload,
    )
    for field in (
        "readiness_status",
        "prior_status",
        "source_2438h_remaining_blocker_closure_ready",
        "remaining_candidate_blocker_closure_ready",
        "remaining_candidate_blocker_count_before",
        "remaining_candidate_blocker_count_after",
        "replay_recheck_handoff_ready",
        "candidate_recheckable_after_closure_count",
        "data_quality_gate_executed",
        "data_quality_gate_passed",
        "data_quality_status",
        "candidate_replay_outputs_complete",
        "candidate_replay_output_record_count",
        "candidate_records_recheckable_after_remaining_blocker_closure",
        "pit_replay_recheck_after_remaining_blocker_closure_complete",
        "candidate_replay_pass_count",
        "candidate_replay_fail_count",
        "candidate_replay_blocked_count",
        "persistent_candidate_replay_blocker_count",
        "forward_aging_handoff_ready",
        "forward_aging_candidate_count",
        "top3_candidate_count",
        "handoff_candidate_count",
        "before_after_row_count",
        "each_candidate_has_replay_status",
        "each_candidate_has_status_reason",
        "pass_fail_blocked_counts_consistent",
        "blocked_candidates_have_persistent_blocker_reason",
        "pass_candidates_have_forward_aging_handoff_key",
        "forward_aging_handoff_pass_only",
        "registry_catalog_docs_alignment",
        "paper_shadow_candidate_found",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "production_enabled",
        "broker_enabled",
        "automatic_execution_allowed",
        "generated_trading_advice",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_persistent_candidate_pit_replay_blocker_escalation_command(
    source_2438i_blocked_recheck_path: Annotated[
        Path, typer.Option("--source-2438i-blocked-recheck")
    ] = m2438j.DEFAULT_SOURCE_2438I_BLOCKED_RECHECK_PATH,
    persistent_candidate_replay_blocker_summary_path: Annotated[
        Path, typer.Option("--persistent-candidate-replay-blocker-summary")
    ] = m2438j.DEFAULT_PERSISTENT_CANDIDATE_REPLAY_BLOCKER_SUMMARY_PATH,
    source_2438h_remaining_blocker_closure_path: Annotated[
        Path, typer.Option("--source-2438h-remaining-blocker-closure")
    ] = m2438j.DEFAULT_SOURCE_2438H_REMAINING_BLOCKER_CLOSURE_PATH,
    source_2438f_candidate_level_blocker_closure_path: Annotated[
        Path, typer.Option("--source-2438f-candidate-level-blocker-closure")
    ] = m2438j.DEFAULT_SOURCE_2438F_CANDIDATE_LEVEL_BLOCKER_CLOSURE_PATH,
    source_2438d_output_closure_path: Annotated[
        Path, typer.Option("--source-2438d-output-closure")
    ] = m2438j.DEFAULT_SOURCE_2438D_OUTPUT_CLOSURE_PATH,
    candidate_replay_output_records_path: Annotated[
        Path, typer.Option("--candidate-replay-output-records")
    ] = m2438j.DEFAULT_CANDIDATE_REPLAY_OUTPUT_RECORDS_PATH,
    source_2438b_engine_blocker_closure_path: Annotated[
        Path, typer.Option("--source-2438b-engine-blocker-closure")
    ] = m2438j.DEFAULT_SOURCE_2438B_ENGINE_BLOCKER_CLOSURE_PATH,
    source_2438i_doc_path: Annotated[
        Path, typer.Option("--source-2438i-doc")
    ] = m2438j.DEFAULT_SOURCE_2438I_DOC_PATH,
    persistent_blocker_summary_doc_path: Annotated[
        Path, typer.Option("--persistent-blocker-summary-doc")
    ] = m2438j.DEFAULT_PERSISTENT_BLOCKER_SUMMARY_DOC_PATH,
    source_2438h_doc_path: Annotated[
        Path, typer.Option("--source-2438h-doc")
    ] = m2438j.DEFAULT_SOURCE_2438H_DOC_PATH,
    source_2438f_doc_path: Annotated[
        Path, typer.Option("--source-2438f-doc")
    ] = m2438j.DEFAULT_SOURCE_2438F_DOC_PATH,
    source_2438d_doc_path: Annotated[
        Path, typer.Option("--source-2438d-doc")
    ] = m2438j.DEFAULT_SOURCE_2438D_DOC_PATH,
    candidate_output_records_doc_path: Annotated[
        Path, typer.Option("--candidate-output-records-doc")
    ] = m2438j.DEFAULT_CANDIDATE_OUTPUT_RECORDS_DOC_PATH,
    source_2438b_doc_path: Annotated[
        Path, typer.Option("--source-2438b-doc")
    ] = m2438j.DEFAULT_SOURCE_2438B_DOC_PATH,
    requirement_doc_path: Annotated[
        Path, typer.Option("--requirement-doc")
    ] = m2438j.DEFAULT_REQUIREMENT_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2438j.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2438j.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2438j.DEFAULT_SYSTEM_FLOW_PATH,
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = m2438j.DEFAULT_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path")
    ] = m2438j.DEFAULT_RATES_PATH,
    data_quality_summary_path: Annotated[
        Path | None, typer.Option("--data-quality-summary")
    ] = None,
    data_quality_output_path: Annotated[
        Path | None, typer.Option("--data-quality-output")
    ] = None,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2438j.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2438j.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = (
        m2438j.run_growth_tilt_persistent_candidate_pit_replay_blocker_escalation(
            source_2438i_blocked_recheck_path=source_2438i_blocked_recheck_path,
            persistent_candidate_replay_blocker_summary_path=(
                persistent_candidate_replay_blocker_summary_path
            ),
            source_2438h_remaining_blocker_closure_path=(
                source_2438h_remaining_blocker_closure_path
            ),
            source_2438f_candidate_level_blocker_closure_path=(
                source_2438f_candidate_level_blocker_closure_path
            ),
            source_2438d_output_closure_path=source_2438d_output_closure_path,
            candidate_replay_output_records_path=candidate_replay_output_records_path,
            source_2438b_engine_blocker_closure_path=(
                source_2438b_engine_blocker_closure_path
            ),
            source_2438i_doc_path=source_2438i_doc_path,
            persistent_blocker_summary_doc_path=persistent_blocker_summary_doc_path,
            source_2438h_doc_path=source_2438h_doc_path,
            source_2438f_doc_path=source_2438f_doc_path,
            source_2438d_doc_path=source_2438d_doc_path,
            candidate_output_records_doc_path=candidate_output_records_doc_path,
            source_2438b_doc_path=source_2438b_doc_path,
            requirement_doc_path=requirement_doc_path,
            report_registry_path=report_registry_path,
            artifact_catalog_path=artifact_catalog_path,
            system_flow_path=system_flow_path,
            prices_path=prices_path,
            rates_path=rates_path,
            data_quality_summary_path=data_quality_summary_path,
            data_quality_output_path=data_quality_output_path,
            output_root=output_root,
            docs_root=docs_root,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_execution_semantics_payload(
        "Growth tilt persistent candidate PIT replay blocker escalation",
        payload,
    )
    for field in (
        "readiness_status",
        "prior_status",
        "source_2438i_blocked_recheck_ready",
        "persistent_blocker_escalation_required",
        "persistent_blocker_escalation_ready",
        "data_quality_gate_executed",
        "data_quality_gate_passed",
        "data_quality_status",
        "candidate_replay_outputs_complete",
        "candidate_replay_output_record_count",
        "candidate_replay_pass_count",
        "candidate_replay_fail_count",
        "candidate_replay_blocked_count",
        "persistent_blocked_candidate_count",
        "persistent_candidate_replay_blocker_count",
        "closure_history_confirmed",
        "pit_replay_engine_blocker_closure_ready",
        "output_completeness_closure_ready",
        "candidate_level_blocker_closure_ready",
        "remaining_blocker_closure_ready",
        "all_escalation_records_have_root_cause_category",
        "all_escalation_records_have_root_cause_layer",
        "all_escalation_records_have_recommended_next_action",
        "all_escalation_records_have_blocker_reason",
        "registry_catalog_docs_alignment",
        "forward_aging_handoff_ready",
        "forward_aging_candidate_count",
        "paper_shadow_candidate_found",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "production_enabled",
        "broker_enabled",
        "automatic_execution_allowed",
        "generated_trading_advice",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "source_validation_error_count",
        "evidence_gap_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_forward_aging_candidate_pack_command(
    source_2438_pit_replay_path: Annotated[
        Path, typer.Option("--source-2438-pit-replay")
    ] = m2439.DEFAULT_SOURCE_2438_PIT_REPLAY_PATH,
    pit_replay_evidence_path: Annotated[
        Path, typer.Option("--pit-replay-evidence")
    ] = m2439.DEFAULT_PIT_REPLAY_EVIDENCE_PATH,
    pit_replay_blocker_summary_path: Annotated[
        Path, typer.Option("--pit-replay-blocker-summary")
    ] = m2439.DEFAULT_PIT_REPLAY_BLOCKER_SUMMARY_PATH,
    pit_replay_doc_path: Annotated[
        Path, typer.Option("--pit-replay-doc")
    ] = m2439.DEFAULT_PIT_REPLAY_DOC_PATH,
    pit_replay_evidence_doc_path: Annotated[
        Path, typer.Option("--pit-replay-evidence-doc")
    ] = m2439.DEFAULT_PIT_REPLAY_EVIDENCE_DOC_PATH,
    pit_replay_blocker_doc_path: Annotated[
        Path, typer.Option("--pit-replay-blocker-doc")
    ] = m2439.DEFAULT_PIT_REPLAY_BLOCKER_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2439.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2439.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2439.DEFAULT_SYSTEM_FLOW_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = m2439.DEFAULT_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = m2439.DEFAULT_RATES_PATH,
    data_quality_summary_path: Annotated[
        Path | None, typer.Option("--data-quality-summary")
    ] = None,
    data_quality_output_path: Annotated[
        Path | None, typer.Option("--data-quality-output")
    ] = None,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2439.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2439.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2439.run_growth_tilt_forward_aging_candidate_pack(
        source_2438_pit_replay_path=source_2438_pit_replay_path,
        pit_replay_evidence_path=pit_replay_evidence_path,
        pit_replay_blocker_summary_path=pit_replay_blocker_summary_path,
        pit_replay_doc_path=pit_replay_doc_path,
        pit_replay_evidence_doc_path=pit_replay_evidence_doc_path,
        pit_replay_blocker_doc_path=pit_replay_blocker_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        prices_path=prices_path,
        rates_path=rates_path,
        data_quality_summary_path=data_quality_summary_path,
        data_quality_output_path=data_quality_output_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt forward aging candidate pack",
        payload,
    )
    for field in (
        "readiness_status",
        "source_2438_ready",
        "pit_replay_source_status",
        "pit_replay_pass_candidate_count",
        "pit_replay_pass_count_from_source",
        "pit_replay_tested_count_from_source",
        "pit_replay_blocked_count_from_source",
        "data_quality_gate_executed",
        "data_quality_gate_passed",
        "data_quality_status",
        "forward_aging_candidate_pack_ready",
        "candidate_tracking_artifact_ready",
        "forward_observation_contract_ready",
        "no_effect_boundary_ready",
        "forward_aging_candidate_count",
        "forward_aging_candidate_count_if_unblocked",
        "valid_until_outcome_capture_ready",
        "candidate_evidence_refresh_cadence",
        "forward_aging_observation_started",
        "forward_aging_observation_written",
        "candidate_tracking_started",
        "evidence_gap_count",
        "historical_screen_run",
        "pit_replay_run",
        "backtest_run",
        "scoring_run",
        "daily_report_run",
        "fresh_market_data_read",
        "fresh_outcome_data_read",
        "manual_review_required",
        "automatic_execution_allowed",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "outcome_backfilled",
        "outcome_binding_executed",
        "outcome_store_mutated",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"observation_horizons={payload.get('observation_horizons')}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_paper_shadow_candidate_promotion_review_command(
    source_2431_existing_candidate_evidence_path: Annotated[
        Path, typer.Option("--source-2431-existing-candidate-evidence")
    ] = m2440.DEFAULT_SOURCE_2431_PATH,
    source_2432_candidate_gauntlet_path: Annotated[
        Path, typer.Option("--source-2432-candidate-gauntlet")
    ] = m2440.DEFAULT_SOURCE_2432_PATH,
    source_2434_component_validation_path: Annotated[
        Path, typer.Option("--source-2434-component-validation")
    ] = m2440.DEFAULT_SOURCE_2434_PATH,
    source_2437_regime_review_path: Annotated[
        Path, typer.Option("--source-2437-regime-review")
    ] = m2440.DEFAULT_SOURCE_2437_PATH,
    source_2438_pit_replay_path: Annotated[
        Path, typer.Option("--source-2438-pit-replay")
    ] = m2440.DEFAULT_SOURCE_2438_PATH,
    source_2439_forward_pack_path: Annotated[
        Path, typer.Option("--source-2439-forward-pack")
    ] = m2440.DEFAULT_SOURCE_2439_PATH,
    source_2431_doc_path: Annotated[
        Path, typer.Option("--source-2431-doc")
    ] = m2440.DEFAULT_SOURCE_2431_DOC_PATH,
    source_2432_doc_path: Annotated[
        Path, typer.Option("--source-2432-doc")
    ] = m2440.DEFAULT_SOURCE_2432_DOC_PATH,
    source_2434_doc_path: Annotated[
        Path, typer.Option("--source-2434-doc")
    ] = m2440.DEFAULT_SOURCE_2434_DOC_PATH,
    source_2437_doc_path: Annotated[
        Path, typer.Option("--source-2437-doc")
    ] = m2440.DEFAULT_SOURCE_2437_DOC_PATH,
    source_2438_doc_path: Annotated[
        Path, typer.Option("--source-2438-doc")
    ] = m2440.DEFAULT_SOURCE_2438_DOC_PATH,
    source_2439_doc_path: Annotated[
        Path, typer.Option("--source-2439-doc")
    ] = m2440.DEFAULT_SOURCE_2439_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2440.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2440.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2440.DEFAULT_SYSTEM_FLOW_PATH,
    prices_path: Annotated[Path, typer.Option("--prices-path")] = m2440.DEFAULT_PRICES_PATH,
    rates_path: Annotated[Path, typer.Option("--rates-path")] = m2440.DEFAULT_RATES_PATH,
    data_quality_summary_path: Annotated[
        Path | None, typer.Option("--data-quality-summary")
    ] = None,
    data_quality_output_path: Annotated[
        Path | None, typer.Option("--data-quality-output")
    ] = None,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2440.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2440.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2440.run_growth_tilt_paper_shadow_candidate_promotion_review(
        source_2431_existing_candidate_evidence_path=(
            source_2431_existing_candidate_evidence_path
        ),
        source_2432_candidate_gauntlet_path=source_2432_candidate_gauntlet_path,
        source_2434_component_validation_path=source_2434_component_validation_path,
        source_2437_regime_review_path=source_2437_regime_review_path,
        source_2438_pit_replay_path=source_2438_pit_replay_path,
        source_2439_forward_pack_path=source_2439_forward_pack_path,
        source_2431_doc_path=source_2431_doc_path,
        source_2432_doc_path=source_2432_doc_path,
        source_2434_doc_path=source_2434_doc_path,
        source_2437_doc_path=source_2437_doc_path,
        source_2438_doc_path=source_2438_doc_path,
        source_2439_doc_path=source_2439_doc_path,
        report_registry_path=report_registry_path,
        artifact_catalog_path=artifact_catalog_path,
        system_flow_path=system_flow_path,
        prices_path=prices_path,
        rates_path=rates_path,
        data_quality_summary_path=data_quality_summary_path,
        data_quality_output_path=data_quality_output_path,
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_execution_semantics_payload(
        "Growth tilt paper-shadow candidate promotion review",
        payload,
    )
    for field in (
        "readiness_status",
        "source_2431_ready",
        "source_2432_ready",
        "source_2434_ready",
        "source_2437_ready",
        "source_2438_ready",
        "source_2439_forward_aging_ready",
        "forward_aging_source_status",
        "pit_replay_source_status",
        "data_quality_gate_executed",
        "data_quality_gate_passed",
        "data_quality_status",
        "promotion_review_ready",
        "evidence_summary_ready",
        "candidate_decision_matrix_ready",
        "blocked_promotion_route_ready",
        "no_effect_boundary_ready",
        "forward_aging_candidate_count",
        "review_candidate_count",
        "paper_shadow_candidate_found",
        "paper_shadow_candidate_count",
        "evidence_gap_count",
        "forward_aging_observation_started",
        "forward_aging_observation_written",
        "candidate_tracking_started",
        "historical_screen_run",
        "pit_replay_run",
        "backtest_run",
        "scoring_run",
        "daily_report_run",
        "fresh_market_data_read",
        "fresh_outcome_data_read",
        "manual_review_required",
        "automatic_execution_allowed",
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "trading_advice_generated",
        "actionable_allocation_generated",
        "outcome_backfilled",
        "outcome_binding_executed",
        "outcome_store_mutated",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "paper_shadow_daily_job_run",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "source_validation_error_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


