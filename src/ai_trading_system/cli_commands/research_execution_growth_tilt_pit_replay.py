from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system import (
    dynamic_strategy_growth_tilt_persistent_candidate_pit_replay_blocker_escalation as m2438j,  # noqa: E501
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation as m2438k,  # noqa: E501
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_pit_replay_engine_blocker_closure as m2438b,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution as m2438m,  # noqa: E501
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
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation as m2438l,  # noqa: E501
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure as m2438d,
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


def register_growth_tilt_pit_replay_strategy_commands(strategies_app: typer.Typer) -> None:
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
    strategies_app.command(
        "growth-tilt-persistent-candidate-pit-replay-blocker-root-cause-remediation"
    )(
        _growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation_command
    )
    strategies_app.command(
        "growth-tilt-top3-candidate-pit-replay-recheck-after-runtime-remediation"
    )(_growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation_command)
    strategies_app.command(
        "growth-tilt-post-runtime-candidate-pit-replay-blocker-resolution"
    )(_growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution_command)


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


def _growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation_command(
    source_2438j_escalation_path: Annotated[
        Path, typer.Option("--source-2438j-escalation")
    ] = m2438k.DEFAULT_SOURCE_2438J_ESCALATION_PATH,
    root_cause_matrix_path: Annotated[
        Path, typer.Option("--root-cause-matrix")
    ] = m2438k.DEFAULT_ROOT_CAUSE_MATRIX_PATH,
    source_2438i_blocked_recheck_path: Annotated[
        Path, typer.Option("--source-2438i-blocked-recheck")
    ] = m2438k.DEFAULT_SOURCE_2438I_BLOCKED_RECHECK_PATH,
    source_2438h_remaining_blocker_closure_path: Annotated[
        Path, typer.Option("--source-2438h-remaining-blocker-closure")
    ] = m2438k.DEFAULT_SOURCE_2438H_REMAINING_BLOCKER_CLOSURE_PATH,
    source_2438f_candidate_level_blocker_closure_path: Annotated[
        Path, typer.Option("--source-2438f-candidate-level-blocker-closure")
    ] = m2438k.DEFAULT_SOURCE_2438F_CANDIDATE_LEVEL_BLOCKER_CLOSURE_PATH,
    source_2438d_output_closure_path: Annotated[
        Path, typer.Option("--source-2438d-output-closure")
    ] = m2438k.DEFAULT_SOURCE_2438D_OUTPUT_CLOSURE_PATH,
    candidate_replay_output_records_path: Annotated[
        Path, typer.Option("--candidate-replay-output-records")
    ] = m2438k.DEFAULT_CANDIDATE_REPLAY_OUTPUT_RECORDS_PATH,
    source_2438b_engine_blocker_closure_path: Annotated[
        Path, typer.Option("--source-2438b-engine-blocker-closure")
    ] = m2438k.DEFAULT_SOURCE_2438B_ENGINE_BLOCKER_CLOSURE_PATH,
    source_2438j_doc_path: Annotated[
        Path, typer.Option("--source-2438j-doc")
    ] = m2438k.DEFAULT_SOURCE_2438J_DOC_PATH,
    root_cause_matrix_doc_path: Annotated[
        Path, typer.Option("--root-cause-matrix-doc")
    ] = m2438k.DEFAULT_ROOT_CAUSE_MATRIX_DOC_PATH,
    source_2438i_doc_path: Annotated[
        Path, typer.Option("--source-2438i-doc")
    ] = m2438k.DEFAULT_SOURCE_2438I_DOC_PATH,
    source_2438h_doc_path: Annotated[
        Path, typer.Option("--source-2438h-doc")
    ] = m2438k.DEFAULT_SOURCE_2438H_DOC_PATH,
    source_2438f_doc_path: Annotated[
        Path, typer.Option("--source-2438f-doc")
    ] = m2438k.DEFAULT_SOURCE_2438F_DOC_PATH,
    source_2438d_doc_path: Annotated[
        Path, typer.Option("--source-2438d-doc")
    ] = m2438k.DEFAULT_SOURCE_2438D_DOC_PATH,
    candidate_output_records_doc_path: Annotated[
        Path, typer.Option("--candidate-output-records-doc")
    ] = m2438k.DEFAULT_CANDIDATE_OUTPUT_RECORDS_DOC_PATH,
    source_2438b_doc_path: Annotated[
        Path, typer.Option("--source-2438b-doc")
    ] = m2438k.DEFAULT_SOURCE_2438B_DOC_PATH,
    requirement_doc_path: Annotated[
        Path, typer.Option("--requirement-doc")
    ] = m2438k.DEFAULT_REQUIREMENT_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2438k.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2438k.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2438k.DEFAULT_SYSTEM_FLOW_PATH,
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = m2438k.DEFAULT_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path")
    ] = m2438k.DEFAULT_RATES_PATH,
    data_quality_summary_path: Annotated[
        Path | None, typer.Option("--data-quality-summary")
    ] = None,
    data_quality_output_path: Annotated[
        Path | None, typer.Option("--data-quality-output")
    ] = None,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2438k.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2438k.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = m2438k.run_growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation(
        source_2438j_escalation_path=source_2438j_escalation_path,
        root_cause_matrix_path=root_cause_matrix_path,
        source_2438i_blocked_recheck_path=source_2438i_blocked_recheck_path,
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
        source_2438j_doc_path=source_2438j_doc_path,
        root_cause_matrix_doc_path=root_cause_matrix_doc_path,
        source_2438i_doc_path=source_2438i_doc_path,
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
    _print_execution_semantics_payload(
        "Growth tilt persistent candidate PIT replay blocker root-cause remediation",
        payload,
    )
    for field in (
        "readiness_status",
        "prior_status",
        "source_2438j_escalation_ready",
        "prior_root_cause_matched",
        "root_cause_remediation_ready",
        "data_quality_gate_executed",
        "data_quality_gate_passed",
        "data_quality_status",
        "runtime_blocker_count_before",
        "runtime_blocker_count_after",
        "replay_runtime_materialization_ready",
        "candidate_replay_runtime_executable",
        "candidate_replay_runtime_executable_count",
        "candidate_spec_to_runtime_input_adapter_ready",
        "replay_runtime_entrypoint_ready",
        "replay_window_materialization_ready",
        "baseline_comparison_runtime_ready",
        "metric_materialization_runtime_ready",
        "pass_fail_threshold_evaluator_ready",
        "source_traceability_runtime_bindings_ready",
        "as_of_boundary_enforced_at_runtime",
        "valid_until_policy_bound_at_runtime",
        "outcome_linkage_key_runtime_bound",
        "forward_aging_handoff_key_runtime_bound",
        "execution_audit_trail_ready",
        "deterministic_runtime_output_supported",
        "candidate_replay_outcome_rechecked",
        "candidate_replay_pass_count",
        "candidate_replay_fail_count",
        "candidate_replay_blocked_count",
        "forward_aging_handoff_ready",
        "forward_aging_candidate_count",
        "paper_shadow_candidate_found",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "production_enabled",
        "broker_enabled",
        "generated_trading_advice",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "source_validation_error_count",
        "evidence_gap_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation_command(
    source_2438k_runtime_remediation_path: Annotated[
        Path, typer.Option("--source-2438k-runtime-remediation")
    ] = m2438l.DEFAULT_SOURCE_2438K_RUNTIME_REMEDIATION_PATH,
    executable_replay_readiness_handoff_path: Annotated[
        Path, typer.Option("--executable-replay-readiness-handoff")
    ] = m2438l.DEFAULT_EXECUTABLE_REPLAY_READINESS_HANDOFF_PATH,
    runtime_materialization_remediation_path: Annotated[
        Path, typer.Option("--runtime-materialization-remediation")
    ] = m2438l.DEFAULT_RUNTIME_MATERIALIZATION_REMEDIATION_PATH,
    runtime_execution_audit_trail_path: Annotated[
        Path, typer.Option("--runtime-execution-audit-trail")
    ] = m2438l.DEFAULT_RUNTIME_EXECUTION_AUDIT_TRAIL_PATH,
    candidate_replay_output_records_path: Annotated[
        Path, typer.Option("--candidate-replay-output-records")
    ] = m2438l.DEFAULT_CANDIDATE_REPLAY_OUTPUT_RECORDS_PATH,
    source_2438k_doc_path: Annotated[
        Path, typer.Option("--source-2438k-doc")
    ] = m2438l.DEFAULT_SOURCE_2438K_DOC_PATH,
    executable_handoff_doc_path: Annotated[
        Path, typer.Option("--executable-handoff-doc")
    ] = m2438l.DEFAULT_EXECUTABLE_HANDOFF_DOC_PATH,
    runtime_materialization_doc_path: Annotated[
        Path, typer.Option("--runtime-materialization-doc")
    ] = m2438l.DEFAULT_RUNTIME_MATERIALIZATION_DOC_PATH,
    runtime_audit_doc_path: Annotated[
        Path, typer.Option("--runtime-audit-doc")
    ] = m2438l.DEFAULT_RUNTIME_AUDIT_DOC_PATH,
    candidate_output_records_doc_path: Annotated[
        Path, typer.Option("--candidate-output-records-doc")
    ] = m2438l.DEFAULT_CANDIDATE_OUTPUT_RECORDS_DOC_PATH,
    requirement_doc_path: Annotated[
        Path, typer.Option("--requirement-doc")
    ] = m2438l.DEFAULT_REQUIREMENT_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2438l.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2438l.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2438l.DEFAULT_SYSTEM_FLOW_PATH,
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = m2438l.DEFAULT_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path")
    ] = m2438l.DEFAULT_RATES_PATH,
    data_quality_summary_path: Annotated[
        Path | None, typer.Option("--data-quality-summary")
    ] = None,
    data_quality_output_path: Annotated[
        Path | None, typer.Option("--data-quality-output")
    ] = None,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2438l.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2438l.DEFAULT_DOCS_ROOT,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = (
        m2438l.run_growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation(
            source_2438k_runtime_remediation_path=(
                source_2438k_runtime_remediation_path
            ),
            executable_replay_readiness_handoff_path=(
                executable_replay_readiness_handoff_path
            ),
            runtime_materialization_remediation_path=(
                runtime_materialization_remediation_path
            ),
            runtime_execution_audit_trail_path=runtime_execution_audit_trail_path,
            candidate_replay_output_records_path=candidate_replay_output_records_path,
            source_2438k_doc_path=source_2438k_doc_path,
            executable_handoff_doc_path=executable_handoff_doc_path,
            runtime_materialization_doc_path=runtime_materialization_doc_path,
            runtime_audit_doc_path=runtime_audit_doc_path,
            candidate_output_records_doc_path=candidate_output_records_doc_path,
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
        "Growth tilt top-3 candidate PIT replay recheck after runtime remediation",
        payload,
    )
    for field in (
        "readiness_status",
        "prior_status",
        "source_2438k_runtime_remediation_ready",
        "runtime_remediation_ready",
        "runtime_blocker_count_after",
        "candidate_replay_runtime_executable_count",
        "executable_replay_readiness_handoff_ready",
        "runtime_remediation_record_count",
        "candidate_replay_outputs_complete",
        "candidate_replay_output_record_count",
        "runtime_metric_materialization_output_ready",
        "baseline_comparison_runtime_output_ready",
        "threshold_evaluator_runtime_output_ready",
        "candidate_replay_outcome_rechecked",
        "candidate_replay_pass_count",
        "candidate_replay_fail_count",
        "candidate_replay_blocked_count",
        "post_runtime_candidate_replay_blocker_count",
        "forward_aging_handoff_ready",
        "forward_aging_candidate_count",
        "paper_shadow_candidate_found",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "production_enabled",
        "broker_enabled",
        "generated_trading_advice",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "data_quality_gate_executed",
        "data_quality_gate_passed",
        "data_quality_status",
        "source_validation_error_count",
        "evidence_gap_count",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


def _growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution_command(
    source_2438l_path: Annotated[
        Path, typer.Option("--source-artifact", "--source-2438l")
    ] = m2438m.DEFAULT_SOURCE_2438L_PATH,
    candidate_config_path: Annotated[
        Path, typer.Option("--candidate-config")
    ] = m2438m.DEFAULT_CANDIDATE_CONFIG_PATH,
    engine_contract_path: Annotated[
        Path, typer.Option("--engine-contract")
    ] = m2438m.DEFAULT_ENGINE_CONTRACT_PATH,
    runtime_evaluation_input_path: Annotated[
        Path | None, typer.Option("--runtime-evaluation-input")
    ] = None,
    requirement_doc_path: Annotated[
        Path, typer.Option("--requirement-doc")
    ] = m2438m.DEFAULT_REQUIREMENT_DOC_PATH,
    report_registry_path: Annotated[
        Path, typer.Option("--report-registry")
    ] = m2438m.DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Annotated[
        Path, typer.Option("--artifact-catalog")
    ] = m2438m.DEFAULT_ARTIFACT_CATALOG_PATH,
    system_flow_path: Annotated[
        Path, typer.Option("--system-flow")
    ] = m2438m.DEFAULT_SYSTEM_FLOW_PATH,
    prices_path: Annotated[
        Path, typer.Option("--prices-path")
    ] = m2438m.DEFAULT_PRICES_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path")
    ] = m2438m.DEFAULT_RATES_PATH,
    data_quality_summary_path: Annotated[
        Path | None, typer.Option("--data-quality-summary")
    ] = None,
    data_quality_output_path: Annotated[
        Path | None, typer.Option("--data-quality-output")
    ] = None,
    source_run_id: Annotated[
        str | None, typer.Option("--source-run-id")
    ] = None,
    candidate_limit: Annotated[int, typer.Option("--candidate-limit")] = 3,
    output_root: Annotated[
        Path, typer.Option("--output-root")
    ] = m2438m.DEFAULT_OUTPUT_ROOT,
    docs_root: Annotated[
        Path, typer.Option("--docs-root")
    ] = m2438m.DEFAULT_DOCS_ROOT,
    strict: Annotated[bool, typer.Option("--strict")] = False,
    as_of: Annotated[str | None, typer.Option("--as-of")] = None,
) -> None:
    payload = (
        m2438m.run_growth_tilt_post_runtime_candidate_pit_replay_blocker_resolution(
            source_2438l_path=source_2438l_path,
            candidate_config_path=candidate_config_path,
            engine_contract_path=engine_contract_path,
            runtime_evaluation_input_path=runtime_evaluation_input_path,
            requirement_doc_path=requirement_doc_path,
            report_registry_path=report_registry_path,
            artifact_catalog_path=artifact_catalog_path,
            system_flow_path=system_flow_path,
            prices_path=prices_path,
            rates_path=rates_path,
            data_quality_summary_path=data_quality_summary_path,
            data_quality_output_path=data_quality_output_path,
            source_run_id=source_run_id,
            candidate_limit=candidate_limit,
            output_root=output_root,
            docs_root=docs_root,
            strict=strict,
            as_of_date=_parse_optional_date(as_of),
        )
    )
    _print_execution_semantics_payload(
        "Growth tilt post-runtime candidate PIT replay blocker resolution",
        payload,
    )
    for field in (
        "readiness_status",
        "candidate_count",
        "runtime_invoked_candidate_count",
        "raw_runtime_output_present_count",
        "required_runtime_metric_count",
        "computed_runtime_metric_count",
        "null_runtime_metric_count",
        "invalid_runtime_metric_count",
        "required_threshold_evaluation_count",
        "completed_threshold_evaluation_count",
        "missing_threshold_evaluation_count",
        "pass_count",
        "fail_count",
        "blocked_count",
        "resolved_blocker_count",
        "unresolved_blocker_count",
        "data_quality_gate_executed",
        "data_quality_gate_passed",
        "data_quality_status",
        "source_validation_error_count",
        "strict_validation_error_count",
        "evidence_gap_count",
        "paper_shadow_enabled",
        "production_enabled",
        "broker_enabled",
        "portfolio_weight_mutated",
    ):
        console.print(f"{field}={_cli_scalar(payload.get(field))}")
    console.print(f"next_route={payload.get('recommended_next_research_task')}")


