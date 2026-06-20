from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.indicator_research import (
    DEFAULT_INDICATOR_OUTPUT_ROOT,
    DEFAULT_INDICATOR_REGISTRY_PATH,
    DEFAULT_MASKING_ABLATION_CAP_RATIO,
    DEFAULT_MASKING_OUTCOME_TICKER,
    DEFAULT_THRESHOLD_REGISTRY_PATH,
    IndicatorResearchError,
    build_backtest_trace_bridge,
    build_component_level_historical_trace,
    build_coverage_audit,
    build_daily_indicator_coverage_gap_report,
    build_daily_indicator_inventory,
    build_dependency_graph,
    build_dynamic_trend_threshold_calibration_prep_report,
    build_dynamic_trend_threshold_sensitivity_review,
    build_gate_availability_audit,
    build_historical_multi_stage_weight_trace_validation,
    build_indicator_diagnostics,
    build_indicator_research_gate,
    build_indicator_research_validation_rollup,
    build_lineage_manifest_repair_report,
    build_long_horizon_evidence_floor_calibration_audit,
    build_mapping_plan,
    build_masking_audit,
    build_masking_casebook,
    build_multi_stage_weight_trace_contract,
    build_ontology_payload,
    build_threshold_calibration_followup_plan,
    build_threshold_calibration_report,
    build_threshold_prioritization_report,
    build_threshold_registry_audit,
    build_valuation_crowding_ablation_validation,
    build_valuation_crowding_masking_effectiveness_review,
    build_valuation_crowding_masking_robustness_review,
    build_valuation_crowding_outcome_availability_audit,
    build_valuation_crowding_pilot_audit,
    build_valuation_crowding_pilot_validation_report,
    write_indicator_artifact_pair,
    write_indicator_framework_validation_pack,
    write_indicator_validation_pack_stability_report,
)
from ai_trading_system.research_campaign import (
    DEFAULT_CAMPAIGN_OUTPUT_ROOT,
    DEFAULT_CAMPAIGN_ROOT,
    DEFAULT_GATE_POLICY_PATH,
    DEFAULT_MIGRATION_PATH,
    DEFAULT_MODULE_REGISTRY_PATH,
    DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
    DEFAULT_WINDOW_POLICY_PATH,
    ResearchCampaignError,
    archive_campaign,
    build_campaign_validation_payload,
    build_case_specific_runner_deprecation_plan,
    build_owner_packet,
    build_status_payload,
    campaign_plan,
    diagnose_campaign,
    evaluate_gate,
    initialize_campaign,
    load_campaign_bundle,
    load_campaign_spec,
    run_campaign_stage,
    validate_stage_adapter_contracts,
    write_campaign_control_plane_v1_validation_artifacts,
)

console = Console()
research_app = typer.Typer(help="研究 Campaign 控制面。", no_args_is_help=True)
campaign_app = typer.Typer(
    help="Research Campaign spec、状态机、证据和 owner packet。",
    no_args_is_help=True,
)
indicators_app = typer.Typer(
    help="日报指标、约束、mapping、遮蔽和研究 gate 控制面。",
    no_args_is_help=True,
)
research_app.add_typer(campaign_app, name="campaign")
research_app.add_typer(indicators_app, name="indicators")


@indicators_app.command("ontology")
def indicator_ontology_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """验证并输出 indicator / signal / constraint ontology。"""
    payload = _build_indicator_payload(lambda: build_ontology_payload(registry_path=registry_path))
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="indicator_research_ontology",
    )
    _print_indicator_artifact("Indicator ontology", payload, paths)


@indicators_app.command("inventory")
def indicator_inventory_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """扫描当前日报指标、约束和 heuristic registry，输出全量 inventory。"""
    payload = _build_indicator_payload(
        lambda: build_daily_indicator_inventory(registry_path=registry_path)
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="daily_indicator_inventory",
    )
    _print_indicator_artifact("Daily indicator inventory", payload, paths)


@indicators_app.command("coverage")
def indicator_coverage_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 research coverage 分类和高影响未验证缺口。"""
    payload = _build_indicator_payload(lambda: build_coverage_audit(registry_path=registry_path))
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="indicator_research_coverage_audit",
    )
    _print_indicator_artifact("Indicator coverage audit", payload, paths)


@indicators_app.command("coverage-gap")
def indicator_coverage_gap_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选日报 multi-stage weight trace JSON。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出日报指标 coverage gap report。"""
    payload = _build_indicator_payload(
        lambda: build_daily_indicator_coverage_gap_report(
            registry_path=registry_path,
            trace_path=trace_path,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="daily_indicator_coverage_gap_report",
    )
    _print_indicator_artifact("Daily indicator coverage gap", payload, paths)


@indicators_app.command("threshold-audit")
def indicator_threshold_audit_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Annotated[
        Path,
        typer.Option("--threshold-registry", help="Threshold registry 路径。"),
    ] = DEFAULT_THRESHOLD_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 TRADING-693 threshold registry audit。"""
    payload = _build_indicator_payload(
        lambda: build_threshold_registry_audit(
            registry_path=registry_path,
            threshold_registry_path=threshold_registry_path,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="threshold_registry_audit",
    )
    _print_indicator_artifact("Threshold registry audit", payload, paths)


@indicators_app.command("threshold-prioritization")
def indicator_threshold_prioritization_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Annotated[
        Path,
        typer.Option("--threshold-registry", help="Threshold registry 路径。"),
    ] = DEFAULT_THRESHOLD_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 TRADING-693 high-impact threshold prioritization report。"""
    payload = _build_indicator_payload(
        lambda: build_threshold_prioritization_report(
            registry_path=registry_path,
            threshold_registry_path=threshold_registry_path,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="threshold_prioritization_report",
    )
    _print_indicator_artifact("Threshold prioritization report", payload, paths)


@indicators_app.command("threshold-calibration")
def indicator_threshold_calibration_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Annotated[
        Path,
        typer.Option("--threshold-registry", help="Threshold registry 路径。"),
    ] = DEFAULT_THRESHOLD_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage / historical trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 realized outcome prices CSV。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit root。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option(
            "--bridge-artifact-root", help="可选 backtest/advisory bridge artifact root。"
        ),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="默认 outcome ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: Annotated[
        float,
        typer.Option("--capped-masking-ratio", help="Capped masking counterfactual ratio。"),
    ] = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 trace/filter start date。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 trace/filter end date。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选 event window start。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选 event window end。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="逗号分隔 asset universe。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 TRADING-695 indicator research threshold calibration sensitivity report。"""
    payload = _build_indicator_payload(
        lambda: build_threshold_calibration_report(
            registry_path=registry_path,
            threshold_registry_path=threshold_registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            outcome_ticker=outcome_ticker,
            capped_masking_ratio=capped_masking_ratio,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="threshold_calibration_report",
    )
    _print_indicator_artifact("Threshold calibration report", payload, paths)


@indicators_app.command("threshold-calibration-followup")
def indicator_threshold_calibration_followup_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Annotated[
        Path,
        typer.Option("--threshold-registry", help="Threshold registry 路径。"),
    ] = DEFAULT_THRESHOLD_REGISTRY_PATH,
    calibration_report_path: Annotated[
        Path | None,
        typer.Option("--calibration-report", help="可选 threshold_calibration_report JSON。"),
    ] = None,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="无 calibration report 时可选 trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="无 calibration report 时可选 prices CSV。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="无 calibration report 时可选 gate audit root。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option(
            "--bridge-artifact-root",
            help="无 calibration report 时可选 bridge artifact root。",
        ),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="默认 outcome ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: Annotated[
        float,
        typer.Option("--capped-masking-ratio", help="Capped masking counterfactual ratio。"),
    ] = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 trace/filter start date。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 trace/filter end date。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选 event window start。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选 event window end。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="逗号分隔 asset universe。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 TRADING-696 threshold calibration follow-up plan。"""
    payload = _build_indicator_payload(
        lambda: build_threshold_calibration_followup_plan(
            registry_path=registry_path,
            threshold_registry_path=threshold_registry_path,
            calibration_report_path=calibration_report_path,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            outcome_ticker=outcome_ticker,
            capped_masking_ratio=capped_masking_ratio,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="threshold_calibration_followup_plan",
    )
    _print_indicator_artifact("Threshold calibration follow-up plan", payload, paths)


@indicators_app.command("dynamic-trend-threshold-calibration-prep")
def indicator_dynamic_trend_threshold_calibration_prep_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Annotated[
        Path,
        typer.Option("--threshold-registry", help="Threshold registry 路径。"),
    ] = DEFAULT_THRESHOLD_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 TRADING-697 dynamic allocation / trend threshold calibration prep report。"""
    payload = _build_indicator_payload(
        lambda: build_dynamic_trend_threshold_calibration_prep_report(
            registry_path=registry_path,
            threshold_registry_path=threshold_registry_path,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="dynamic_trend_threshold_calibration_prep_report",
    )
    _print_indicator_artifact("Dynamic/trend threshold calibration prep report", payload, paths)


@indicators_app.command("dynamic-trend-threshold-sensitivity-review")
def indicator_dynamic_trend_threshold_sensitivity_review_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Annotated[
        Path,
        typer.Option("--threshold-registry", help="Threshold registry 路径。"),
    ] = DEFAULT_THRESHOLD_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage / historical trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 realized outcome prices CSV。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit root。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option(
            "--bridge-artifact-root", help="可选 backtest/advisory bridge artifact root。"
        ),
    ] = None,
    coverage_extension_root: Annotated[
        Path | None,
        typer.Option(
            "--coverage-extension-root",
            help="可选 TRADING-699 coverage extension root，例如 outputs/research_campaigns。",
        ),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="默认 outcome ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 trace/filter start date。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 trace/filter end date。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选 event window start。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选 event window end。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="逗号分隔 asset universe。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 TRADING-698 dynamic allocation / trend threshold sensitivity review。"""
    payload = _build_indicator_payload(
        lambda: build_dynamic_trend_threshold_sensitivity_review(
            registry_path=registry_path,
            threshold_registry_path=threshold_registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            coverage_extension_root=coverage_extension_root,
            outcome_ticker=outcome_ticker,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="dynamic_trend_threshold_sensitivity_review",
    )
    _print_indicator_artifact(
        "Dynamic/trend threshold sensitivity review",
        payload,
        paths,
    )


@indicators_app.command("graph")
def indicator_graph_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 Feature -> Indicator -> Mapping -> Constraint -> Research Weight 依赖图。"""
    payload = _build_indicator_payload(
        lambda: build_dependency_graph(registry_path=registry_path, trace_path=trace_path)
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="indicator_dependency_graph",
    )
    _print_indicator_artifact("Indicator dependency graph", payload, paths)


@indicators_app.command("trace-contract")
def indicator_trace_contract_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 multi-stage research weight trace contract。"""
    payload = _build_indicator_payload(
        lambda: build_multi_stage_weight_trace_contract(registry_path=registry_path)
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="multi_stage_weight_trace_contract",
    )
    _print_indicator_artifact("Multi-stage weight trace contract", payload, paths)


@indicators_app.command("diagnose")
def indicator_diagnose_command(
    indicator_id: Annotated[str, typer.Option("--id", help="Indicator id。")],
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 mapping-free diagnostics，不生成 research weight。"""
    payload = _build_indicator_payload(
        lambda: build_indicator_diagnostics(
            indicator_id=indicator_id,
            registry_path=registry_path,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id=f"mapping_free_indicator_diagnostics_{indicator_id}",
    )
    _print_indicator_artifact("Indicator diagnostics", payload, paths)


@indicators_app.command("mapping-plan")
def indicator_mapping_plan_command(
    indicator_id: Annotated[str, typer.Option("--id", help="Indicator id。")],
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """生成 mapping candidate hypothesis cards，不运行 backfill。"""
    payload = _build_indicator_payload(
        lambda: build_mapping_plan(indicator_id=indicator_id, registry_path=registry_path)
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id=f"indicator_mapping_candidate_plan_{indicator_id}",
    )
    _print_indicator_artifact("Indicator mapping plan", payload, paths)


@indicators_app.command("masking")
def indicator_masking_command(
    indicator_id: Annotated[str, typer.Option("--id", help="Indicator id。")],
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """计算或声明 masking ratio / dominance audit。"""
    payload = _build_indicator_payload(
        lambda: build_masking_audit(
            indicator_id=indicator_id,
            registry_path=registry_path,
            trace_path=trace_path,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id=f"indicator_masking_and_dominance_audit_{indicator_id}",
    )
    _print_indicator_artifact("Indicator masking audit", payload, paths)


@indicators_app.command("gate")
def indicator_gate_command(
    indicator_id: Annotated[str, typer.Option("--id", help="Indicator id。")],
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 indicator-to-signal research gate。"""
    payload = _build_indicator_payload(
        lambda: build_indicator_research_gate(
            indicator_id=indicator_id,
            registry_path=registry_path,
            trace_path=trace_path,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id=f"indicator_to_signal_research_gate_{indicator_id}",
    )
    _print_indicator_artifact("Indicator research gate", payload, paths)


@indicators_app.command("valuation-crowding-pilot")
def valuation_crowding_pilot_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 valuation/crowding 高影响覆盖审计 pilot。"""
    payload = _build_indicator_payload(
        lambda: build_valuation_crowding_pilot_audit(
            registry_path=registry_path,
            trace_path=trace_path,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="valuation_crowding_pilot_audit",
    )
    _print_indicator_artifact("Valuation/crowding pilot", payload, paths)


@indicators_app.command("valuation-crowding-pilot-validation")
def valuation_crowding_pilot_validation_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 valuation/crowding high-impact pilot validation report。"""
    payload = _build_indicator_payload(
        lambda: build_valuation_crowding_pilot_validation_report(
            registry_path=registry_path,
            trace_path=trace_path,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="valuation_crowding_pilot_validation_report",
    )
    _print_indicator_artifact("Valuation/crowding pilot validation", payload, paths)


@indicators_app.command("masking-casebook")
def indicator_masking_casebook_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 prices_daily.csv，用于 forward outcome。"),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="casebook outcome 代理 ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 valuation/crowding -> trend masking casebook。"""
    payload = _build_indicator_payload(
        lambda: build_masking_casebook(
            registry_path=registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            outcome_ticker=outcome_ticker,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="indicator_masking_casebook_valuation_crowding_trend",
    )
    _print_indicator_artifact("Indicator masking casebook", payload, paths)


@indicators_app.command("ablation-validation")
def indicator_ablation_validation_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 prices_daily.csv，用于 forward outcome。"),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="ablation outcome 代理 ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: Annotated[
        float,
        typer.Option("--capped-masking-ratio", help="只读 capped masking 诊断上限。"),
    ] = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 valuation/crowding masking 只读 counterfactual/ablation validation。"""
    payload = _build_indicator_payload(
        lambda: build_valuation_crowding_ablation_validation(
            registry_path=registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            outcome_ticker=outcome_ticker,
            capped_masking_ratio=capped_masking_ratio,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="valuation_crowding_ablation_validation",
    )
    _print_indicator_artifact("Valuation/crowding ablation validation", payload, paths)


@indicators_app.command("masking-effectiveness-review")
def indicator_masking_effectiveness_review_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 prices_daily.csv，用于 forward outcome。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit 输出根目录。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option("--bridge-artifact-root", help="可选 backtest/simulation artifact 根目录。"),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="effectiveness outcome 代理 ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: Annotated[
        float,
        typer.Option("--capped-masking-ratio", help="只读 capped masking 诊断上限。"),
    ] = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 valuation/crowding masking effectiveness review。"""
    payload = _build_indicator_payload(
        lambda: build_valuation_crowding_masking_effectiveness_review(
            registry_path=registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            outcome_ticker=outcome_ticker,
            capped_masking_ratio=capped_masking_ratio,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="valuation_crowding_masking_effectiveness_review",
    )
    _print_indicator_artifact("Masking effectiveness review", payload, paths)


@indicators_app.command("masking-robustness-review")
def indicator_masking_robustness_review_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 prices_daily.csv，用于 forward outcome。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit 输出根目录。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option("--bridge-artifact-root", help="可选 backtest/simulation artifact 根目录。"),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="robustness outcome 代理 ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: Annotated[
        float,
        typer.Option("--capped-masking-ratio", help="只读 capped masking 诊断上限。"),
    ] = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 valuation/crowding masking robustness review。"""
    payload = _build_indicator_payload(
        lambda: build_valuation_crowding_masking_robustness_review(
            registry_path=registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            outcome_ticker=outcome_ticker,
            capped_masking_ratio=capped_masking_ratio,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="valuation_crowding_masking_robustness_review",
    )
    _print_indicator_artifact("Masking robustness review", payload, paths)


@indicators_app.command("validation-rollup")
def indicator_validation_rollup_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 prices_daily.csv，用于 forward outcome。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit 输出根目录。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option("--bridge-artifact-root", help="可选 backtest/simulation artifact 根目录。"),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="rollup outcome 代理 ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: Annotated[
        float,
        typer.Option("--capped-masking-ratio", help="只读 capped masking 诊断上限。"),
    ] = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 TRADING-665～691 indicator research validation rollup。"""
    payload = _build_indicator_payload(
        lambda: build_indicator_research_validation_rollup(
            registry_path=registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            outcome_ticker=outcome_ticker,
            capped_masking_ratio=capped_masking_ratio,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="indicator_research_validation_rollup",
    )
    _print_indicator_artifact("Indicator research validation rollup", payload, paths)


@indicators_app.command("long-horizon-floor-calibration-audit")
def indicator_long_horizon_floor_calibration_audit_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 prices_daily.csv，用于 forward outcome。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit 输出根目录。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option("--bridge-artifact-root", help="可选 backtest/simulation artifact 根目录。"),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="calibration audit outcome 代理 ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: Annotated[
        float,
        typer.Option("--capped-masking-ratio", help="只读 capped masking 诊断上限。"),
    ] = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 long-horizon evidence floor calibration audit。"""
    payload = _build_indicator_payload(
        lambda: build_long_horizon_evidence_floor_calibration_audit(
            registry_path=registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            outcome_ticker=outcome_ticker,
            capped_masking_ratio=capped_masking_ratio,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="long_horizon_evidence_floor_calibration_audit",
    )
    _print_indicator_artifact("Long-horizon floor calibration audit", payload, paths)


@indicators_app.command("outcome-availability-audit")
def indicator_outcome_availability_audit_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 prices_daily.csv，用于 realized outcome。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit 输出根目录。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option("--bridge-artifact-root", help="可选 backtest/simulation artifact 根目录。"),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="outcome 代理 ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: Annotated[
        float,
        typer.Option("--capped-masking-ratio", help="只读 capped masking 诊断上限。"),
    ] = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 valuation/crowding realized outcome availability audit。"""
    payload = _build_indicator_payload(
        lambda: build_valuation_crowding_outcome_availability_audit(
            registry_path=registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            outcome_ticker=outcome_ticker,
            capped_masking_ratio=capped_masking_ratio,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="valuation_crowding_outcome_availability_audit",
    )
    _print_indicator_artifact("Outcome availability audit", payload, paths)


@indicators_app.command("historical-trace-validation")
def indicator_historical_trace_validation_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 historical/replay multi-stage trace JSON。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit 输出根目录。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 historical replay/backtest multi-stage weight trace validation。"""
    payload = _build_indicator_payload(
        lambda: build_historical_multi_stage_weight_trace_validation(
            registry_path=registry_path,
            trace_path=trace_path,
            gate_audit_root=gate_audit_root,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="historical_multi_stage_weight_trace_validation",
    )
    _print_indicator_artifact("Historical trace validation", payload, paths)


@indicators_app.command("gate-availability-audit")
def indicator_gate_availability_audit_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 historical/replay multi-stage trace JSON。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="historical gate audit 输出根目录。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 historical trace gate availability audit。"""
    payload = _build_indicator_payload(
        lambda: build_gate_availability_audit(
            registry_path=registry_path,
            gate_audit_root=gate_audit_root,
            trace_path=trace_path,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="historical_trace_gate_availability_audit",
    )
    _print_indicator_artifact("Historical gate availability audit", payload, paths)


@indicators_app.command("lineage-manifest-repair")
def indicator_lineage_manifest_repair_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 historical/replay multi-stage trace JSON。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="historical gate audit 输出根目录。"),
    ] = None,
    root_cause_audit_path: Annotated[
        Path | None,
        typer.Option(
            "--root-cause-audit-path",
            help="可选修复前 gate availability audit JSON，用于锁定 affected artifacts。",
        ),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 historical replay lineage manifest repair report。"""
    payload = _build_indicator_payload(
        lambda: build_lineage_manifest_repair_report(
            registry_path=registry_path,
            trace_path=trace_path,
            gate_audit_root=gate_audit_root,
            root_cause_audit_path=root_cause_audit_path,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="lineage_manifest_repair_report",
    )
    _print_indicator_artifact("Lineage manifest repair", payload, paths)


@indicators_app.command("component-historical-trace")
def indicator_component_historical_trace_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 historical/replay multi-stage trace JSON。"),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit 输出根目录。"),
    ] = None,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """输出 component-level historical trace，供 masking diagnostic 使用。"""
    payload = _build_indicator_payload(
        lambda: build_component_level_historical_trace(
            registry_path=registry_path,
            trace_path=trace_path,
            gate_audit_root=gate_audit_root,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="component_level_historical_trace",
    )
    _print_indicator_artifact("Component-level historical trace", payload, paths)


@indicators_app.command("backtest-trace-bridge")
def indicator_backtest_trace_bridge_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 historical/replay multi-stage trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option("--prices-path", help="可选 prices_daily.csv，用于 forward outcome。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option("--bridge-artifact-root", help="可选 backtest/simulation artifact 根目录。"),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="bridge outcome 代理 ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """从 backtest/historical simulation/advisory outcome artifact 导出 trace bridge。"""
    payload = _build_indicator_payload(
        lambda: build_backtest_trace_bridge(
            registry_path=registry_path,
            trace_path=trace_path,
            prices_path=prices_path,
            bridge_artifact_root=bridge_artifact_root,
            outcome_ticker=outcome_ticker,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    paths = write_indicator_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="backtest_trace_bridge",
    )
    _print_indicator_artifact("Backtest trace bridge", payload, paths)


@indicators_app.command("validation-pack")
def indicator_validation_pack_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Annotated[
        Path,
        typer.Option("--threshold-registry", help="Threshold registry 路径。"),
    ] = DEFAULT_THRESHOLD_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option(
            "--prices-path",
            help="可选 prices_daily.csv，用于 casebook/ablation outcome。",
        ),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit 输出根目录。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option("--bridge-artifact-root", help="可选 backtest/simulation artifact 根目录。"),
    ] = None,
    coverage_extension_root: Annotated[
        Path | None,
        typer.Option(
            "--coverage-extension-root",
            help="可选 TRADING-699 coverage extension root，例如 outputs/research_campaigns。",
        ),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="casebook/ablation outcome 代理 ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: Annotated[
        float,
        typer.Option("--capped-masking-ratio", help="只读 capped masking 诊断上限。"),
    ] = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """写出 TRADING-665～685 indicator framework v1 validation pack。"""
    payload = _build_indicator_payload(
        lambda: write_indicator_framework_validation_pack(
            registry_path=registry_path,
            threshold_registry_path=threshold_registry_path,
            output_root=output_root,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            coverage_extension_root=coverage_extension_root,
            outcome_ticker=outcome_ticker,
            capped_masking_ratio=capped_masking_ratio,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    _print_status("Indicator validation pack", payload["status"])
    console.print(
        f"artifacts={len(payload['artifacts'])}；production_effect={payload['production_effect']}"
    )


@indicators_app.command("validation-pack-stability")
def indicator_validation_pack_stability_command(
    registry_path: Annotated[
        Path,
        typer.Option("--registry", help="Indicator research registry 路径。"),
    ] = DEFAULT_INDICATOR_REGISTRY_PATH,
    threshold_registry_path: Annotated[
        Path,
        typer.Option("--threshold-registry", help="Threshold registry 路径。"),
    ] = DEFAULT_THRESHOLD_REGISTRY_PATH,
    trace_path: Annotated[
        Path | None,
        typer.Option("--trace-path", help="可选 multi-stage weight trace JSON。"),
    ] = None,
    prices_path: Annotated[
        Path | None,
        typer.Option(
            "--prices-path",
            help="可选 prices_daily.csv，用于 casebook/ablation outcome。",
        ),
    ] = None,
    gate_audit_root: Annotated[
        Path | None,
        typer.Option("--gate-audit-root", help="可选 historical gate audit 输出根目录。"),
    ] = None,
    bridge_artifact_root: Annotated[
        Path | None,
        typer.Option("--bridge-artifact-root", help="可选 backtest/simulation artifact 根目录。"),
    ] = None,
    coverage_extension_root: Annotated[
        Path | None,
        typer.Option(
            "--coverage-extension-root",
            help="可选 TRADING-699 coverage extension root，例如 outputs/research_campaigns。",
        ),
    ] = None,
    outcome_ticker: Annotated[
        str,
        typer.Option("--outcome-ticker", help="casebook/ablation outcome 代理 ticker。"),
    ] = DEFAULT_MASKING_OUTCOME_TICKER,
    capped_masking_ratio: Annotated[
        float,
        typer.Option("--capped-masking-ratio", help="只读 capped masking 诊断上限。"),
    ] = DEFAULT_MASKING_ABLATION_CAP_RATIO,
    start_date: Annotated[
        str | None,
        typer.Option("--start-date", help="可选 historical trace 起始日期 YYYY-MM-DD。"),
    ] = None,
    end_date: Annotated[
        str | None,
        typer.Option("--end-date", help="可选 historical trace 结束日期 YYYY-MM-DD。"),
    ] = None,
    event_window_start: Annotated[
        str | None,
        typer.Option("--event-window-start", help="可选事件窗口起始日期 YYYY-MM-DD。"),
    ] = None,
    event_window_end: Annotated[
        str | None,
        typer.Option("--event-window-end", help="可选事件窗口结束日期 YYYY-MM-DD。"),
    ] = None,
    asset_universe: Annotated[
        str | None,
        typer.Option("--asset-universe", help="可选资产集合，逗号分隔。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Indicator research 输出目录。"),
    ] = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> None:
    """对 validation-pack 做两次 rerun 稳定性检查。"""
    payload = _build_indicator_payload(
        lambda: write_indicator_validation_pack_stability_report(
            registry_path=registry_path,
            threshold_registry_path=threshold_registry_path,
            output_root=output_root,
            trace_path=trace_path,
            prices_path=prices_path,
            gate_audit_root=gate_audit_root,
            bridge_artifact_root=bridge_artifact_root,
            coverage_extension_root=coverage_extension_root,
            outcome_ticker=outcome_ticker,
            capped_masking_ratio=capped_masking_ratio,
            start_date=start_date,
            end_date=end_date,
            event_window_start=event_window_start,
            event_window_end=event_window_end,
            asset_universe=asset_universe,
        )
    )
    _print_status("Indicator validation pack stability", payload["status"])
    console.print(
        f"stable={payload['summary']['stable']}；"
        f"artifact_count={payload['summary']['artifact_count']}；"
        f"production_effect={payload['production_effect']}"
    )


@campaign_app.command("init")
def init_campaign_command(
    spec: Annotated[Path, typer.Option("--spec", help="Campaign YAML/JSON spec 路径。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    module_registry_path: Annotated[
        Path,
        typer.Option("--module-registry", help="Module capability registry 路径。"),
    ] = DEFAULT_MODULE_REGISTRY_PATH,
    gate_policy_path: Annotated[
        Path,
        typer.Option("--gate-policy", help="Research gate policy 路径。"),
    ] = DEFAULT_GATE_POLICY_PATH,
    window_policy_path: Annotated[
        Path,
        typer.Option("--window-policy", help="Window/holdout policy 路径。"),
    ] = DEFAULT_WINDOW_POLICY_PATH,
    migration_path: Annotated[
        Path,
        typer.Option("--migration-config", help="历史 evidence 迁移配置路径。"),
    ] = DEFAULT_MIGRATION_PATH,
    force: Annotated[bool, typer.Option(help="覆盖已有 campaign 状态。")] = False,
) -> None:
    """创建 Campaign state、evidence store、transition audit 和 reproducibility manifest。"""
    try:
        payload = initialize_campaign(
            spec_path=spec,
            campaign_root=campaign_root,
            module_registry_path=module_registry_path,
            gate_policy_path=gate_policy_path,
            window_policy_path=window_policy_path,
            migration_path=migration_path,
            force=force,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    style = "green" if payload["validation_status"] != "FAIL" else "red"
    console.print(f"[{style}]Campaign 初始化：{payload['validation_status']}[/{style}]")
    console.print(f"Campaign：{payload['campaign_id']}")
    console.print(f"Stage：{payload['current_stage']}；Outcome：{payload['current_outcome']}")
    console.print(f"Evidence records：{payload['evidence_record_count']}")
    console.print(f"目录：{payload['campaign_dir']}")


@campaign_app.command("validate")
def validate_campaign_command(
    spec: Annotated[
        Path | None,
        typer.Option("--spec", help="Campaign YAML/JSON spec 路径。"),
    ] = None,
    campaign_id: Annotated[
        str | None,
        typer.Option("--id", help="已初始化 campaign id。"),
    ] = None,
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    module_registry_path: Annotated[
        Path,
        typer.Option("--module-registry", help="Module capability registry 路径。"),
    ] = DEFAULT_MODULE_REGISTRY_PATH,
    gate_policy_path: Annotated[
        Path,
        typer.Option("--gate-policy", help="Research gate policy 路径。"),
    ] = DEFAULT_GATE_POLICY_PATH,
    window_policy_path: Annotated[
        Path,
        typer.Option("--window-policy", help="Window/holdout policy 路径。"),
    ] = DEFAULT_WINDOW_POLICY_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """验证 Campaign spec、module boundary、holdout policy、gate policy 和 safety metadata。"""
    if bool(spec) == bool(campaign_id):
        raise typer.BadParameter("--spec 和 --id 必须且只能指定一个")
    try:
        if spec is not None:
            campaign_spec = load_campaign_spec(spec)
        else:
            campaign_spec, _, _ = load_campaign_bundle(campaign_id or "", campaign_root)
        payload = build_campaign_validation_payload(
            spec=campaign_spec,
            module_registry_path=module_registry_path,
            gate_policy_path=gate_policy_path,
            window_policy_path=window_policy_path,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign validation", payload["validation_status"])
    console.print(
        f"issues={payload['summary']['issue_count']}；"
        f"errors={payload['summary']['error_count']}；"
        f"warnings={payload['summary']['warning_count']}；"
        f"production_effect={payload['safety_boundary']['production_effect']}"
    )
    for issue in payload["issues"][:10]:
        console.print(f"{issue['severity']}: {issue['issue_id']}: {issue['message']}")
    if payload["validation_status"] == "FAIL":
        raise typer.Exit(code=1)


@campaign_app.command("validate-adapters")
def validate_campaign_adapters_command(
    adapter_registry_path: Annotated[
        Path,
        typer.Option("--adapter-registry", help="Campaign stage adapter registry 路径。"),
    ] = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
    module_registry_path: Annotated[
        Path,
        typer.Option("--module-registry", help="Module capability registry 路径。"),
    ] = DEFAULT_MODULE_REGISTRY_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """验证 Campaign stage adapter contract、输入 artifact 和 safety metadata。"""
    try:
        payload = validate_stage_adapter_contracts(
            adapter_registry_path=adapter_registry_path,
            module_registry_path=module_registry_path,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign adapter contract validation", payload["validation_status"])
    console.print(
        f"adapters={payload['adapter_count']}；"
        f"issues={len(payload['issues'])}；"
        f"production_effect={payload['production_effect']}"
    )
    for issue in payload["issues"][:10]:
        console.print(f"{issue['severity']}: {issue['issue_id']}: {issue['message']}")
    if payload["validation_status"] == "FAIL":
        raise typer.Exit(code=1)


@campaign_app.command("validation-pack")
def campaign_validation_pack_command(
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Campaign 输出 artifact 根目录。"),
    ] = DEFAULT_CAMPAIGN_OUTPUT_ROOT,
    adapter_registry_path: Annotated[
        Path,
        typer.Option("--adapter-registry", help="Campaign stage adapter registry 路径。"),
    ] = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """写出 Control Plane v1 rc5 adapter/parity/budget/next-action validation pack。"""
    try:
        payload = write_campaign_control_plane_v1_validation_artifacts(
            campaign_root=campaign_root,
            output_root=output_root,
            adapter_registry_path=adapter_registry_path,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign validation pack", payload["status"])
    console.print(
        f"artifacts={len(payload['artifacts'])}；production_effect={payload['production_effect']}"
    )


@campaign_app.command("plan")
def plan_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    gate_policy_path: Annotated[
        Path,
        typer.Option("--gate-policy", help="Research gate policy 路径。"),
    ] = DEFAULT_GATE_POLICY_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """输出当前 stage/outcome、预算使用、允许动作、阻断动作和推荐下一阶段。"""
    try:
        payload = campaign_plan(
            campaign_id=campaign_id,
            campaign_root=campaign_root,
            gate_policy_path=gate_policy_path,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_campaign_plan(payload)


@campaign_app.command("allowed-actions")
def allowed_actions_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """输出当前 Campaign 允许的下一步动作。"""
    try:
        payload = campaign_plan(campaign_id=campaign_id, campaign_root=campaign_root)
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    result = {
        "campaign_id": campaign_id,
        "allowed_next_actions": payload["allowed_next_actions"],
        "adapter_run_mode": payload["adapter_run_mode"],
        "production_effect": "none",
    }
    _write_json_if_requested(json_output_path, result)
    _print_action_list("Allowed actions", result["allowed_next_actions"])


@campaign_app.command("blocked-actions")
def blocked_actions_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """输出当前 Campaign 被阻断的动作。"""
    try:
        payload = campaign_plan(campaign_id=campaign_id, campaign_root=campaign_root)
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    result = {
        "campaign_id": campaign_id,
        "blocked_actions": payload["blocked_actions"],
        "required_owner_actions": payload["required_owner_actions"],
        "adapter_run_mode": payload["adapter_run_mode"],
        "production_effect": "none",
    }
    _write_json_if_requested(json_output_path, result)
    _print_action_list("Blocked actions", result["blocked_actions"])
    _print_action_list("Owner required", result["required_owner_actions"])


@campaign_app.command("budget")
def budget_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """输出 evidence budget used/remaining。"""
    try:
        payload = campaign_plan(campaign_id=campaign_id, campaign_root=campaign_root)
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    result = {
        "campaign_id": campaign_id,
        "budget_status": payload["budget_status"],
        "evidence_budget_used": payload["evidence_budget_used"],
        "evidence_budget_remaining": payload["evidence_budget_remaining"],
        "production_effect": "none",
    }
    _write_json_if_requested(json_output_path, result)
    _print_status("Campaign budget", result["budget_status"])
    console.print(f"used={result['evidence_budget_used']}")
    console.print(f"remaining={result['evidence_budget_remaining']}")


@campaign_app.command("source-artifacts")
def source_artifacts_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """输出 Campaign source artifact lineage。"""
    try:
        payload = build_status_payload(
            campaign_id=campaign_id,
            detailed=True,
            campaign_root=campaign_root,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    result = {
        "campaign_id": campaign_id,
        "source_artifacts": payload["source_artifacts"],
        "adapter_runtime": payload["adapter_runtime"],
        "production_effect": "none",
    }
    _write_json_if_requested(json_output_path, result)
    _print_action_list("Source artifacts", result["source_artifacts"])


@campaign_app.command("run")
def run_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    stage: Annotated[
        str,
        typer.Option("--stage", help="要运行的 stage，默认 next。"),
    ] = "next",
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    module_registry_path: Annotated[
        Path,
        typer.Option("--module-registry", help="Module capability registry 路径。"),
    ] = DEFAULT_MODULE_REGISTRY_PATH,
    gate_policy_path: Annotated[
        Path,
        typer.Option("--gate-policy", help="Research gate policy 路径。"),
    ] = DEFAULT_GATE_POLICY_PATH,
    window_policy_path: Annotated[
        Path,
        typer.Option("--window-policy", help="Window/holdout policy 路径。"),
    ] = DEFAULT_WINDOW_POLICY_PATH,
    adapter_registry_path: Annotated[
        Path,
        typer.Option("--adapter-registry", help="Campaign stage adapter registry 路径。"),
    ] = DEFAULT_STAGE_ADAPTER_REGISTRY_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Campaign 输出 artifact 根目录。"),
    ] = DEFAULT_CAMPAIGN_OUTPUT_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """按状态机运行允许的下一阶段；缺少计算 adapter 时 fail-closed。"""
    try:
        payload = run_campaign_stage(
            campaign_id=campaign_id,
            requested_stage=stage,
            campaign_root=campaign_root,
            module_registry_path=module_registry_path,
            gate_policy_path=gate_policy_path,
            window_policy_path=window_policy_path,
            adapter_registry_path=adapter_registry_path,
            output_root=output_root,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign stage run", payload["outcome"])
    console.print(f"run_id={payload['run_id']}；stage={payload['stage']}")
    console.print(
        f"adapter={payload.get('adapter_id') or 'none'}；status={payload.get('adapter_status')}"
    )
    if payload["outcome"] == "BLOCKED":
        raise typer.Exit(code=1)


@campaign_app.command("diagnose")
def diagnose_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    gate_policy_path: Annotated[
        Path,
        typer.Option("--gate-policy", help="Research gate policy 路径。"),
    ] = DEFAULT_GATE_POLICY_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """聚合 campaign-level evidence matrix，并披露 best/worst/missing evidence。"""
    try:
        payload = diagnose_campaign(
            campaign_id=campaign_id,
            campaign_root=campaign_root,
            gate_policy_path=gate_policy_path,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign diagnosis", payload["current_outcome"])
    console.print(
        f"positive={len(payload['positive_evidence'])}；"
        f"negative={len(payload['negative_evidence'])}；"
        f"missing={len(payload['missing_required_evidence_categories'])}"
    )


@campaign_app.command("gate")
def gate_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    gate_policy_path: Annotated[
        Path,
        typer.Option("--gate-policy", help="Research gate policy 路径。"),
    ] = DEFAULT_GATE_POLICY_PATH,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """用配置化 gate policy 计算当前 Campaign gate 结果。"""
    try:
        spec, state, evidence = load_campaign_bundle(campaign_id, campaign_root)
        payload = evaluate_gate(
            spec=spec,
            state=state,
            evidence=evidence,
            gate_policy_path=gate_policy_path,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign gate", payload["decision_outcome"])
    console.print(
        f"scorecard={payload['scorecard_policy']}；"
        f"missing={len(payload['missing_required_evidence_categories'])}；"
        f"blockers={len(payload['blocking_evidence_ids'])}"
    )


@campaign_app.command("status")
def status_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    view: Annotated[
        str,
        typer.Option("--view", help="concise 或 detailed。"),
    ] = "concise",
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """输出 canonical Campaign status。"""
    if view not in {"concise", "detailed"}:
        raise typer.BadParameter("--view must be concise or detailed")
    try:
        payload = build_status_payload(
            campaign_id=campaign_id,
            detailed=view == "detailed",
            campaign_root=campaign_root,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign status", payload["current_outcome"])
    console.print(f"stage={payload['current_stage']}；evidence={payload['evidence_record_count']}")
    console.print(f"budget={payload['budget_status']}；run_mode={payload['adapter_run_mode']}")
    console.print(f"allowed={', '.join(payload['allowed_next_actions']) or 'none'}")
    console.print(f"blocked={', '.join(payload['blocked_actions']) or 'none'}")


@campaign_app.command("packet")
def packet_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Owner packet 输出根目录。"),
    ] = DEFAULT_CAMPAIGN_OUTPUT_ROOT,
    gate_policy_path: Annotated[
        Path,
        typer.Option("--gate-policy", help="Research gate policy 路径。"),
    ] = DEFAULT_GATE_POLICY_PATH,
) -> None:
    """生成 Campaign Reader Brief / owner packet，不写 owner decision。"""
    try:
        payload = build_owner_packet(
            campaign_id=campaign_id,
            campaign_root=campaign_root,
            output_root=output_root,
            gate_policy_path=gate_policy_path,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _print_status("Campaign owner packet", payload["decision"])
    console.print(f"JSON：{payload['json_path']}")
    console.print(f"Markdown：{payload['markdown_path']}")
    console.print("owner_decision_appended=false；production_effect=none")


@campaign_app.command("archive")
def archive_campaign_command(
    campaign_id: Annotated[str, typer.Option("--id", help="Campaign id。")],
    reason: Annotated[
        str,
        typer.Option("--reason", help="Archive reason。"),
    ] = "manual_archive",
    campaign_root: Annotated[
        Path,
        typer.Option(help="Campaign 状态目录。"),
    ] = DEFAULT_CAMPAIGN_ROOT,
) -> None:
    """归档 Campaign，不写 owner decision、不触发 production effect。"""
    try:
        payload = archive_campaign(
            campaign_id=campaign_id,
            campaign_root=campaign_root,
            reason=reason,
        )
    except ResearchCampaignError as exc:
        raise typer.BadParameter(str(exc)) from exc
    _print_status("Campaign archive", payload["current_outcome"])
    console.print(f"stage={payload['current_stage']}；reason={payload['archive_reason']}")


@campaign_app.command("deprecation-plan")
def deprecation_plan_campaign_command(
    json_output_path: Annotated[
        Path | None,
        typer.Option("--json-output-path", help="可选 JSON 输出路径。"),
    ] = None,
) -> None:
    """输出旧 B2/B3 task-specific runner 的 Campaign 替代边界。"""
    payload = build_case_specific_runner_deprecation_plan()
    _write_json_if_requested(json_output_path, payload)
    _print_status("Campaign deprecation plan", payload["status"])
    for runner in payload["old_runners"]:
        console.print(
            f"{runner['old_command']} -> {runner['replacement_campaign_command']}；"
            f"parity={runner['parity_status']}；status={runner['deprecation_status']}"
        )


def _print_campaign_plan(payload: dict[str, object]) -> None:
    _print_status("Campaign plan", str(payload["current_outcome"]))
    console.print(f"stage={payload['current_stage']}；next={payload['next_recommended_stage']}")
    console.print(f"budget={payload['budget_status']}")
    console.print(
        f"adapter={payload.get('adapter_id') or 'none'}；"
        f"run_mode={payload.get('adapter_run_mode') or 'none'}"
    )
    console.print(f"allowed={', '.join(payload['allowed_next_actions']) or 'none'}")
    console.print(f"blocked={', '.join(payload['blocked_actions']) or 'none'}")
    console.print(f"owner_required={', '.join(payload['required_owner_actions']) or 'none'}")


def _print_status(label: str, status: str) -> None:
    style = "green" if status in {"PASS", "PROMISING"} else "yellow"
    if status in {"FAIL", "BLOCKED", "REJECTED"}:
        style = "red"
    console.print(f"[{style}]{label}：{status}[/{style}]")


def _write_json_if_requested(path: Path | None, payload: dict[str, object]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _build_indicator_payload(builder):  # type: ignore[no-untyped-def]
    try:
        return builder()
    except IndicatorResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc


def _print_indicator_artifact(
    label: str,
    payload: dict[str, object],
    paths: dict[str, str],
) -> None:
    _print_status(label, str(payload["status"]))
    summary = payload.get("summary")
    if isinstance(summary, dict):
        compact = "; ".join(f"{key}={value}" for key, value in list(summary.items())[:4])
        if compact:
            console.print(compact)
    console.print(f"JSON：{paths['json_path']}")
    console.print(f"Markdown：{paths['markdown_path']}")
    console.print("research_only=true；production_effect=none")
    if str(payload["status"]) == "FAIL" or str(payload["status"]).endswith("_BLOCKED"):
        raise typer.Exit(code=1)


def _print_action_list(label: str, values: object) -> None:
    console.print(f"{label}:")
    if not values:
        console.print("- none")
        return
    for value in values:
        console.print(f"- {value}")
