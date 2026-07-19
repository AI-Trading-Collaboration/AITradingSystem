from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from ai_trading_system.etf_portfolio.data import load_standard_prices
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_PRICE_PATH,
    load_etf_config_bundle,
)
from ai_trading_system.etf_portfolio.weight_calibration import (
    DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH,
    DEFAULT_ETF_WEIGHT_CALIBRATION_DATA_DIR,
    DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH,
    DEFAULT_WEIGHT_CALIBRATION_PRESET_CONFIG_PATH,
    DEFAULT_WEIGHT_CALIBRATION_VALIDATION_DIR,
    DEFAULT_WEIGHT_CANDIDATE_COMPARISON_DIR,
    DEFAULT_WEIGHT_DUAL_TRACK_REPORT_DIR,
    DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH,
    DEFAULT_WEIGHT_FORWARD_EVIDENCE_DIR,
    DEFAULT_WEIGHT_INITIAL_RECOMMENDATION_DIR,
    DEFAULT_WEIGHT_OVERFIT_DIAGNOSTICS_DIR,
    DEFAULT_WEIGHT_OVERFIT_EXPLANATION_DIR,
    DEFAULT_WEIGHT_PROPOSAL_DIR,
    DEFAULT_WEIGHT_REGIME_ROBUSTNESS_DIR,
    DEFAULT_WEIGHT_SEARCH_DIAGNOSTICS_DIR,
    DEFAULT_WEIGHT_TOP_CANDIDATE_EXPORT_DIR,
    WEIGHT_ROBUST_SEARCH_PACK_IDS,
    WEIGHT_SEARCH_DIAGNOSTICS_DEFAULT_PRESETS,
    build_backtest_forward_evidence_aggregation,
    build_candidate_weight_proposals,
    build_dual_track_weight_calibration_report,
    build_dual_track_weight_calibration_validation_report,
    build_historical_weight_calibration_usability_validation_report,
    build_historical_weight_search_diagnostics_report,
    build_weight_candidate_comparison_table,
    build_weight_initial_recommendation_report,
    build_weight_overfit_diagnostics,
    build_weight_overfit_explanations,
    build_weight_regime_robustness_heatmap,
    build_weight_top_candidate_export,
    enroll_candidate_weights_forward,
    enroll_top_weight_candidates_forward,
    find_latest_weight_search_run_dir,
    load_candidate_weight_registry,
    load_weight_calibration_preset,
    load_weight_calibration_preset_registry,
    load_weight_forward_enrollments,
    load_weight_search_definition,
    load_weight_search_registry,
    read_weight_search_run_payload,
    register_candidate_weight_sets,
    resolve_weight_calibration_preset,
    run_historical_weight_search,
    write_backtest_forward_evidence_aggregation,
    write_candidate_weight_proposals,
    write_dual_track_weight_calibration_report,
    write_dual_track_weight_calibration_validation_report,
    write_historical_weight_calibration_usability_validation_report,
    write_historical_weight_search_diagnostics_report,
    write_weight_candidate_comparison_table,
    write_weight_initial_recommendation_report,
    write_weight_overfit_diagnostics,
    write_weight_overfit_explanations,
    write_weight_regime_robustness_heatmap,
    write_weight_search_run,
    write_weight_top_candidate_export,
)
from ai_trading_system.etf_portfolio.weight_calibration_cache import (
    DEFAULT_WEIGHT_CALIBRATION_CACHE_POLICY_CONFIG_PATH,
    DEFAULT_WEIGHT_CALIBRATION_CACHE_VALIDATION_DIR,
    DEFAULT_WEIGHT_CALIBRATION_PERFORMANCE_REPORT_DIR,
    build_weight_calibration_cache_parallel_validation_report,
    load_weight_calibration_cache_policy_config,
    write_weight_calibration_cache_parallel_validation_report,
    write_weight_calibration_performance_report,
)
from ai_trading_system.etf_portfolio.weight_calibration_profiling import (
    DEFAULT_WEIGHT_CALIBRATION_PROFILING_POLICY_CONFIG_PATH,
    DEFAULT_WEIGHT_CALIBRATION_PROFILING_REPORT_DIR,
    DEFAULT_WEIGHT_CALIBRATION_PROFILING_VALIDATION_DIR,
    build_weight_calibration_profiling_report,
    build_weight_calibration_profiling_validation_report,
    load_weight_calibration_profiling_policy_config,
    normalize_weight_calibration_profile_mode,
    profiling_mode_settings,
    run_with_optional_cprofile,
    write_cprofile_artifacts,
    write_weight_calibration_candidate_hotspot_table,
    write_weight_calibration_profiling_report,
    write_weight_calibration_profiling_validation_report,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    latest_json_file as _latest_json_file,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    load_optional_json_payload as _load_optional_json_payload,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date as _parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.registration import weight_calibration_app
from ai_trading_system.reports.report_index import DEFAULT_REPORT_REGISTRY_PATH


def _echo_weight_shadow_enrollment_summary(
    enrollment_path: Path,
    enrollment: Mapping[str, object],
) -> None:
    latest_selection = enrollment.get("latest_selection")
    latest = latest_selection if isinstance(latest_selection, Mapping) else {}
    raw_results = latest.get("enrollment_results")
    results = (
        [dict(item) for item in raw_results if isinstance(item, Mapping)]
        if isinstance(raw_results, list)
        else []
    )
    typer.echo(f"ETF weight candidate shadow enrollment：{enrollment_path}")
    typer.echo(f"enrollment_count={enrollment['enrollment_count']}")
    typer.echo(f"selected_weight_set_count={len(latest.get('weight_set_ids') or [])}")
    for result in results:
        typer.echo("enrollment_result=" + json.dumps(result, ensure_ascii=False, sort_keys=True))
    typer.echo("shared_shadow_registry_mutated=false")
    typer.echo("production_weights_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("validate-config")
def weight_calibration_validate_config_command(
    search: Annotated[
        str,
        typer.Option("--search", "--config", help="weight search id。"),
    ] = "etf_initial_weight_search_v1",
    config_path: Annotated[
        Path,
        typer.Option(help="weight search config path。"),
    ] = DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH,
) -> None:
    """校验 TRADING-071A historical weight search config。"""
    registry = load_weight_search_registry(config_path)
    definition = load_weight_search_definition(search, config_path)
    objective = registry.objective_policies[definition.objective_policy]
    benchmark_set = registry.benchmark_sets[definition.benchmark_set]
    typer.echo("ETF weight calibration config 校验通过。")
    typer.echo(f"search_id={definition.search_id}")
    typer.echo(f"config_hash={registry.config_hash}")
    typer.echo(f"universe={','.join(definition.universe)}")
    typer.echo(f"grid_step={definition.grid_step:.4f}")
    typer.echo(f"max_candidate_count={definition.max_candidate_count}")
    typer.echo(f"objective_policy={definition.objective_policy}")
    typer.echo(f"objective_policy_status={objective.policy_status}")
    typer.echo(f"benchmark_set={definition.benchmark_set}")
    typer.echo(f"benchmark_ids={','.join(benchmark_set.benchmark_ids)}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("search")
def weight_calibration_search_command(
    search: Annotated[
        str,
        typer.Option("--search", "--config", help="weight search id。"),
    ] = "etf_initial_weight_search_v1",
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="weight search config YAML path。"),
    ] = DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH,
    preset: Annotated[
        str | None,
        typer.Option("--preset", help="historical range preset id。"),
    ] = None,
    preset_config_path: Annotated[
        Path,
        typer.Option("--preset-config-path", help="historical range preset config YAML path。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_PRESET_CONFIG_PATH,
    prices_path: Annotated[Path, typer.Option(help="ETF 标准价格缓存路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    start: Annotated[
        str | None,
        typer.Option("--start", help="historical search start YYYY-MM-DD。"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="historical search end YYYY-MM-DD。"),
    ] = None,
    max_candidates: Annotated[
        int | None,
        typer.Option("--max-candidates", help="lower-than-config candidate evaluation cap。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="weight calibration report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    data_output_dir: Annotated[
        Path,
        typer.Option(help="weight calibration runtime data 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_DATA_DIR,
) -> None:
    """执行 TRADING-071B bounded historical ETF weight search。"""
    if preset is not None and (start is not None or end is not None):
        raise typer.BadParameter("--preset cannot be combined with --start or --end")
    config = load_etf_config_bundle()
    registry = load_weight_search_registry(config_path, etf_config=config)
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 weight search。")
        raise typer.Exit(code=1)
    run_start = _parse_date(start) if start else None
    run_end = _parse_date(end) if end else None
    preset_context = None
    if preset is not None:
        historical_preset = load_weight_calibration_preset(
            preset,
            preset_config_path,
            etf_config=config,
            weight_search_registry=registry,
        )
        available_dates = pd.to_datetime(prices["date"], errors="coerce").dropna()
        if available_dates.empty:
            raise typer.BadParameter("prices_path has no valid date values")
        preset_context = resolve_weight_calibration_preset(
            historical_preset,
            available_start=available_dates.min().date(),
            available_end=available_dates.max().date(),
        )
        run_start = preset_context["start_date"]
        run_end = preset_context["end_date"]
    run = run_historical_weight_search(
        prices,
        etf_config=config,
        quality_report=quality_report,
        registry=registry,
        search_id=search,
        start=run_start,
        end=run_end,
        range_preset=preset_context,
        max_candidates=max_candidates,
    )
    paths = write_weight_search_run(
        run,
        report_root=output_dir,
        data_root=data_output_dir,
    )
    generation = run.payload["candidate_generation"]
    typer.echo(f"ETF weight calibration search 完成：{run.run_id}")
    typer.echo(f"report={paths['summary_md']}")
    typer.echo(f"data_dir={paths['data_dir']}")
    typer.echo(f"evaluated_candidate_count={generation['evaluated_candidate_count']}")
    typer.echo(f"total_valid_candidate_count={generation['total_valid_candidate_count']}")
    typer.echo(f"blocked_candidate_count={len(run.payload['blocked_candidates'])}")
    typer.echo(f"data_quality_status={run.payload['data_quality_status']}")
    if preset_context is not None:
        typer.echo(f"preset_id={preset_context['preset_id']}")
        typer.echo(
            "resolved_date_range="
            f"{preset_context['start_date'].isoformat()}:{preset_context['end_date'].isoformat()}"
        )
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("register-candidates")
def weight_calibration_register_candidates_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="historical weight search run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 historical weight search run。"),
    ] = False,
    top: Annotated[
        int | None,
        typer.Option("--top", help="登记 ranking 前 N 个 candidates。"),
    ] = None,
    weight_set: Annotated[
        list[str] | None,
        typer.Option("--weight-set", help="candidate_id 或 weight_set_id，可重复。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="weight calibration report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    registry_path: Annotated[
        Path,
        typer.Option(help="candidate initial weight set registry path。"),
    ] = DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH,
) -> None:
    """登记 TRADING-071D candidate-only initial weight sets。"""
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    if weight_set and top is not None:
        raise typer.BadParameter("--weight-set cannot be combined with --top")
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    run_dir = find_latest_weight_search_run_dir(output_dir) if latest else output_dir / str(run_id)
    payload = read_weight_search_run_payload(run_dir)
    registry = register_candidate_weight_sets(
        payload,
        registry_path=registry_path,
        top=None if weight_set else (top or 3),
        weight_set_ids=weight_set,
    )
    typer.echo(f"ETF candidate weight registry：{registry_path}")
    typer.echo(f"source_search_run_id={payload['search_run_id']}")
    typer.echo(f"candidate_count={registry['candidate_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("export-top")
def weight_calibration_export_top_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="historical weight search run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 historical weight search run。"),
    ] = False,
    top: Annotated[
        int,
        typer.Option("--top", help="导出 ranking 前 N 个 candidates。"),
    ] = 10,
    search_output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight calibration search report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    overfit_path: Annotated[
        Path | None,
        typer.Option(help="optional overfit diagnostics JSON path。"),
    ] = None,
    export_dir: Annotated[
        Path,
        typer.Option("--export-dir", help="Top-N candidate export 输出目录。"),
    ] = DEFAULT_WEIGHT_TOP_CANDIDATE_EXPORT_DIR,
) -> None:
    """导出 TRADING-078B Top-N historical candidate weight sets。"""
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    run_dir = (
        find_latest_weight_search_run_dir(search_output_dir)
        if latest
        else search_output_dir / str(run_id)
    )
    search_payload = read_weight_search_run_payload(run_dir)
    source_paths = {"historical_search": str(run_dir / "summary.json")}
    if overfit_path is not None:
        source_paths["overfit_diagnostics"] = str(overfit_path)
    payload = build_weight_top_candidate_export(
        search_payload,
        top=top,
        overfit_payload=_load_optional_json_payload(overfit_path),
        source_paths=source_paths,
    )
    paths = write_weight_top_candidate_export(payload, output_dir=export_dir)
    typer.echo(f"ETF weight top-N candidate export：{paths['markdown']}")
    typer.echo(f"json={paths['json']}")
    typer.echo(f"csv={paths['csv']}")
    typer.echo(f"exported_candidate_count={payload['exported_candidate_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("comparison")
def weight_calibration_comparison_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="historical weight search run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 historical weight search run。"),
    ] = False,
    top: Annotated[
        int,
        typer.Option("--top", help="包含 ranking 前 N 个 candidates。"),
    ] = 10,
    search_output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight calibration search report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    top_export_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078B Top-N JSON path。"),
    ] = None,
    comparison_dir: Annotated[
        Path,
        typer.Option("--comparison-dir", help="candidate comparison table 输出目录。"),
    ] = DEFAULT_WEIGHT_CANDIDATE_COMPARISON_DIR,
) -> None:
    """生成 TRADING-078C candidate weights and benchmarks comparison table。"""
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    run_dir = (
        find_latest_weight_search_run_dir(search_output_dir)
        if latest
        else search_output_dir / str(run_id)
    )
    search_payload = read_weight_search_run_payload(run_dir)
    source_paths = {"historical_search": str(run_dir / "summary.json")}
    if top_export_path is not None:
        source_paths["top_candidate_export"] = str(top_export_path)
    payload = build_weight_candidate_comparison_table(
        search_payload,
        top_export_payload=_load_optional_json_payload(top_export_path),
        top=top,
        source_paths=source_paths,
    )
    paths = write_weight_candidate_comparison_table(payload, output_dir=comparison_dir)
    typer.echo(f"ETF weight candidate comparison table：{paths['markdown']}")
    typer.echo(f"json={paths['json']}")
    typer.echo(f"csv={paths['csv']}")
    typer.echo(f"row_count={payload['row_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("regime-robustness")
def weight_calibration_regime_robustness_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="historical weight search run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 historical weight search run。"),
    ] = False,
    top: Annotated[
        int,
        typer.Option("--top", help="包含 ranking 前 N 个 candidates。"),
    ] = 10,
    search_output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight calibration search report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    top_export_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078B Top-N JSON path。"),
    ] = None,
    heatmap_dir: Annotated[
        Path,
        typer.Option("--heatmap-dir", help="regime robustness heatmap 输出目录。"),
    ] = DEFAULT_WEIGHT_REGIME_ROBUSTNESS_DIR,
) -> None:
    """生成 TRADING-078D regime robustness heatmap-ready data。"""
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    run_dir = (
        find_latest_weight_search_run_dir(search_output_dir)
        if latest
        else search_output_dir / str(run_id)
    )
    search_payload = read_weight_search_run_payload(run_dir)
    source_paths = {"historical_search": str(run_dir / "summary.json")}
    if top_export_path is not None:
        source_paths["top_candidate_export"] = str(top_export_path)
    payload = build_weight_regime_robustness_heatmap(
        search_payload,
        top_export_payload=_load_optional_json_payload(top_export_path),
        top=top,
        source_paths=source_paths,
    )
    paths = write_weight_regime_robustness_heatmap(payload, output_dir=heatmap_dir)
    typer.echo(f"ETF weight regime robustness heatmap：{paths['markdown']}")
    typer.echo(f"json={paths['json']}")
    typer.echo(f"csv={paths['csv']}")
    typer.echo(f"matrix_row_count={payload['matrix_row_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("enroll-forward")
def weight_calibration_enroll_forward_command(
    latest: Annotated[
        bool,
        typer.Option(
            "--latest",
            help="使用当前 candidate weight registry；未指定 --top 时默认登记前三名。",
        ),
    ] = False,
    top: Annotated[
        int | None,
        typer.Option("--top", help="按 registry rank 登记前 N 个 candidates。"),
    ] = None,
    weight_set: Annotated[
        list[str] | None,
        typer.Option("--weight-set", help="weight_set_id 或 source_candidate_id，可重复。"),
    ] = None,
    registry_path: Annotated[
        Path,
        typer.Option(help="candidate initial weight set registry path。"),
    ] = DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH,
    enrollment_path: Annotated[
        Path,
        typer.Option(help="dual-track forward enrollment registry path。"),
    ] = DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH,
) -> None:
    """登记 TRADING-071E dual-track forward observation candidates。"""
    if weight_set and top is not None:
        raise typer.BadParameter("--weight-set cannot be combined with --top")
    if not weight_set and top is None and not latest:
        raise typer.BadParameter("--weight-set or --latest/--top is required")
    registry = load_candidate_weight_registry(registry_path)
    enrollment = enroll_candidate_weights_forward(
        registry,
        enrollment_path=enrollment_path,
        top=None if weight_set else (top or 3),
        weight_set_ids=weight_set,
    )
    latest_selection = enrollment.get("latest_selection", {})
    typer.echo(f"ETF weight calibration forward enrollment：{enrollment_path}")
    typer.echo(f"enrollment_count={enrollment['enrollment_count']}")
    typer.echo(f"selected_weight_set_count={len(latest_selection.get('weight_set_ids') or [])}")
    typer.echo("shared_shadow_registry_mutated=false")
    typer.echo("production_weights_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("enroll-top")
def weight_calibration_enroll_top_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="historical weight search run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 historical weight search run。"),
    ] = False,
    top: Annotated[
        int,
        typer.Option("--top", help="enroll Top-N shadow_ready candidates。"),
    ] = 3,
    search_output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight calibration search report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    top_export_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078B Top-N JSON path。"),
    ] = None,
    comparison_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078C comparison JSON path，保留为 source link。"),
    ] = None,
    enrollment_path: Annotated[
        Path,
        typer.Option(help="dual-track forward enrollment registry path。"),
    ] = DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH,
) -> None:
    """从 TRADING-078B Top-N shortlist 登记 shadow-ready weight candidates。"""
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    run_dir = (
        find_latest_weight_search_run_dir(search_output_dir)
        if latest
        else search_output_dir / str(run_id)
    )
    search_payload = read_weight_search_run_payload(run_dir)
    source_paths = {"historical_search": str(run_dir / "summary.json")}
    if top_export_path is not None:
        source_paths["top_candidate_export"] = str(top_export_path)
    if comparison_path is not None:
        source_paths["comparison_table"] = str(comparison_path)
    enrollment = enroll_top_weight_candidates_forward(
        search_payload,
        top_export_payload=_load_optional_json_payload(top_export_path),
        comparison_payload=_load_optional_json_payload(comparison_path),
        source_paths=source_paths,
        enrollment_path=enrollment_path,
        top=top,
    )
    _echo_weight_shadow_enrollment_summary(enrollment_path, enrollment)


@weight_calibration_app.command("enroll")
def weight_calibration_enroll_command(
    weight_set: Annotated[
        list[str],
        typer.Option("--weight-set", help="weight_set_id 或 source_candidate_id，可重复。"),
    ],
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="historical weight search run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 historical weight search run。"),
    ] = False,
    search_output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight calibration search report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    top_export_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078B Top-N JSON path。"),
    ] = None,
    comparison_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078C comparison JSON path，保留为 source link。"),
    ] = None,
    enrollment_path: Annotated[
        Path,
        typer.Option(help="dual-track forward enrollment registry path。"),
    ] = DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH,
) -> None:
    """按 weight_set_id 登记单个或多个 shadow-ready weight candidates。"""
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    if not weight_set:
        raise typer.BadParameter("--weight-set is required")
    run_dir = (
        find_latest_weight_search_run_dir(search_output_dir)
        if latest
        else search_output_dir / str(run_id)
    )
    search_payload = read_weight_search_run_payload(run_dir)
    source_paths = {"historical_search": str(run_dir / "summary.json")}
    if top_export_path is not None:
        source_paths["top_candidate_export"] = str(top_export_path)
    if comparison_path is not None:
        source_paths["comparison_table"] = str(comparison_path)
    enrollment = enroll_top_weight_candidates_forward(
        search_payload,
        top_export_payload=_load_optional_json_payload(top_export_path),
        comparison_payload=_load_optional_json_payload(comparison_path),
        source_paths=source_paths,
        enrollment_path=enrollment_path,
        weight_set_ids=weight_set,
    )
    _echo_weight_shadow_enrollment_summary(enrollment_path, enrollment)


@weight_calibration_app.command("aggregate-evidence")
def weight_calibration_aggregate_evidence_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="evidence aggregation date YYYY-MM-DD。"),
    ] = None,
    latest_search: Annotated[
        bool,
        typer.Option("--latest-search", help="读取最新 historical weight search run。"),
    ] = False,
    search_run_id: Annotated[
        str | None,
        typer.Option("--search-run-id", help="historical weight search run id。"),
    ] = None,
    search_output_dir: Annotated[
        Path,
        typer.Option(help="weight calibration search report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    candidate_registry_path: Annotated[
        Path,
        typer.Option(help="candidate initial weight set registry path。"),
    ] = DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH,
    enrollment_path: Annotated[
        Path,
        typer.Option(help="dual-track forward enrollment registry path。"),
    ] = DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH,
    forward_dashboard_path: Annotated[
        Path | None,
        typer.Option(help="optional ETF forward dashboard JSON path。"),
    ] = None,
    weekly_review_path: Annotated[
        Path | None,
        typer.Option(help="optional ETF weekly review JSON path。"),
    ] = None,
    decision_journal_path: Annotated[
        Path | None,
        typer.Option(help="optional ETF decision journal report JSON path。"),
    ] = None,
    parameter_review_path: Annotated[
        Path | None,
        typer.Option(help="optional ETF parameter review evidence/report JSON path。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="backtest-vs-forward evidence 输出目录。"),
    ] = DEFAULT_WEIGHT_FORWARD_EVIDENCE_DIR,
) -> None:
    """聚合 TRADING-071F backtest expectation vs forward evidence。"""
    if search_run_id is not None and latest_search:
        raise typer.BadParameter("--search-run-id and --latest-search cannot be combined")
    run_date = _parse_date(as_of) if as_of else date.today()
    search_payload = None
    source_paths: dict[str, str] = {
        "candidate_registry": str(candidate_registry_path),
        "forward_enrollment": str(enrollment_path),
    }
    if search_run_id is not None or latest_search:
        run_dir = (
            find_latest_weight_search_run_dir(search_output_dir)
            if latest_search
            else search_output_dir / str(search_run_id)
        )
        search_payload = read_weight_search_run_payload(run_dir)
        source_paths["historical_search"] = str(run_dir / "summary.json")
    for key, path in {
        "forward_dashboard": forward_dashboard_path,
        "weekly_review": weekly_review_path,
        "decision_journal": decision_journal_path,
        "parameter_review": parameter_review_path,
    }.items():
        if path is not None:
            source_paths[key] = str(path)
    payload = build_backtest_forward_evidence_aggregation(
        as_of=run_date,
        candidate_registry=load_candidate_weight_registry(candidate_registry_path),
        forward_enrollments=load_weight_forward_enrollments(enrollment_path),
        search_payload=search_payload,
        forward_dashboard=_load_optional_json_payload(forward_dashboard_path),
        weekly_review=_load_optional_json_payload(weekly_review_path),
        decision_journal=_load_optional_json_payload(decision_journal_path),
        parameter_review=_load_optional_json_payload(parameter_review_path),
        source_paths=source_paths,
    )
    paths = write_backtest_forward_evidence_aggregation(payload, output_dir=output_dir)
    typer.echo(f"ETF weight backtest-forward evidence：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"evidence_record_count={payload['evidence_record_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("overfit-diagnostics")
def weight_calibration_overfit_diagnostics_command(
    latest_search: Annotated[
        bool,
        typer.Option("--latest-search", help="读取最新 historical weight search run。"),
    ] = False,
    search_run_id: Annotated[
        str | None,
        typer.Option("--search-run-id", help="historical weight search run id。"),
    ] = None,
    search_output_dir: Annotated[
        Path,
        typer.Option(help="weight calibration search report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    candidate_registry_path: Annotated[
        Path,
        typer.Option(help="candidate initial weight set registry path。"),
    ] = DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH,
    evidence_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-071F evidence JSON path。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="overfit diagnostics 输出目录。"),
    ] = DEFAULT_WEIGHT_OVERFIT_DIAGNOSTICS_DIR,
) -> None:
    """生成 TRADING-071G overfit risk and stability diagnostics。"""
    if search_run_id is not None and latest_search:
        raise typer.BadParameter("--search-run-id and --latest-search cannot be combined")
    search_payload = None
    if search_run_id is not None or latest_search:
        run_dir = (
            find_latest_weight_search_run_dir(search_output_dir)
            if latest_search
            else search_output_dir / str(search_run_id)
        )
        search_payload = read_weight_search_run_payload(run_dir)
    payload = build_weight_overfit_diagnostics(
        candidate_registry=load_candidate_weight_registry(candidate_registry_path),
        search_payload=search_payload,
        evidence_payload=_load_optional_json_payload(evidence_path),
    )
    paths = write_weight_overfit_diagnostics(payload, output_dir=output_dir)
    typer.echo(f"ETF weight overfit diagnostics：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['candidate_count']}")
    typer.echo(f"risk_counts={payload['risk_counts']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("overfit-explain")
def weight_calibration_overfit_explain_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="historical weight search run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 historical weight search run。"),
    ] = False,
    top: Annotated[
        int,
        typer.Option("--top", help="解释 ranking 前 N 个 candidates。"),
    ] = 10,
    search_output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight calibration search report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    top_export_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078B Top-N JSON path。"),
    ] = None,
    overfit_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-071G overfit diagnostics JSON path。"),
    ] = None,
    explanation_dir: Annotated[
        Path,
        typer.Option("--explanation-dir", help="overfit explanation 输出目录。"),
    ] = DEFAULT_WEIGHT_OVERFIT_EXPLANATION_DIR,
) -> None:
    """生成 TRADING-078E human-readable overfit risk explanation。"""
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    run_dir = (
        find_latest_weight_search_run_dir(search_output_dir)
        if latest
        else search_output_dir / str(run_id)
    )
    search_payload = read_weight_search_run_payload(run_dir)
    source_paths = {"historical_search": str(run_dir / "summary.json")}
    if top_export_path is not None:
        source_paths["top_candidate_export"] = str(top_export_path)
    if overfit_path is not None:
        source_paths["overfit_diagnostics"] = str(overfit_path)
    payload = build_weight_overfit_explanations(
        search_payload,
        top_export_payload=_load_optional_json_payload(top_export_path),
        overfit_payload=_load_optional_json_payload(overfit_path),
        top=top,
        source_paths=source_paths,
    )
    paths = write_weight_overfit_explanations(payload, output_dir=explanation_dir)
    typer.echo(f"ETF weight overfit explanation：{paths['markdown']}")
    typer.echo(f"json={paths['json']}")
    typer.echo(f"candidate_count={payload['candidate_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("recommendation")
def weight_calibration_recommendation_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="historical weight search run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 historical weight search run。"),
    ] = False,
    top: Annotated[
        int,
        typer.Option("--top", help="报告包含 ranking 前 N 个 candidates。"),
    ] = 10,
    search_output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="weight calibration search report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    top_export_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078B Top-N JSON path。"),
    ] = None,
    comparison_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078C comparison JSON path。"),
    ] = None,
    regime_robustness_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078D regime robustness JSON path。"),
    ] = None,
    overfit_explanation_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078E overfit explanation JSON path。"),
    ] = None,
    enrollment_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078F forward enrollment registry path。"),
    ] = None,
    report_dir: Annotated[
        Path,
        typer.Option("--report-dir", help="initial recommendation report 输出目录。"),
    ] = DEFAULT_WEIGHT_INITIAL_RECOMMENDATION_DIR,
) -> None:
    """生成 TRADING-078G initial ETF weight candidate recommendation report。"""
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    run_dir = (
        find_latest_weight_search_run_dir(search_output_dir)
        if latest
        else search_output_dir / str(run_id)
    )
    search_payload = read_weight_search_run_payload(run_dir)
    source_paths = {"historical_search": str(run_dir / "summary.json")}
    optional_sources = {
        "top_candidate_export": top_export_path,
        "comparison_table": comparison_path,
        "regime_robustness": regime_robustness_path,
        "overfit_explanation": overfit_explanation_path,
        "forward_enrollment": enrollment_path,
    }
    for key, path in optional_sources.items():
        if path is not None:
            source_paths[key] = str(path)
    payload = build_weight_initial_recommendation_report(
        search_payload,
        top_export_payload=_load_optional_json_payload(top_export_path),
        comparison_payload=_load_optional_json_payload(comparison_path),
        regime_robustness_payload=_load_optional_json_payload(regime_robustness_path),
        overfit_explanation_payload=_load_optional_json_payload(overfit_explanation_path),
        enrollment_payload=_load_optional_json_payload(enrollment_path),
        top=top,
        source_paths=source_paths,
    )
    paths = write_weight_initial_recommendation_report(payload, output_dir=report_dir)
    shadow = payload["shadow_enrollment_recommendations"]
    typer.echo(f"ETF initial weight recommendation report：{paths['markdown']}")
    typer.echo(f"json={paths['json']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"suggested_action={shadow['suggested_action']}")
    typer.echo(f"recommended_weight_set_ids={shadow['recommended_weight_set_ids']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("diagnostics")
def weight_calibration_diagnostics_command(
    search: Annotated[
        list[str] | None,
        typer.Option("--search", help="weight search id，可重复。"),
    ] = None,
    include_robust_packs: Annotated[
        bool,
        typer.Option(
            "--include-robust-packs",
            help="同时运行 TRADING-079 bounded robust search packs。",
        ),
    ] = False,
    preset: Annotated[
        list[str] | None,
        typer.Option("--preset", help="historical range preset id，可重复。"),
    ] = None,
    top: Annotated[
        int,
        typer.Option("--top", help="每个 preset/search 保留 ranking 前 N 个 candidates。"),
    ] = 10,
    max_candidates: Annotated[
        int | None,
        typer.Option("--max-candidates", help="lower-than-config candidate evaluation cap。"),
    ] = None,
    search_config_path: Annotated[
        Path,
        typer.Option(help="weight search config YAML path。"),
    ] = DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH,
    preset_config_path: Annotated[
        Path,
        typer.Option(help="historical range preset config YAML path。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_PRESET_CONFIG_PATH,
    prices_path: Annotated[
        Path,
        typer.Option(help="ETF 标准价格缓存路径。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="historical weight search diagnostics 输出目录。"),
    ] = DEFAULT_WEIGHT_SEARCH_DIAGNOSTICS_DIR,
    cache: Annotated[
        str,
        typer.Option(
            "--cache",
            help="diagnostics cache mode: read-write, read-only, or disabled。",
        ),
    ] = "read-write",
    no_cache: Annotated[
        bool,
        typer.Option("--no-cache", help="关闭 diagnostics cache。"),
    ] = False,
    force_refresh: Annotated[
        bool,
        typer.Option("--force-refresh", help="忽略可用 cache 并重算。"),
    ] = False,
    workers: Annotated[
        str,
        typer.Option("--workers", help="parallel worker count: auto 或正整数。"),
    ] = "auto",
    resume: Annotated[
        bool,
        typer.Option("--resume", help="按 run manifest / cache 尝试恢复 diagnostics run。"),
    ] = False,
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="resume 或固定 run manifest id。"),
    ] = None,
    include_performance_report: Annotated[
        bool,
        typer.Option("--include-performance-report", help="写出 runtime performance report。"),
    ] = False,
    profile: Annotated[
        str,
        typer.Option("--profile", help="profiling mode: off, summary, detailed, or cprofile。"),
    ] = "summary",
    profile_output: Annotated[
        Path | None,
        typer.Option("--profile-output", help="profiling artifacts 输出目录。"),
    ] = None,
    profile_top_n: Annotated[
        int | None,
        typer.Option("--profile-top-n", help="profiling report Top-N rows。"),
    ] = None,
    cache_policy_path: Annotated[
        Path,
        typer.Option(help="weight calibration cache policy YAML path。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_CACHE_POLICY_CONFIG_PATH,
    profiling_policy_path: Annotated[
        Path,
        typer.Option(help="weight calibration profiling policy YAML path。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_PROFILING_POLICY_CONFIG_PATH,
    performance_report_dir: Annotated[
        Path,
        typer.Option(help="weight calibration performance report 输出目录。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_PERFORMANCE_REPORT_DIR,
) -> None:
    """生成 TRADING-079 historical weight search diagnostics and rescue report。"""
    if resume and not run_id:
        typer.echo("--resume requires --run-id for auditable diagnostics resume。")
        raise typer.Exit(code=2)
    if profile_top_n is not None and profile_top_n <= 0:
        raise typer.BadParameter("--profile-top-n must be positive")
    config = load_etf_config_bundle()
    registry = load_weight_search_registry(search_config_path, etf_config=config)
    preset_registry = load_weight_calibration_preset_registry(
        preset_config_path,
        etf_config=config,
        weight_search_registry=registry,
    )
    cache_policy = load_weight_calibration_cache_policy_config(cache_policy_path)
    profiling_policy = load_weight_calibration_profiling_policy_config(profiling_policy_path)
    profile_mode = normalize_weight_calibration_profile_mode(profile, policy=profiling_policy)
    profile_settings = profiling_mode_settings(profiling_policy, profile_mode)
    cache_mode = "disabled" if no_cache else cache
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 weight diagnostics。")
        raise typer.Exit(code=1)
    selected_searches = list(search or ["etf_initial_weight_search_v1"])
    if include_robust_packs:
        selected_searches.extend(WEIGHT_ROBUST_SEARCH_PACK_IDS)
    selected_searches = list(dict.fromkeys(selected_searches))
    selected_presets = list(preset or WEIGHT_SEARCH_DIAGNOSTICS_DEFAULT_PRESETS)
    builder_kwargs = {
        "etf_config": config,
        "quality_report": quality_report,
        "registry": registry,
        "preset_registry": preset_registry,
        "search_ids": selected_searches,
        "preset_ids": selected_presets,
        "top": top,
        "max_candidates": max_candidates,
        "source_paths": {
            "weight_search_config": str(search_config_path),
            "weight_calibration_presets": str(preset_config_path),
            "prices": str(prices_path),
            "cache_policy": str(cache_policy_path),
            "profiling_policy": str(profiling_policy_path),
        },
        "cache_policy": cache_policy,
        "cache_mode": cache_mode,
        "force_refresh": force_refresh,
        "workers": workers,
        "resume_run_id": run_id if resume else None,
        "include_performance_report": include_performance_report,
        "profiling_policy": profiling_policy,
        "profile_mode": profile_mode,
    }
    payload, cprofile_profiler = run_with_optional_cprofile(
        build_historical_weight_search_diagnostics_report,
        prices,
        enabled=profile_settings.cprofile,
        **builder_kwargs,
    )
    paths = write_historical_weight_search_diagnostics_report(
        payload,
        output_dir=output_dir,
    )
    performance_paths = None
    if include_performance_report and isinstance(payload.get("performance_report"), dict):
        performance_paths = write_weight_calibration_performance_report(
            payload["performance_report"],
            output_dir=performance_report_dir,
        )
    profile_paths = None
    cprofile_paths = None
    candidate_hotspot_paths = None
    if profile_settings.enabled:
        run_manifest = payload.get("run_manifest") or {}
        profile_dir = profile_output or (
            DEFAULT_WEIGHT_CALIBRATION_PROFILING_REPORT_DIR
            / str(run_manifest.get("run_id") or "unknown_run")
        )
        if cprofile_profiler is not None:
            cprofile_paths = write_cprofile_artifacts(
                cprofile_profiler,
                output_dir=profile_dir,
                top_n=profile_top_n or profiling_policy.weight_calibration_profiling.top_n,
            )
        profiling_report = build_weight_calibration_profiling_report(
            payload,
            policy=profiling_policy,
            profile_mode=profile_mode,
            profile_top_n=profile_top_n,
            cprofile_artifacts=cprofile_paths,
        )
        profile_paths = write_weight_calibration_profiling_report(
            profiling_report,
            output_dir=profile_dir,
        )
        candidate_hotspot_paths = write_weight_calibration_candidate_hotspot_table(
            profiling_report["candidate_hotspots"],
            output_dir=profile_dir,
        )
    criteria = payload["shadow_minimum_criteria"]
    cache_summary = payload.get("cache_summary") or {}
    typer.echo(f"ETF historical weight search diagnostics：{paths['markdown']}")
    typer.echo(f"json={paths['json']}")
    typer.echo(f"stable_shapes_csv={paths['stable_shapes_csv']}")
    typer.echo(f"near_shadow_csv={paths['near_shadow_csv']}")
    if performance_paths is not None:
        typer.echo(f"performance_report={performance_paths['markdown']}")
        typer.echo(f"performance_json={performance_paths['json']}")
    if profile_paths is not None:
        typer.echo(f"profiling_report={profile_paths['markdown']}")
        typer.echo(f"profiling_json={profile_paths['json']}")
        typer.echo(f"candidate_hotspots={candidate_hotspot_paths['markdown']}")
    if cprofile_paths is not None:
        typer.echo(f"cprofile_stats={cprofile_paths['stats']}")
        typer.echo(f"cprofile_top_functions={cprofile_paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"preset_result_count={payload['preset_result_count']}")
    typer.echo(f"candidate_observation_count={payload['candidate_observation_count']}")
    typer.echo(f"shadow_ready_count={criteria['shadow_ready_count']}")
    typer.echo(f"minimum_criteria_status={criteria['status']}")
    typer.echo(f"cache_mode={cache_summary.get('cache_mode', cache_mode)}")
    typer.echo(
        "price_returns_matrix_cache_status="
        f"{cache_summary.get('price_returns_matrix_cache_status', 'not_reported')}"
    )
    typer.echo(
        "diagnostics_aggregation_cache_status="
        f"{cache_summary.get('diagnostics_aggregation_cache_status', 'not_reported')}"
    )
    typer.echo(f"cache_hit_count={cache_summary.get('cache_hit_count', 0)}")
    typer.echo(f"cache_miss_count={cache_summary.get('cache_miss_count', 0)}")
    typer.echo(f"cache_write_count={cache_summary.get('cache_write_count', 0)}")
    typer.echo(f"resume_status={cache_summary.get('resume_status', 'not_reported')}")
    typer.echo(f"worker_count={cache_summary.get('worker_count', workers)}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("performance-validate")
def weight_calibration_performance_validate_command(
    cache_policy_path: Annotated[
        Path,
        typer.Option(help="weight calibration cache policy YAML path。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_CACHE_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="cache / parallel validation report 输出目录。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_CACHE_VALIDATION_DIR,
) -> None:
    """校验 TRADING-080 cache、parallel runner、resume 和 performance safety gate。"""
    payload = build_weight_calibration_cache_parallel_validation_report(
        policy_config_path=cache_policy_path,
    )
    paths = write_weight_calibration_cache_parallel_validation_report(
        payload,
        output_dir=output_dir,
    )
    typer.echo(f"ETF weight calibration cache / parallel validation：{paths['markdown']}")
    typer.echo(f"json={paths['json']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)

@weight_calibration_app.command("profiling-validate")
def weight_calibration_profiling_validate_command(
    profiling_policy_path: Annotated[
        Path,
        typer.Option(help="weight calibration profiling policy YAML path。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_PROFILING_POLICY_CONFIG_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry YAML path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="profiling validation report 输出目录。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_PROFILING_VALIDATION_DIR,
) -> None:
    """校验 TRADING-081 profiling workflow 和 safety boundary。"""
    payload = build_weight_calibration_profiling_validation_report(
        policy_config_path=profiling_policy_path,
        report_registry_path=report_registry_path,
    )
    paths = write_weight_calibration_profiling_validation_report(
        payload,
        output_dir=output_dir,
    )
    typer.echo(f"ETF weight calibration profiling validation：{paths['markdown']}")
    typer.echo(f"json={paths['json']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@weight_calibration_app.command("generate-proposals")
def weight_calibration_generate_proposals_command(
    candidate_registry_path: Annotated[
        Path,
        typer.Option(help="candidate initial weight set registry path。"),
    ] = DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH,
    evidence_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-071F evidence JSON path。"),
    ] = None,
    overfit_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-071G overfit diagnostics JSON path。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="candidate weight proposal 输出目录。"),
    ] = DEFAULT_WEIGHT_PROPOSAL_DIR,
) -> None:
    """生成 TRADING-071H candidate weight proposal-only recommendations。"""
    payload = build_candidate_weight_proposals(
        candidate_registry=load_candidate_weight_registry(candidate_registry_path),
        evidence_payload=_load_optional_json_payload(evidence_path),
        overfit_payload=_load_optional_json_payload(overfit_path),
    )
    paths = write_candidate_weight_proposals(payload, output_dir=output_dir)
    typer.echo(f"ETF weight candidate proposals：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"proposal_count={payload['proposal_count']}")
    typer.echo(f"proposal_type_counts={payload['proposal_type_counts']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("report")
def weight_calibration_report_command(
    latest: Annotated[
        bool,
        typer.Option(
            "--latest",
            help="读取最新 historical/evidence/diagnostics/proposal artifacts。",
        ),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option(help="报告日期；默认使用今天。"),
    ] = None,
    search_run_id: Annotated[
        str | None,
        typer.Option("--search-run-id", help="historical weight search run id。"),
    ] = None,
    search_output_dir: Annotated[
        Path,
        typer.Option(help="weight calibration search report 输出目录。"),
    ] = DEFAULT_ETF_WEIGHT_CALIBRATION_REPORT_DIR,
    candidate_registry_path: Annotated[
        Path,
        typer.Option(help="candidate initial weight set registry path。"),
    ] = DEFAULT_CANDIDATE_WEIGHT_REGISTRY_PATH,
    enrollment_path: Annotated[
        Path,
        typer.Option(help="weight calibration forward enrollment ledger path。"),
    ] = DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH,
    evidence_path: Annotated[
        Path | None,
        typer.Option(help="TRADING-071F evidence JSON path；`--latest` 时可省略。"),
    ] = None,
    overfit_path: Annotated[
        Path | None,
        typer.Option(help="TRADING-071G overfit diagnostics JSON path；`--latest` 时可省略。"),
    ] = None,
    proposals_path: Annotated[
        Path | None,
        typer.Option(help="TRADING-071H proposal JSON path；`--latest` 时可省略。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="dual-track calibration report 输出目录。"),
    ] = DEFAULT_WEIGHT_DUAL_TRACK_REPORT_DIR,
) -> None:
    """生成 TRADING-071I dual-track calibration JSON/Markdown report。"""
    if search_run_id is not None and latest:
        raise typer.BadParameter("--search-run-id and --latest cannot be combined")
    report_date = _parse_date(as_of) if as_of else date.today()
    source_paths: dict[str, str] = {
        "candidate_registry": str(candidate_registry_path),
        "forward_enrollment": str(enrollment_path),
    }
    search_payload = None
    if search_run_id is not None or latest:
        run_dir = (
            find_latest_weight_search_run_dir(search_output_dir)
            if latest
            else search_output_dir / str(search_run_id)
        )
        search_payload = read_weight_search_run_payload(run_dir)
        source_paths["historical_search"] = str(run_dir / "summary.json")

    resolved_evidence_path = evidence_path or (
        _latest_json_file(DEFAULT_WEIGHT_FORWARD_EVIDENCE_DIR, "backtest_forward_evidence_*.json")
        if latest
        else None
    )
    resolved_overfit_path = overfit_path or (
        _latest_json_file(DEFAULT_WEIGHT_OVERFIT_DIAGNOSTICS_DIR, "overfit_diagnostics_*.json")
        if latest
        else None
    )
    resolved_proposals_path = proposals_path or (
        _latest_json_file(DEFAULT_WEIGHT_PROPOSAL_DIR, "candidate_weight_proposals_*.json")
        if latest
        else None
    )
    for key, path in {
        "backtest_forward_evidence": resolved_evidence_path,
        "overfit_diagnostics": resolved_overfit_path,
        "candidate_weight_proposals": resolved_proposals_path,
    }.items():
        if path is not None:
            source_paths[key] = str(path)

    payload = build_dual_track_weight_calibration_report(
        as_of=report_date,
        candidate_registry=load_candidate_weight_registry(candidate_registry_path),
        forward_enrollments=load_weight_forward_enrollments(enrollment_path),
        search_payload=search_payload,
        evidence_payload=_load_optional_json_payload(resolved_evidence_path),
        overfit_payload=_load_optional_json_payload(resolved_overfit_path),
        proposals_payload=_load_optional_json_payload(resolved_proposals_path),
        source_paths=source_paths,
    )
    paths = write_dual_track_weight_calibration_report(payload, output_dir=output_dir)
    typer.echo(f"ETF weight dual-track calibration report：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['summary']['candidate_count']}")
    typer.echo(f"proposal_count={payload['summary']['proposal_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@weight_calibration_app.command("validate")
def weight_calibration_validate_command(
    search_config_path: Annotated[
        Path,
        typer.Option(help="weight search config YAML path。"),
    ] = DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry YAML path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    proposals_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-071H proposal JSON path。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-071I dual-track calibration report JSON path。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="dual-track calibration validation 输出目录。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_VALIDATION_DIR,
) -> None:
    """执行 TRADING-071K dual-track calibration validation gate。"""
    payload = build_dual_track_weight_calibration_validation_report(
        search_config_path=search_config_path,
        report_registry_path=report_registry_path,
        proposals_payload=(
            _load_optional_json_payload(proposals_path) if proposals_path is not None else None
        ),
        report_payload=(
            _load_optional_json_payload(report_path) if report_path is not None else None
        ),
    )
    paths = write_dual_track_weight_calibration_validation_report(
        payload,
        output_dir=output_dir,
    )
    typer.echo(f"ETF weight dual-track calibration validation gate：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@weight_calibration_app.command("usability-validate")
def weight_calibration_usability_validate_command(
    search_config_path: Annotated[
        Path,
        typer.Option(help="weight search config YAML path。"),
    ] = DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH,
    preset_config_path: Annotated[
        Path,
        typer.Option(help="historical range preset config YAML path。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_PRESET_CONFIG_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry YAML path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    recommendation_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078G recommendation report JSON path。"),
    ] = None,
    enrollment_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-078F forward enrollment registry JSON path。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="historical calibration usability validation 输出目录。"),
    ] = DEFAULT_WEIGHT_CALIBRATION_VALIDATION_DIR,
) -> None:
    """执行 TRADING-078I historical calibration usability validation gate。"""
    payload = build_historical_weight_calibration_usability_validation_report(
        search_config_path=search_config_path,
        preset_config_path=preset_config_path,
        report_registry_path=report_registry_path,
        recommendation_payload=(
            _load_optional_json_payload(recommendation_path)
            if recommendation_path is not None
            else None
        ),
        enrollment_payload=(
            _load_optional_json_payload(enrollment_path) if enrollment_path is not None else None
        ),
    )
    paths = write_historical_weight_calibration_usability_validation_report(
        payload,
        output_dir=output_dir,
    )
    typer.echo(f"ETF historical weight calibration usability validation gate：{paths['markdown']}")
    typer.echo(f"json={paths['json']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
