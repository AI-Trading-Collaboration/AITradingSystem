from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated, Literal, cast

import pandas as pd
import typer
import yaml
from rich.console import Console

from ai_trading_system.backtest.daily import (
    DEFAULT_BENCHMARK_TICKERS,
    BacktestRegimeContext,
)
from ai_trading_system.benchmark_policy import (
    DEFAULT_BENCHMARK_POLICY_CONFIG_PATH,
    default_benchmark_policy_report_path,
    load_benchmark_policy,
    lookup_benchmark_policy_entry,
    render_benchmark_policy_lookup,
    validate_benchmark_policy,
    write_benchmark_policy_report,
)
from ai_trading_system.calibration_protocol import (
    DEFAULT_CALIBRATION_PROTOCOL_PATH,
    default_calibration_protocol_report_path,
    load_calibration_protocol_manifest,
    validate_calibration_protocol_manifest,
    write_calibration_protocol_report,
)
from ai_trading_system.config import (
    DEFAULT_MARKET_REGIMES_CONFIG_PATH,
    PROJECT_ROOT,
    configured_rate_series,
    load_data_quality,
    load_market_regimes,
    load_universe,
    market_regime_by_id,
)
from ai_trading_system.data.quality import (
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.decision_causal_chains import (
    DEFAULT_DECISION_CAUSAL_CHAIN_PATH,
    build_decision_causal_chain_ledger,
    default_decision_causal_chain_report_path,
    load_decision_causal_chain_ledger,
    load_decision_outcomes_frame,
    lookup_decision_causal_chain,
    render_decision_causal_chain_lookup,
    write_decision_causal_chain_ledger,
    write_decision_causal_chain_report,
)
from ai_trading_system.decision_learning_queue import (
    DEFAULT_DECISION_LEARNING_QUEUE_PATH,
    build_decision_learning_queue,
    default_decision_learning_queue_report_path,
    load_decision_learning_queue,
    lookup_decision_learning_item,
    render_decision_learning_item_lookup,
    write_decision_learning_queue,
    write_decision_learning_queue_report,
)
from ai_trading_system.decision_outcomes import (
    DEFAULT_DECISION_OUTCOMES_PATH,
    DEFAULT_OUTCOME_HORIZONS,
    build_decision_outcomes,
    default_decision_calibration_report_path,
    load_decision_snapshots,
    write_decision_calibration_report,
    write_decision_outcomes_csv,
)
from ai_trading_system.decision_snapshots import (
    DEFAULT_DECISION_SNAPSHOT_DIR,
    default_decision_snapshot_path,
)
from ai_trading_system.feedback_loop_review import (
    build_feedback_loop_review_report,
    default_feedback_loop_review_report_path,
    write_feedback_loop_review_report,
)
from ai_trading_system.feedback_sample_policy import (
    DEFAULT_FEEDBACK_SAMPLE_POLICY_PATH,
    load_feedback_sample_policy,
)
from ai_trading_system.market_feedback_optimization import (
    DEFAULT_MARKET_FEEDBACK_REPLAY_START,
    build_market_feedback_optimization_report,
    default_market_feedback_optimization_report_path,
    write_market_feedback_optimization_report,
)
from ai_trading_system.parameter_candidates import (
    DEFAULT_PARAMETER_CANDIDATE_LEDGER_PATH,
    build_parameter_candidate_ledger,
    default_parameter_candidate_report_path,
    load_parameter_candidate_ledger,
    write_parameter_candidate_ledger,
    write_parameter_candidate_report,
)
from ai_trading_system.parameter_governance import (
    DEFAULT_PARAMETER_GOVERNANCE_MANIFEST_PATH,
    build_parameter_governance_report,
    default_parameter_governance_report_path,
    default_parameter_governance_summary_path,
    write_parameter_governance_report,
    write_parameter_governance_summary,
)
from ai_trading_system.parameter_replay import (
    DEFAULT_BACKTEST_ROBUSTNESS_DIR,
    build_parameter_replay_report,
    default_parameter_replay_report_path,
    default_parameter_replay_summary_path,
    latest_backtest_robustness_summary_path,
    write_parameter_replay_report,
    write_parameter_replay_summary,
)
from ai_trading_system.prediction_ledger import (
    DEFAULT_PARAMETER_SHADOW_PREDICTION_LEDGER_PATH,
    DEFAULT_PREDICTION_LEDGER_PATH,
    DEFAULT_PREDICTION_OUTCOMES_PATH,
    append_prediction_records,
    build_parameter_shadow_prediction_records,
    build_prediction_outcomes,
    build_shadow_maturity_report,
    build_shadow_prediction_records,
    build_shadow_prediction_run_report,
    default_prediction_outcome_report_path,
    default_shadow_maturity_report_path,
    default_shadow_prediction_report_path,
    load_prediction_ledger,
    load_prediction_outcomes,
    write_prediction_outcome_report,
    write_prediction_outcomes_csv,
    write_shadow_maturity_report,
    write_shadow_prediction_run_report,
)
from ai_trading_system.rule_experiments import (
    DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
    build_rule_experiment_ledger,
    default_rule_experiment_report_path,
    load_rule_experiment_ledger,
    lookup_rule_experiment,
    render_rule_experiment_lookup,
    write_rule_experiment_ledger,
    write_rule_experiment_report,
)
from ai_trading_system.rule_governance import (
    DEFAULT_RULE_CARDS_PATH,
    default_rule_governance_report_path,
    load_rule_card_store,
    lookup_rule_card,
    promote_rule_card,
    render_rule_card_lookup,
    retire_rule_card,
    validate_rule_card_store,
    write_rule_governance_report,
    write_rule_lifecycle_action_report,
)
from ai_trading_system.shadow_iteration import (
    DEFAULT_SHADOW_ITERATION_REGISTRY_PATH,
    DEFAULT_SHADOW_ITERATION_REPORT_DIR,
    DEFAULT_SHADOW_ITERATION_RUN_ROOT,
    build_forward_shadow_evaluation_report,
    build_shadow_iteration_report,
    register_forward_shadow_candidate,
    write_forward_shadow_evaluation_outputs,
    write_shadow_iteration_outputs,
)
from ai_trading_system.shadow_weight_profiles import (
    DEFAULT_DECISION_SNAPSHOT_SEARCH_DIR,
    DEFAULT_SHADOW_PARAMETER_OBJECTIVE_PATH,
    DEFAULT_SHADOW_PARAMETER_PROMOTION_CONTRACT_PATH,
    DEFAULT_SHADOW_PARAMETER_SEARCH_OUTPUT_ROOT,
    DEFAULT_SHADOW_PARAMETER_SEARCH_SPACE_PATH,
    DEFAULT_SHADOW_POSITION_GATE_PROFILE_MANIFEST_PATH,
    DEFAULT_SHADOW_WEIGHT_PROFILE_MANIFEST_PATH,
    DEFAULT_SHADOW_WEIGHT_PROFILE_OBSERVATION_LEDGER_PATH,
    build_shadow_parameter_promotion_report,
    build_shadow_parameter_search_report,
    build_shadow_weight_performance_report,
    build_shadow_weight_prediction_records,
    build_shadow_weight_profile_run_report,
    default_shadow_parameter_promotion_report_path,
    default_shadow_weight_performance_csv_path,
    default_shadow_weight_performance_report_path,
    default_shadow_weight_profile_report_path,
    write_shadow_parameter_promotion_report,
    write_shadow_parameter_search_bundle,
    write_shadow_weight_observation_ledger,
    write_shadow_weight_performance_csv,
    write_shadow_weight_performance_report,
    write_shadow_weight_profile_report,
)
from ai_trading_system.weight_calibration import (
    DEFAULT_APPROVED_CALIBRATION_OVERLAY_PATH,
    DEFAULT_CURRENT_CONTEXT_PATH,
    DEFAULT_EFFECTIVE_WEIGHTS_PATH,
    DEFAULT_WEIGHT_PROFILE_PATH,
    apply_calibration_overlays,
    load_calibration_overlays,
    load_weight_profile,
    write_effective_weights,
)

feedback_app = typer.Typer(help="决策结果观察、校准和因果链查询。", no_args_is_help=True)
console = Console()


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


def _parse_csv_items(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _parse_positive_int_csv(value: str, label: str) -> list[int]:
    items = _parse_csv_items(value)
    if not items:
        raise typer.BadParameter(f"{label}不能为空。")
    parsed: list[int] = []
    for item in items:
        try:
            integer = int(item)
        except ValueError as exc:
            raise typer.BadParameter(f"{label}必须是逗号分隔的正整数。") from exc
        if integer <= 0:
            raise typer.BadParameter(f"{label}必须是正整数。")
        parsed.append(integer)
    return parsed


def _download_manifest_path(prices_path: Path) -> Path:
    return prices_path.parent / "download_manifest.csv"


def _marketstack_prices_path(prices_path: Path) -> Path:
    return prices_path.parent / "prices_marketstack_daily.csv"


def _requires_marketstack_prices(prices_path: Path) -> bool:
    default_prices_path = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
    try:
        return prices_path.resolve() == default_prices_path.resolve()
    except OSError:
        return prices_path == default_prices_path


def _path_from_snapshot_trace(snapshot: dict[str, object]) -> Path | None:
    trace = snapshot.get("trace")
    if not isinstance(trace, dict):
        return None
    path_text = trace.get("trace_bundle_path")
    return Path(str(path_text)) if path_text else None


def _trace_dataset_path(trace_bundle: dict[str, object], dataset_type: str) -> Path | None:
    dataset_refs = trace_bundle.get("dataset_refs")
    if not isinstance(dataset_refs, list):
        return None
    for dataset in dataset_refs:
        if not isinstance(dataset, dict):
            continue
        if dataset.get("dataset_type") == dataset_type and dataset.get("path"):
            return Path(str(dataset["path"]))
    return None


def _trace_quality_report_path(trace_bundle: dict[str, object]) -> Path | None:
    quality_refs = trace_bundle.get("quality_refs")
    if not isinstance(quality_refs, list):
        return None
    for quality in quality_refs:
        if not isinstance(quality, dict):
            continue
        if quality.get("report_path"):
            return Path(str(quality["report_path"]))
    return None


@feedback_app.command("apply-calibration-overlay")
def apply_calibration_overlay_command(
    context_path: Annotated[
        Path,
        typer.Option(help="当前 decision context JSON 路径。"),
    ] = DEFAULT_CURRENT_CONTEXT_PATH,
    weight_profile_path: Annotated[
        Path,
        typer.Option(help="当前基础权重 profile YAML 路径。"),
    ] = DEFAULT_WEIGHT_PROFILE_PATH,
    overlays_path: Annotated[
        Path,
        typer.Option(help="approved calibration overlay JSON 路径。"),
    ] = DEFAULT_APPROVED_CALIBRATION_OVERLAY_PATH,
    output_path: Annotated[
        Path,
        typer.Option(help="effective weights JSON 输出路径。"),
    ] = DEFAULT_EFFECTIVE_WEIGHTS_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校准匹配日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
) -> None:
    """根据 context、weight profile 和 approved overlays 计算 effective weights。"""
    calibration_date = _parse_date(as_of) if as_of else date.today()
    try:
        profile = load_weight_profile(weight_profile_path)
        overlays = load_calibration_overlays(overlays_path)
        if context_path.exists():
            context = json.loads(context_path.read_text(encoding="utf-8"))
            if not isinstance(context, dict):
                raise ValueError("context JSON must contain an object")
        elif overlays:
            raise ValueError(f"context JSON not found: {context_path}")
        else:
            context = {
                "as_of": calibration_date.isoformat(),
                "context_path": str(context_path),
                "context_status": "missing_no_approved_overlay",
            }
        application = apply_calibration_overlays(
            context=context,
            profile=profile,
            overlays=overlays,
            as_of=calibration_date,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]历史校准 overlay 计算失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc

    written_path = write_effective_weights(application, output_path)
    console.print("[green]历史校准 effective weights 已生成。[/green]")
    console.print(f"Weight profile version：{application.weight_profile_version}")
    console.print(
        "Matched overlays："
        f"{', '.join(application.matched_overlays) if application.matched_overlays else '无'}"
    )
    console.print(f"Confidence delta：{application.confidence_delta:+.2f}")
    console.print(f"Position multiplier：{application.position_multiplier:.2f}")
    console.print(f"输出：{written_path}")
    console.print("治理边界：本命令只计算校准结果，不修改 production scoring 或 position_gate。")


@feedback_app.command("validate-calibration-protocol")
def validate_calibration_protocol_command(
    manifest_path: Annotated[
        Path,
        typer.Option(help="调权实验 protocol manifest YAML 路径。"),
    ] = DEFAULT_CALIBRATION_PROTOCOL_PATH,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 校验报告输出路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
) -> None:
    """校验回测调权 protocol manifest，防止无审计调参。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    try:
        manifest = load_calibration_protocol_manifest(manifest_path)
    except (OSError, ValueError, yaml.YAMLError) as exc:
        console.print(f"[red]调权协议 manifest 读取失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    report = validate_calibration_protocol_manifest(
        manifest,
        manifest_path=manifest_path,
        as_of=validation_date,
    )
    report_output = output_path or default_calibration_protocol_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report_output = write_calibration_protocol_report(report, report_output)
    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]调权协议校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_output}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    console.print("治理边界：本命令不修改 production scoring、position_gate、overlay 或回测仓位。")
    if not report.passed:
        raise typer.Exit(code=1)


@feedback_app.command("calibrate")
def calibrate_decision_outcomes(
    decision_snapshot_path: Annotated[
        Path,
        typer.Option(help="decision_snapshot JSON 文件或目录路径。"),
    ] = DEFAULT_DECISION_SNAPSHOT_DIR,
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径，用于复用数据质量门禁。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="校准截止日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    horizons: Annotated[
        str,
        typer.Option(help="逗号分隔的 outcome 观察窗口，单位为交易日。"),
    ] = ",".join(str(item) for item in DEFAULT_OUTCOME_HORIZONS),
    strategy_ticker: Annotated[
        str,
        typer.Option(help="AI proxy 或策略代理标的。"),
    ] = "SMH",
    benchmarks: Annotated[
        str,
        typer.Option(help="逗号分隔的对比基准 ticker。"),
    ] = ",".join(DEFAULT_BENCHMARK_TICKERS),
    benchmark_policy_path: Annotated[
        Path,
        typer.Option(help="benchmark policy YAML 路径，用于审计 AI proxy / benchmark 解释口径。"),
    ] = DEFAULT_BENCHMARK_POLICY_CONFIG_PATH,
    benchmark_policy_report_path: Annotated[
        Path | None,
        typer.Option(help="可选：Markdown benchmark policy 校验报告输出路径。"),
    ] = None,
    outcomes_path: Annotated[
        Path,
        typer.Option(help="decision_outcomes CSV 输出路径。"),
    ] = DEFAULT_DECISION_OUTCOMES_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 校准报告输出路径。"),
    ] = None,
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告输出路径。"),
    ] = None,
) -> None:
    """从历史 decision_snapshot 生成 outcome 和评分校准报告。"""
    calibration_date = _parse_date(as_of) if as_of else date.today()
    horizon_values = _parse_positive_int_csv(horizons, "outcome 观察窗口")
    benchmark_tickers = tuple(_parse_csv_items(benchmarks))
    if not benchmark_tickers:
        raise typer.BadParameter("至少需要一个对比基准 ticker。")
    benchmark_policy_report = validate_benchmark_policy(
        load_benchmark_policy(benchmark_policy_path),
        as_of=calibration_date,
        selected_strategy_ticker=strategy_ticker,
        selected_benchmark_tickers=benchmark_tickers,
    )
    if benchmark_policy_report_path is not None:
        write_benchmark_policy_report(benchmark_policy_report, benchmark_policy_report_path)
    if not benchmark_policy_report.passed:
        console.print("[red]基准政策校验失败，已停止决策校准。[/red]")
        console.print(
            f"错误数：{benchmark_policy_report.error_count}；"
            f"警告数：{benchmark_policy_report.warning_count}"
        )
        if benchmark_policy_report_path is not None:
            console.print(f"基准政策报告：{benchmark_policy_report_path}")
        raise typer.Exit(code=1)
    tickers = list(dict.fromkeys([strategy_ticker, *benchmark_tickers]))
    universe = load_universe()
    data_quality_config = load_data_quality()
    quality_output = quality_report_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        calibration_date,
    )
    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=tickers,
        expected_rate_series=configured_rate_series(universe),
        quality_config=data_quality_config,
        as_of=calibration_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        console.print("[red]数据质量门禁失败，已停止决策校准。[/red]")
        console.print(f"数据质量报告：{quality_output}")
        console.print(
            f"错误数：{data_quality_report.error_count}；"
            f"警告数：{data_quality_report.warning_count}"
        )
        raise typer.Exit(code=1)

    snapshots = load_decision_snapshots(decision_snapshot_path)
    if not snapshots:
        raise typer.BadParameter(f"未找到 decision_snapshot：{decision_snapshot_path}")
    prices_frame = pd.read_csv(prices_path)
    market_regimes = load_market_regimes(DEFAULT_MARKET_REGIMES_CONFIG_PATH)
    default_market_regime = market_regime_by_id(
        market_regimes,
        market_regimes.default_backtest_regime,
    )
    market_regime = BacktestRegimeContext(
        regime_id=default_market_regime.regime_id,
        name=default_market_regime.name,
        start_date=default_market_regime.start_date,
        anchor_date=default_market_regime.anchor_date,
        anchor_event=default_market_regime.anchor_event,
        description=default_market_regime.description,
    )
    result = build_decision_outcomes(
        snapshots=snapshots,
        prices=prices_frame,
        as_of=calibration_date,
        horizons=tuple(horizon_values),
        strategy_ticker=strategy_ticker,
        benchmark_tickers=benchmark_tickers,
        market_regime=market_regime,
        data_quality_report=data_quality_report,
        benchmark_policy_report=benchmark_policy_report,
    )
    outcomes_output = write_decision_outcomes_csv(result, outcomes_path)
    calibration_report_output = report_path or default_decision_calibration_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        calibration_date,
    )
    calibration_report_output = write_decision_calibration_report(
        result,
        outcomes_path=outcomes_output,
        data_quality_report_path=quality_output,
        output_path=calibration_report_output,
    )

    sample_policy = load_feedback_sample_policy()
    decision_diagnostic_floor = sample_policy.decision_outcomes.diagnostic_floor
    status_style = "green" if len(result.available_rows) >= decision_diagnostic_floor else "yellow"
    console.print(
        f"[{status_style}]决策校准完成。可用 outcome："
        f"{len(result.available_rows)}[/{status_style}]"
    )
    console.print(f"校准报告：{calibration_report_output}")
    console.print(f"Outcome CSV：{outcomes_output}")
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")
    console.print(f"基准政策状态：{benchmark_policy_report.status}")
    if benchmark_policy_report_path is not None:
        console.print(f"基准政策报告：{benchmark_policy_report_path}")


@feedback_app.command("calibrate-predictions")
def calibrate_prediction_outcomes(
    prediction_ledger_path: Annotated[
        Path,
        typer.Option(help="prediction/shadow ledger CSV 路径。"),
    ] = DEFAULT_PREDICTION_LEDGER_PATH,
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径，用于复用数据质量门禁。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="校准截止日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    horizons: Annotated[
        str,
        typer.Option(help="逗号分隔的 prediction outcome 观察窗口，单位为交易日。"),
    ] = ",".join(str(item) for item in DEFAULT_OUTCOME_HORIZONS),
    strategy_ticker: Annotated[
        str,
        typer.Option(help="AI proxy 或策略代理标的。"),
    ] = "SMH",
    benchmarks: Annotated[
        str,
        typer.Option(help="逗号分隔的对比基准 ticker。"),
    ] = ",".join(DEFAULT_BENCHMARK_TICKERS),
    outcomes_path: Annotated[
        Path,
        typer.Option(help="prediction_outcomes CSV 输出路径。"),
    ] = DEFAULT_PREDICTION_OUTCOMES_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown prediction outcome 报告输出路径。"),
    ] = None,
    quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告输出路径。"),
    ] = None,
) -> None:
    """从 prediction/shadow ledger 生成前向 outcome 和模型版本分桶报告。"""
    calibration_date = _parse_date(as_of) if as_of else date.today()
    horizon_values = _parse_positive_int_csv(horizons, "prediction outcome 观察窗口")
    benchmark_tickers = tuple(_parse_csv_items(benchmarks))
    if not benchmark_tickers:
        raise typer.BadParameter("至少需要一个对比基准 ticker。")
    prediction_rows = load_prediction_ledger(prediction_ledger_path)
    if not prediction_rows:
        raise typer.BadParameter(f"未找到 prediction ledger：{prediction_ledger_path}")
    tickers = list(dict.fromkeys([strategy_ticker, *benchmark_tickers]))
    universe = load_universe()
    data_quality_config = load_data_quality()
    quality_output = quality_report_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        calibration_date,
    )
    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=tickers,
        expected_rate_series=configured_rate_series(universe),
        quality_config=data_quality_config,
        as_of=calibration_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(data_quality_report, quality_output)
    if not data_quality_report.passed:
        console.print("[red]数据质量门禁失败，已停止 prediction outcome 校准。[/red]")
        console.print(f"数据质量报告：{quality_output}")
        console.print(
            f"错误数：{data_quality_report.error_count}；"
            f"警告数：{data_quality_report.warning_count}"
        )
        raise typer.Exit(code=1)

    market_regimes = load_market_regimes(DEFAULT_MARKET_REGIMES_CONFIG_PATH)
    default_market_regime = market_regime_by_id(
        market_regimes,
        market_regimes.default_backtest_regime,
    )
    market_regime = BacktestRegimeContext(
        regime_id=default_market_regime.regime_id,
        name=default_market_regime.name,
        start_date=default_market_regime.start_date,
        anchor_date=default_market_regime.anchor_date,
        anchor_event=default_market_regime.anchor_event,
        description=default_market_regime.description,
    )
    result = build_prediction_outcomes(
        prediction_rows=prediction_rows,
        prices=pd.read_csv(prices_path),
        as_of=calibration_date,
        horizons=tuple(horizon_values),
        strategy_ticker=strategy_ticker,
        benchmark_tickers=benchmark_tickers,
        market_regime=market_regime,
        data_quality_report=data_quality_report,
    )
    outcomes_output = write_prediction_outcomes_csv(result, outcomes_path)
    report_output = report_path or default_prediction_outcome_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        calibration_date,
    )
    report_output = write_prediction_outcome_report(
        result,
        outcomes_path=outcomes_output,
        data_quality_report_path=quality_output,
        output_path=report_output,
    )

    sample_policy = load_feedback_sample_policy()
    prediction_diagnostic_floor = sample_policy.prediction_outcomes.diagnostic_floor
    status_style = (
        "green" if len(result.available_rows) >= prediction_diagnostic_floor else "yellow"
    )
    console.print(
        f"[{status_style}]Prediction outcome 校准完成。可用 outcome："
        f"{len(result.available_rows)}[/{status_style}]"
    )
    console.print(f"Prediction outcome 报告：{report_output}")
    console.print(f"Prediction outcome CSV：{outcomes_output}")
    console.print(f"数据质量报告：{quality_output}（{data_quality_report.status}）")


@feedback_app.command("run-shadow")
def run_shadow_predictions_command(
    rule_experiment_path: Annotated[
        Path,
        typer.Option(help="rule experiment ledger JSON 路径。"),
    ] = DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
    decision_snapshot_path: Annotated[
        Path | None,
        typer.Option(help="production decision_snapshot JSON 路径；不传时按 as-of 推导。"),
    ] = None,
    trace_bundle_path: Annotated[
        Path | None,
        typer.Option(help="production trace bundle JSON 路径；不传时从 snapshot.trace 推导。"),
    ] = None,
    features_path: Annotated[
        Path | None,
        typer.Option(help="特征快照 CSV 路径；不传时从 trace dataset_refs 推导。"),
    ] = None,
    data_quality_report_path: Annotated[
        Path | None,
        typer.Option(help="数据质量报告路径；不传时从 trace quality_refs 推导。"),
    ] = None,
    prediction_ledger_path: Annotated[
        Path,
        typer.Option(help="append-only prediction ledger CSV 输出路径。"),
    ] = DEFAULT_PREDICTION_LEDGER_PATH,
    candidate_ids: Annotated[
        str | None,
        typer.Option(help="可选：逗号分隔的 candidate_id 白名单。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="shadow 运行日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown shadow runner 报告输出路径。"),
    ] = None,
) -> None:
    """把 rule experiment challenger 追加到 prediction ledger，production_effect=none。"""
    run_date = _parse_date(as_of) if as_of else date.today()
    snapshot_path = decision_snapshot_path or default_decision_snapshot_path(
        DEFAULT_DECISION_SNAPSHOT_DIR,
        run_date,
    )
    if not snapshot_path.exists():
        raise typer.BadParameter(f"decision_snapshot 不存在：{snapshot_path}")
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    trace_path = trace_bundle_path or _path_from_snapshot_trace(snapshot)
    if trace_path is None or not trace_path.exists():
        raise typer.BadParameter(f"trace bundle 不存在：{trace_path}")
    trace_bundle = json.loads(trace_path.read_text(encoding="utf-8"))
    feature_snapshot_path = features_path or _trace_dataset_path(
        trace_bundle,
        "processed_feature_cache",
    )
    if feature_snapshot_path is None:
        raise typer.BadParameter("无法从 trace bundle 推导 feature snapshot 路径。")
    quality_path = data_quality_report_path or _trace_quality_report_path(trace_bundle)
    if quality_path is None:
        raise typer.BadParameter("无法从 trace bundle 推导 data quality report 路径。")
    try:
        rule_experiment_ledger = load_rule_experiment_ledger(rule_experiment_path)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"rule experiment ledger 不存在：{rule_experiment_path}") from exc
    selected = tuple(_parse_csv_items(candidate_ids)) if candidate_ids else ()
    records = build_shadow_prediction_records(
        snapshot=snapshot,
        trace_bundle=trace_bundle,
        trace_bundle_path=trace_path,
        features_path=feature_snapshot_path,
        data_quality_report_path=quality_path,
        rule_experiment_ledger=rule_experiment_ledger,
        as_of=run_date,
        selected_candidate_ids=selected,
    )
    ledger_output = append_prediction_records(records, prediction_ledger_path)
    report = build_shadow_prediction_run_report(
        as_of=run_date,
        decision_snapshot_path=snapshot_path,
        trace_bundle_path=trace_path,
        rule_experiment_path=rule_experiment_path,
        prediction_ledger_path=ledger_output,
        records=records,
        candidate_count=len(rule_experiment_ledger.get("candidates", [])),
        warnings=(
            ("没有可运行的 challenger；请检查 forward_shadow_plan 状态和日期窗口。",)
            if not records
            else ()
        ),
    )
    output_report = report_path or default_shadow_prediction_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        run_date,
    )
    output_report = write_shadow_prediction_run_report(report, output_report)
    style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{style}]Shadow runner 状态：{report.status}[/{style}]")
    console.print(f"写入 prediction：{report.appended_count}")
    console.print(f"Prediction ledger：{ledger_output}")
    console.print(f"Shadow runner 报告：{output_report}")


@feedback_app.command("run-parameter-shadow")
def run_parameter_shadow_predictions_command(
    parameter_candidate_ledger_path: Annotated[
        Path,
        typer.Option(help="parameter candidates ledger JSON 路径。"),
    ] = DEFAULT_PARAMETER_CANDIDATE_LEDGER_PATH,
    decision_snapshot_path: Annotated[
        Path | None,
        typer.Option(help="production decision_snapshot JSON 路径；不传时按 as-of 推导。"),
    ] = None,
    trace_bundle_path: Annotated[
        Path | None,
        typer.Option(help="production trace bundle JSON 路径；不传时从 snapshot.trace 推导。"),
    ] = None,
    features_path: Annotated[
        Path | None,
        typer.Option(help="特征快照 CSV 路径；不传时从 trace dataset_refs 推导。"),
    ] = None,
    data_quality_report_path: Annotated[
        Path | None,
        typer.Option(help="数据质量报告路径；不传时从 trace quality_refs 推导。"),
    ] = None,
    prediction_ledger_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "append-only parameter shadow prediction ledger CSV 输出路径；"
                "默认写入隔离 flow validation ledger，不写正式 production ledger。"
            )
        ),
    ] = None,
    candidate_ids: Annotated[
        str | None,
        typer.Option(help="可选：逗号分隔的 candidate_id 白名单。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="shadow 运行日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown parameter shadow runner 报告输出路径。"),
    ] = None,
) -> None:
    """把参数候选追加到 prediction ledger，production_effect=none。"""
    run_date = _parse_date(as_of) if as_of else date.today()
    snapshot_path = decision_snapshot_path or default_decision_snapshot_path(
        DEFAULT_DECISION_SNAPSHOT_DIR,
        run_date,
    )
    if not snapshot_path.exists():
        raise typer.BadParameter(f"decision_snapshot 不存在：{snapshot_path}")
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    trace_path = trace_bundle_path or _path_from_snapshot_trace(snapshot)
    if trace_path is None or not trace_path.exists():
        raise typer.BadParameter(f"trace bundle 不存在：{trace_path}")
    trace_bundle = json.loads(trace_path.read_text(encoding="utf-8"))
    feature_snapshot_path = features_path or _trace_dataset_path(
        trace_bundle,
        "processed_feature_cache",
    )
    if feature_snapshot_path is None:
        raise typer.BadParameter("无法从 trace bundle 推导 feature snapshot 路径。")
    quality_path = data_quality_report_path or _trace_quality_report_path(trace_bundle)
    if quality_path is None:
        raise typer.BadParameter("无法从 trace bundle 推导 data quality report 路径。")
    try:
        parameter_candidate_ledger = load_parameter_candidate_ledger(
            parameter_candidate_ledger_path
        )
    except FileNotFoundError as exc:
        raise typer.BadParameter(
            f"parameter candidate ledger 不存在：{parameter_candidate_ledger_path}"
        ) from exc
    selected = tuple(_parse_csv_items(candidate_ids)) if candidate_ids else ()
    records = build_parameter_shadow_prediction_records(
        snapshot=snapshot,
        trace_bundle=trace_bundle,
        trace_bundle_path=trace_path,
        features_path=feature_snapshot_path,
        data_quality_report_path=quality_path,
        parameter_candidate_ledger=parameter_candidate_ledger,
        selected_candidate_ids=selected,
    )
    parameter_shadow_ledger_path = (
        prediction_ledger_path or DEFAULT_PARAMETER_SHADOW_PREDICTION_LEDGER_PATH
    )
    ledger_output = append_prediction_records(records, parameter_shadow_ledger_path)
    warnings: list[str] = []
    if not records:
        warnings.append(
            "没有可运行的 parameter challenger；请检查 recommendation_status 和门禁模式。"
        )
    if parameter_candidate_ledger.get("evaluation_mode") == "flow_validation":
        warnings.append(
            "本次 parameter shadow 来自 flow_validation candidate ledger；"
            "仅用于接线验证，不得作为生产证据。"
        )
    report = build_shadow_prediction_run_report(
        as_of=run_date,
        decision_snapshot_path=snapshot_path,
        trace_bundle_path=trace_path,
        rule_experiment_path=parameter_candidate_ledger_path,
        prediction_ledger_path=ledger_output,
        records=records,
        candidate_count=len(parameter_candidate_ledger.get("candidates", [])),
        warnings=tuple(warnings),
        source_label="Parameter candidate ledger",
    )
    output_report = report_path or (
        PROJECT_ROOT
        / "outputs"
        / "reports"
        / f"parameter_shadow_predictions_{run_date.isoformat()}.md"
    )
    output_report = write_shadow_prediction_run_report(report, output_report)
    style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{style}]Parameter shadow runner 状态：{report.status}[/{style}]")
    console.print(f"写入 prediction：{report.appended_count}")
    console.print(f"Prediction ledger：{ledger_output}")
    console.print(f"Parameter shadow runner 报告：{output_report}")


@feedback_app.command("run-shadow-weight-profiles")
def run_shadow_weight_profiles_command(
    manifest_path: Annotated[
        Path,
        typer.Option(help="shadow weight profile manifest YAML 路径。"),
    ] = DEFAULT_SHADOW_WEIGHT_PROFILE_MANIFEST_PATH,
    gate_manifest_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "可选：shadow position gate profile manifest YAML 路径；"
                "提供后与 weight profile 组合观察 gate cap 参数。"
            )
        ),
    ] = DEFAULT_SHADOW_POSITION_GATE_PROFILE_MANIFEST_PATH,
    decision_snapshot_path: Annotated[
        Path | None,
        typer.Option(help="production decision_snapshot JSON 路径；不传时按 as-of 推导。"),
    ] = None,
    trace_bundle_path: Annotated[
        Path | None,
        typer.Option(help="可选：写 prediction ledger 时使用的 trace bundle JSON 路径。"),
    ] = None,
    features_path: Annotated[
        Path | None,
        typer.Option(help="可选：写 prediction ledger 时使用的 feature snapshot CSV 路径。"),
    ] = None,
    data_quality_report_path: Annotated[
        Path | None,
        typer.Option(help="可选：写 prediction ledger 时使用的数据质量报告路径。"),
    ] = None,
    observation_ledger_path: Annotated[
        Path,
        typer.Option(help="独立 shadow weight observation ledger CSV 输出路径。"),
    ] = DEFAULT_SHADOW_WEIGHT_PROFILE_OBSERVATION_LEDGER_PATH,
    prediction_ledger_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "可选：隔离 prediction ledger CSV 路径；提供后才追加 "
                "production_effect=none 的 shadow prediction。"
            )
        ),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="观察日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown shadow weight profile 报告输出路径。"),
    ] = None,
) -> None:
    """运行隔离 shadow weight profile 与主线评分对比，production_effect=none。"""
    run_date = _parse_date(as_of) if as_of else date.today()
    snapshot_path = decision_snapshot_path or default_decision_snapshot_path(
        DEFAULT_DECISION_SNAPSHOT_DIR,
        run_date,
    )
    if not snapshot_path.exists():
        raise typer.BadParameter(f"decision_snapshot 不存在：{snapshot_path}")
    try:
        report = build_shadow_weight_profile_run_report(
            as_of=run_date,
            decision_snapshot_path=snapshot_path,
            manifest_path=manifest_path,
            gate_manifest_path=gate_manifest_path,
            observation_ledger_path=observation_ledger_path,
            prediction_ledger_path=prediction_ledger_path,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]Shadow weight profile 运行失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    observation_output = write_shadow_weight_observation_ledger(
        report,
        observation_ledger_path,
    )
    appended_predictions = 0
    prediction_output: Path | None = None
    if prediction_ledger_path is not None:
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        trace_path = trace_bundle_path or _path_from_snapshot_trace(snapshot)
        if trace_path is None or not trace_path.exists():
            raise typer.BadParameter(f"trace bundle 不存在：{trace_path}")
        trace_bundle = json.loads(trace_path.read_text(encoding="utf-8"))
        feature_snapshot_path = features_path or _trace_dataset_path(
            trace_bundle,
            "processed_feature_cache",
        )
        if feature_snapshot_path is None:
            raise typer.BadParameter("无法从 trace bundle 推导 feature snapshot 路径。")
        quality_path = data_quality_report_path or _trace_quality_report_path(trace_bundle)
        if quality_path is None:
            raise typer.BadParameter("无法从 trace bundle 推导 data quality report 路径。")
        records = build_shadow_weight_prediction_records(
            report,
            snapshot=snapshot,
            trace_bundle=trace_bundle,
            trace_bundle_path=trace_path,
            features_path=feature_snapshot_path,
            data_quality_report_path=quality_path,
        )
        existing_prediction_ids = {
            str(row.get("prediction_id") or "")
            for row in load_prediction_ledger(prediction_ledger_path)
        }
        new_records = tuple(
            record
            for record in records
            if str(record.get("prediction_id") or "") not in existing_prediction_ids
        )
        prediction_output = append_prediction_records(new_records, prediction_ledger_path)
        appended_predictions = len(new_records)
    output_report = report_path or default_shadow_weight_profile_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        run_date,
    )
    output_report = write_shadow_weight_profile_report(report, output_report)
    style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{style}]Shadow weight profile 状态：{report.status}[/{style}]")
    console.print(f"Profile 数：{len(report.observations)}")
    console.print(f"Observation ledger：{observation_output}")
    if prediction_output is not None:
        console.print(f"Shadow prediction ledger：{prediction_output}")
        console.print(f"写入 shadow prediction：{appended_predictions}")
    console.print(f"报告：{output_report}")
    console.print("治理边界：本命令不修改生产权重、approved overlay、日报结论或仓位 gate。")


@feedback_app.command("evaluate-shadow-weight-performance")
def evaluate_shadow_weight_performance_command(
    observation_ledger_path: Annotated[
        Path,
        typer.Option(help="shadow weight observation ledger CSV 路径。"),
    ] = DEFAULT_SHADOW_WEIGHT_PROFILE_OBSERVATION_LEDGER_PATH,
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="评估截止日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(help="评估起始日期，格式为 YYYY-MM-DD；默认读取全部 observation。"),
    ] = None,
    strategy_ticker: Annotated[
        str,
        typer.Option(help="AI proxy 或策略代理标的。"),
    ] = "SMH",
    horizon_days: Annotated[
        int,
        typer.Option(help="非重叠表现评估观察窗口，单位为交易日。"),
    ] = 1,
    cost_bps: Annotated[
        float,
        typer.Option(help="单边交易成本 bps，用于 position-weighted 验证。"),
    ] = 5.0,
    slippage_bps: Annotated[
        float,
        typer.Option(help="线性滑点 bps，用于 position-weighted 验证。"),
    ] = 0.0,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown shadow weight performance 报告输出路径。"),
    ] = None,
    csv_output_path: Annotated[
        Path | None,
        typer.Option(help="机器可读 shadow weight performance CSV 输出路径。"),
    ] = None,
) -> None:
    """比较 shadow 权重与主线 gate 后仓位的 position-weighted 表现。"""
    evaluation_date = _parse_date(as_of) if as_of else date.today()
    since_date = _parse_date(since) if since else None
    try:
        report = build_shadow_weight_performance_report(
            as_of=evaluation_date,
            since=since_date,
            observation_ledger_path=observation_ledger_path,
            prices_path=prices_path,
            strategy_ticker=strategy_ticker,
            horizon_days=horizon_days,
            cost_bps=cost_bps,
            slippage_bps=slippage_bps,
        )
    except (OSError, ValueError) as exc:
        console.print(f"[red]Shadow weight performance 评估失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    csv_path = csv_output_path or default_shadow_weight_performance_csv_path(
        PROJECT_ROOT / "outputs" / "reports",
        evaluation_date,
    )
    csv_path = write_shadow_weight_performance_csv(report, csv_path)
    report_path = output_path or default_shadow_weight_performance_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        evaluation_date,
    )
    report_path = write_shadow_weight_performance_report(
        report,
        report_path,
        csv_path=csv_path,
    )
    style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{style}]Shadow weight performance 状态：{report.status}[/{style}]")
    if report.best_positive_profile is not None:
        best = report.best_positive_profile
        console.print(
            "Return-leading profile：" f"{best.profile_id}（excess={best.excess_total_return:.2%}）"
        )
    elif report.best_profile is not None:
        console.print("当前没有正向 excess 的 shadow weight profile。")
    console.print(f"Performance CSV：{csv_path}")
    console.print(f"Performance 报告：{report_path}")
    console.print("治理边界：本命令不修改生产权重、approved overlay、日报结论或仓位 gate。")


@feedback_app.command("search-shadow-parameters")
def search_shadow_parameters_command(
    from_date: Annotated[
        str,
        typer.Option("--from", help="搜索起始日期，格式为 YYYY-MM-DD。"),
    ],
    to_date: Annotated[
        str,
        typer.Option("--to", help="搜索截止日期，格式为 YYYY-MM-DD。"),
    ],
    decision_snapshot_path: Annotated[
        Path,
        typer.Option(help="decision_snapshot JSON 文件或目录路径。"),
    ] = DEFAULT_DECISION_SNAPSHOT_SEARCH_DIR,
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    search_space_path: Annotated[
        Path,
        typer.Option(help="shadow 参数搜索空间 YAML 路径。"),
    ] = DEFAULT_SHADOW_PARAMETER_SEARCH_SPACE_PATH,
    objective_path: Annotated[
        Path,
        typer.Option(help="shadow 参数目标函数 YAML 路径。"),
    ] = DEFAULT_SHADOW_PARAMETER_OBJECTIVE_PATH,
    output_root: Annotated[
        Path,
        typer.Option(help="参数搜索输出根目录。"),
    ] = DEFAULT_SHADOW_PARAMETER_SEARCH_OUTPUT_ROOT,
    run_id: Annotated[
        str | None,
        typer.Option(help="搜索 run id；不传则按日期和 UTC 时间生成。"),
    ] = None,
    strategy_ticker: Annotated[
        str,
        typer.Option(help="AI proxy 或策略代理标的。"),
    ] = "SMH",
    horizon_days: Annotated[
        int,
        typer.Option(help="表现评估观察窗口，单位为交易日。"),
    ] = 1,
    cost_bps: Annotated[
        float,
        typer.Option(help="单边交易成本 bps。"),
    ] = 5.0,
    slippage_bps: Annotated[
        float,
        typer.Option(help="线性滑点 bps。"),
    ] = 0.0,
    max_trials: Annotated[
        int | None,
        typer.Option(help="可选：最多评估 trial 数，用于快速 smoke。"),
    ] = None,
) -> None:
    """按指定区间搜索 validation-only shadow weight/gate 参数候选。"""
    start = _parse_date(from_date)
    end = _parse_date(to_date)
    effective_run_id = run_id or (
        f"shadow_parameter_search_{start.isoformat()}_{end.isoformat()}_"
        f"{datetime.now(tz=UTC).strftime('%Y%m%dT%H%M%SZ')}"
    )
    output_dir = output_root / effective_run_id
    try:
        report = build_shadow_parameter_search_report(
            run_id=effective_run_id,
            start=start,
            end=end,
            decision_snapshot_path=decision_snapshot_path,
            prices_path=prices_path,
            search_space_path=search_space_path,
            objective_path=objective_path,
            output_dir=output_dir,
            strategy_ticker=strategy_ticker,
            horizon_days=horizon_days,
            cost_bps=cost_bps,
            slippage_bps=slippage_bps,
            max_trials=max_trials,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]Shadow parameter search 失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    paths = write_shadow_parameter_search_bundle(report)
    style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{style}]Shadow parameter search 状态：{report.status}[/{style}]")
    console.print(f"Trial 数：{len(report.trials)}")
    console.print(f"Pareto front：{len(report.pareto_front)}")
    if report.best_trial is not None:
        best = report.best_trial
        console.print(
            "Best trial："
            f"{best.trial_id}（objective={best.objective_score:.4f}, "
            f"excess={best.excess_total_return:.2%}）"
        )
    else:
        console.print("当前没有 eligible best trial。")
        if report.best_diagnostic_trial is not None:
            diagnostic = report.best_diagnostic_trial
            console.print(
                "Diagnostic-leading trial："
                f"{diagnostic.trial_id}（not eligible: "
                f"{diagnostic.ineligibility_reason or 'not_eligible'}）"
            )
    if report.factorial_attribution is not None:
        console.print("Factorial primary driver：" f"{report.factorial_attribution.primary_driver}")
    console.print(f"输出目录：{paths['output_dir']}")
    console.print(f"搜索报告：{paths['search_report']}")
    console.print(f"Best profile YAML：{paths['best_profiles_yaml']}")
    console.print("治理边界：本命令不修改生产权重、approved overlay、日报结论或仓位 gate。")


@feedback_app.command("evaluate-shadow-parameter-promotion")
def evaluate_shadow_parameter_promotion_command(
    search_output_dir: Annotated[
        Path,
        typer.Option(help="search-shadow-parameters 输出目录，包含 manifest.json 和 trials.csv。"),
    ],
    contract_path: Annotated[
        Path,
        typer.Option(help="shadow parameter promotion contract YAML 路径。"),
    ] = DEFAULT_SHADOW_PARAMETER_PROMOTION_CONTRACT_PATH,
    output_path: Annotated[
        Path | None,
        typer.Option(help="promotion contract 报告输出路径；不传则写入 search output 目录。"),
    ] = None,
) -> None:
    """用独立 contract 检查 shadow parameter search 是否只能进入后续 shadow。"""
    try:
        report = build_shadow_parameter_promotion_report(
            search_output_dir=search_output_dir,
            contract_path=contract_path,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]Shadow parameter promotion 评估失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    run_id = str(report.search_manifest.get("run_id") or search_output_dir.name)
    destination = output_path or default_shadow_parameter_promotion_report_path(
        search_output_dir,
        run_id,
    )
    written = write_shadow_parameter_promotion_report(report, destination)
    style = "green" if report.status == "READY_FOR_OWNER_REVIEW" else "yellow"
    console.print(f"[{style}]Shadow parameter promotion 状态：{report.status}[/{style}]")
    console.print(f"Selected trial：{report.selected_trial_id or 'none'}")
    console.print(f"Contract checks：{len(report.checks)}")
    console.print(f"Promotion 报告：{written}")
    console.print(f"Promotion JSON：{written.with_suffix('.json')}")
    console.print("治理边界：本命令只评估 contract，不修改 production 参数或 ledger。")


@feedback_app.command("run-shadow-iteration")
def run_shadow_iteration_command(
    as_of: Annotated[
        str,
        typer.Option(help="shadow iteration 运行日期，格式为 YYYY-MM-DD。"),
    ],
    search_output_dir: Annotated[
        Path | None,
        typer.Option(help="可选：search-shadow-parameters 输出目录。"),
    ] = None,
    registry_path: Annotated[
        Path,
        typer.Option(
            hidden=True,
            help="测试/隔离用：shadow iteration registry CSV 输出路径。",
        ),
    ] = DEFAULT_SHADOW_ITERATION_REGISTRY_PATH,
    reports_dir: Annotated[
        Path,
        typer.Option(hidden=True, help="测试/隔离用：shadow iteration 报告目录。"),
    ] = DEFAULT_SHADOW_ITERATION_REPORT_DIR,
    run_output_root: Annotated[
        Path,
        typer.Option(hidden=True, help="测试/隔离用：shadow iteration run 输出根目录。"),
    ] = DEFAULT_SHADOW_ITERATION_RUN_ROOT,
    contract_path: Annotated[
        Path,
        typer.Option(
            hidden=True,
            help="测试/隔离用：shadow parameter promotion contract YAML 路径。",
        ),
    ] = DEFAULT_SHADOW_PARAMETER_PROMOTION_CONTRACT_PATH,
) -> None:
    """读取既有 search output，维护 shadow iteration registry 和只读报告。"""
    run_date = _parse_date(as_of)
    try:
        report = build_shadow_iteration_report(
            as_of=run_date,
            search_output_dir=search_output_dir,
            registry_path=registry_path,
            reports_dir=reports_dir,
            run_output_root=run_output_root,
            contract_path=contract_path,
        )
        paths = write_shadow_iteration_outputs(report)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]Shadow iteration 运行失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{style}]Shadow iteration 状态：{report.status}[/{style}]")
    console.print(f"Source search run：{report.source_search_run_id}")
    console.print(f"Active candidates：{len(report.active_candidates)}")
    if report.best_weight_only is not None:
        console.print(f"Best weight-only：{report.best_weight_only.trial_id}")
    if report.best_gate_only is not None:
        console.print(f"Best gate-only：{report.best_gate_only.trial_id}")
    if report.best_weight_gate_bundle is not None:
        console.print(f"Best bundle：{report.best_weight_gate_bundle.trial_id}")
    console.print(f"Registry：{paths['registry']}")
    console.print(f"报告：{paths['markdown_report']}")
    console.print(f"JSON：{paths['json_report']}")
    console.print("治理边界：本命令不修改 production 权重、gate、approved overlay 或正式 ledger。")


@feedback_app.command("register-forward-shadow")
def register_forward_shadow_command(
    iteration_id: Annotated[
        str,
        typer.Option(help="shadow iteration registry 中的 iteration_id。"),
    ],
    candidate_id: Annotated[
        str,
        typer.Option(help="registry trial_id；也可传同一个 iteration_id 作一致性校验。"),
    ],
    as_of: Annotated[
        str | None,
        typer.Option(help="登记日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    registry_path: Annotated[
        Path,
        typer.Option(
            hidden=True,
            help="测试/隔离用：shadow iteration registry CSV 路径。",
        ),
    ] = DEFAULT_SHADOW_ITERATION_REGISTRY_PATH,
) -> None:
    """仅把 shadow iteration registry 中的候选标记为持续 forward shadow。"""
    run_date = _parse_date(as_of) if as_of else date.today()
    try:
        row = register_forward_shadow_candidate(
            registry_path=registry_path,
            iteration_id=iteration_id,
            candidate_id=candidate_id,
            as_of=run_date,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]Forward shadow 登记失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    console.print("[green]Forward shadow 登记完成[/green]")
    console.print(f"Iteration：{row.get('iteration_id')}")
    console.print(f"Trial：{row.get('trial_id')}")
    console.print(f"Status：{row.get('status')}")
    console.print(f"Registry：{registry_path}")
    console.print("治理边界：本命令只更新 shadow_iteration_registry.csv，不修改 production 配置。")


@feedback_app.command("evaluate-forward-shadow")
def evaluate_forward_shadow_command(
    as_of: Annotated[
        str,
        typer.Option(help="forward shadow 评估日期，格式为 YYYY-MM-DD。"),
    ],
    registry_path: Annotated[
        Path,
        typer.Option(
            hidden=True,
            help="测试/隔离用：shadow iteration registry CSV 路径。",
        ),
    ] = DEFAULT_SHADOW_ITERATION_REGISTRY_PATH,
    reports_dir: Annotated[
        Path,
        typer.Option(hidden=True, help="测试/隔离用：forward shadow 评估报告目录。"),
    ] = DEFAULT_SHADOW_ITERATION_REPORT_DIR,
) -> None:
    """每日评估 active forward shadow 候选并更新只读 lifecycle 状态。"""
    run_date = _parse_date(as_of)
    try:
        report = build_forward_shadow_evaluation_report(
            as_of=run_date,
            registry_path=registry_path,
            reports_dir=reports_dir,
        )
        paths = write_forward_shadow_evaluation_outputs(report)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]Forward shadow 评估失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{style}]Forward shadow 评估状态：{report.status}[/{style}]")
    console.print(f"Evaluated candidates：{len(report.evaluations)}")
    console.print(
        "Actions："
        + ", ".join(f"{action}={count}" for action, count in report.action_counts.items())
    )
    console.print(f"Registry：{paths['registry']}")
    console.print(f"报告：{paths['markdown_report']}")
    console.print(f"JSON：{paths['json_report']}")
    console.print("治理边界：本命令不修改 production 权重、gate、approved overlay 或正式 ledger。")


@feedback_app.command("shadow-maturity")
def shadow_maturity_command(
    prediction_outcomes_path: Annotated[
        Path,
        typer.Option(help="prediction_outcomes CSV 路径。"),
    ] = DEFAULT_PREDICTION_OUTCOMES_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="成熟度评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    min_available_samples: Annotated[
        int | None,
        typer.Option(
            help=(
                "进入 owner/rule card 审批前所需最低可用 outcome 样本数；"
                "promotion mode 默认读取 prediction promotion floor；"
                "validation mode 默认读取 prediction pilot floor。"
            )
        ),
    ] = None,
    review_mode: Annotated[
        str,
        typer.Option(
            help=(
                "成熟度复核模式：promotion 使用生产晋级门槛；validation 只启动"
                "后续验证复核，不允许 production 晋级。"
            )
        ),
    ] = "promotion",
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown shadow maturity 报告输出路径。"),
    ] = None,
) -> None:
    """按 candidate/horizon 汇总 forward shadow outcome 样本成熟度。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    if review_mode not in {"promotion", "validation"}:
        raise typer.BadParameter("review_mode 必须是 promotion 或 validation")
    parsed_review_mode = cast(Literal["promotion", "validation"], review_mode)
    rows = load_prediction_outcomes(prediction_outcomes_path)
    report = build_shadow_maturity_report(
        outcome_rows=rows,
        outcomes_path=prediction_outcomes_path,
        as_of=report_date,
        min_available_samples=min_available_samples,
        review_mode=parsed_review_mode,
    )
    report_output = output_path or default_shadow_maturity_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        report_date,
    )
    report_output = write_shadow_maturity_report(report, report_output)
    style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{style}]Shadow 样本成熟度：{report.status}[/{style}]")
    console.print(f"Review mode：{report.review_mode}")
    console.print(f"分组数：{len(report.groups)}")
    console.print(f"报告：{report_output}")


@feedback_app.command("build-causal-chain")
def build_decision_causal_chains_command(
    decision_snapshot_path: Annotated[
        Path,
        typer.Option(help="decision_snapshot JSON 文件或目录路径。"),
    ] = DEFAULT_DECISION_SNAPSHOT_DIR,
    outcomes_path: Annotated[
        Path,
        typer.Option(help="decision_outcomes CSV 路径；不存在时只生成 signal-time 链条。"),
    ] = DEFAULT_DECISION_OUTCOMES_PATH,
    output_path: Annotated[
        Path,
        typer.Option(help="decision_causal_chain ledger JSON 输出路径。"),
    ] = DEFAULT_DECISION_CAUSAL_CHAIN_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 因果链报告输出路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="报告日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
) -> None:
    """构建 decision causal chain ledger。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    snapshots = load_decision_snapshots(decision_snapshot_path)
    if not snapshots:
        raise typer.BadParameter(f"未找到 decision_snapshot：{decision_snapshot_path}")
    outcomes = load_decision_outcomes_frame(outcomes_path)
    ledger = build_decision_causal_chain_ledger(
        snapshots=snapshots,
        outcomes=outcomes,
    )
    ledger_output = write_decision_causal_chain_ledger(ledger, output_path)
    report_output = report_path or default_decision_causal_chain_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        report_date,
    )
    report_output = write_decision_causal_chain_report(
        ledger,
        ledger_path=ledger_output,
        output_path=report_output,
    )

    console.print("[green]决策因果链已生成。[/green]")
    console.print(f"链条数：{ledger.chain_count}")
    console.print(f"Ledger：{ledger_output}")
    console.print(f"报告：{report_output}")


@feedback_app.command("lookup-chain")
def lookup_decision_causal_chain_command(
    chain_id: Annotated[
        str,
        typer.Option("--id", help="decision causal chain id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="decision_causal_chain ledger JSON 路径。"),
    ] = DEFAULT_DECISION_CAUSAL_CHAIN_PATH,
) -> None:
    """按 chain_id 反查 decision causal chain。"""
    try:
        chain = lookup_decision_causal_chain(input_path, chain_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"decision causal chain ledger 不存在：{input_path}") from exc
    except KeyError as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_decision_causal_chain_lookup(chain))


@feedback_app.command("build-learning-queue")
def build_decision_learning_queue_command(
    causal_chain_path: Annotated[
        Path,
        typer.Option(help="decision_causal_chain ledger JSON 输入路径。"),
    ] = DEFAULT_DECISION_CAUSAL_CHAIN_PATH,
    output_path: Annotated[
        Path,
        typer.Option(help="decision learning queue JSON 输出路径。"),
    ] = DEFAULT_DECISION_LEARNING_QUEUE_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 学习队列报告输出路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="报告日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    min_available_windows: Annotated[
        int,
        typer.Option(help="形成非 sample_limited 归因所需的最少可用 outcome 窗口。"),
    ] = 1,
) -> None:
    """从 decision causal chain 生成学习复核队列。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    try:
        causal_ledger = load_decision_causal_chain_ledger(causal_chain_path)
    except FileNotFoundError as exc:
        raise typer.BadParameter(
            f"decision causal chain ledger 不存在：{causal_chain_path}"
        ) from exc
    ledger = build_decision_learning_queue(
        chains=tuple(causal_ledger.get("chains", [])),
        min_available_windows=min_available_windows,
    )
    queue_output = write_decision_learning_queue(ledger, output_path)
    report_output = report_path or default_decision_learning_queue_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        report_date,
    )
    report_output = write_decision_learning_queue_report(
        ledger,
        ledger_path=queue_output,
        output_path=report_output,
    )

    console.print("[green]决策学习队列已生成。[/green]")
    console.print(f"复核项数：{ledger.item_count}")
    console.print(f"Queue：{queue_output}")
    console.print(f"报告：{report_output}")


@feedback_app.command("lookup-learning")
def lookup_decision_learning_queue_command(
    review_id: Annotated[
        str,
        typer.Option("--id", help="learning review id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="decision learning queue JSON 路径。"),
    ] = DEFAULT_DECISION_LEARNING_QUEUE_PATH,
) -> None:
    """按 review_id 反查 learning queue 项。"""
    try:
        item = lookup_decision_learning_item(input_path, review_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"decision learning queue 不存在：{input_path}") from exc
    except KeyError as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_decision_learning_item_lookup(item))


@feedback_app.command("build-rule-experiments")
def build_rule_experiments_command(
    learning_queue_path: Annotated[
        Path,
        typer.Option(help="decision learning queue JSON 输入路径。"),
    ] = DEFAULT_DECISION_LEARNING_QUEUE_PATH,
    output_path: Annotated[
        Path,
        typer.Option(help="rule experiment ledger JSON 输出路径。"),
    ] = DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 规则实验台账报告输出路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="报告日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    replay_start: Annotated[
        str,
        typer.Option(help="历史 replay 计划起点，格式为 YYYY-MM-DD。"),
    ] = "2021-02-22",
    replay_end: Annotated[
        str | None,
        typer.Option(help="历史 replay 计划终点，格式为 YYYY-MM-DD，默认 as-of。"),
    ] = None,
    shadow_start: Annotated[
        str | None,
        typer.Option(help="前向 shadow 计划起点，格式为 YYYY-MM-DD，默认 as-of。"),
    ] = None,
    shadow_days: Annotated[
        int,
        typer.Option(help="前向 shadow 最少观察天数。"),
    ] = 20,
) -> None:
    """从 learning queue 生成候选规则实验台账。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    try:
        learning_ledger = load_decision_learning_queue(learning_queue_path)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"decision learning queue 不存在：{learning_queue_path}") from exc
    replay_start_date = _parse_date(replay_start)
    replay_end_date = _parse_date(replay_end) if replay_end else report_date
    shadow_start_date = _parse_date(shadow_start) if shadow_start else report_date
    ledger = build_rule_experiment_ledger(
        learning_items=tuple(learning_ledger.get("items", [])),
        replay_start=replay_start_date,
        replay_end=replay_end_date,
        shadow_start=shadow_start_date,
        shadow_days=shadow_days,
    )
    ledger_output = write_rule_experiment_ledger(ledger, output_path)
    report_output = report_path or default_rule_experiment_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        report_date,
    )
    report_output = write_rule_experiment_report(
        ledger,
        ledger_path=ledger_output,
        output_path=report_output,
    )

    console.print("[green]候选规则实验台账已生成。[/green]")
    console.print(f"候选规则数：{ledger.candidate_count}")
    console.print(f"Ledger：{ledger_output}")
    console.print(f"报告：{report_output}")


@feedback_app.command("lookup-rule-experiment")
def lookup_rule_experiment_command(
    candidate_id: Annotated[
        str,
        typer.Option("--id", help="rule experiment candidate id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="rule experiment ledger JSON 路径。"),
    ] = DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
) -> None:
    """按 candidate_id 反查候选规则实验。"""
    try:
        candidate = lookup_rule_experiment(input_path, candidate_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"rule experiment ledger 不存在：{input_path}") from exc
    except KeyError as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_rule_experiment_lookup(candidate))


@feedback_app.command("validate-rule-cards")
def validate_rule_cards_command(
    input_path: Annotated[
        Path,
        typer.Option(help="rule cards YAML 路径。"),
    ] = DEFAULT_RULE_CARDS_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 规则治理校验报告输出路径。"),
    ] = None,
) -> None:
    """校验 production / candidate / retired rule card registry。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report = validate_rule_card_store(
        load_rule_card_store(input_path),
        as_of=validation_date,
    )
    report_path = output_path or default_rule_governance_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    write_rule_governance_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]规则治理状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"Rule cards：{report.card_count}；"
        f"Production：{report.production_count}；Candidate：{report.candidate_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@feedback_app.command("lookup-rule-card")
def lookup_rule_card_command(
    rule_id: Annotated[
        str,
        typer.Option("--id", help="rule card id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="rule cards YAML 路径。"),
    ] = DEFAULT_RULE_CARDS_PATH,
) -> None:
    """按 rule_id 反查 rule card。"""
    try:
        card = lookup_rule_card(input_path, rule_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"rule card registry 不存在：{input_path}") from exc
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_rule_card_lookup(card))


@feedback_app.command("promote-rule-card")
def promote_rule_card_command(
    rule_id: Annotated[
        str,
        typer.Option("--id", help="candidate rule card id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="rule cards YAML 输入路径。"),
    ] = DEFAULT_RULE_CARDS_PATH,
    output_path: Annotated[
        Path | None,
        typer.Option(help="rule cards YAML 输出路径；不传则覆盖输入路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="批准日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    approved_by: Annotated[
        str,
        typer.Option(help="owner / reviewer 标识。"),
    ] = "",
    approval_rationale: Annotated[
        str,
        typer.Option(help="owner 批准理由。"),
    ] = "",
    promotion_report_ref: Annotated[
        str,
        typer.Option(help="model promotion report 或等价审计引用。"),
    ] = "",
    outcome_refs: Annotated[
        str,
        typer.Option(help="逗号分隔的 prediction outcome / shadow maturity 引用。"),
    ] = "",
    production_since: Annotated[
        str | None,
        typer.Option(help="production rule 生效日期，格式为 YYYY-MM-DD；默认 as-of。"),
    ] = None,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 生命周期操作报告输出路径。"),
    ] = None,
) -> None:
    """把已批准 candidate rule card 受控升级为 production。"""
    promotion_date = _parse_date(as_of) if as_of else date.today()
    output = output_path or input_path
    try:
        report = promote_rule_card(
            input_path=input_path,
            output_path=output,
            rule_id=rule_id,
            as_of=promotion_date,
            approved_by=approved_by,
            approval_rationale=approval_rationale,
            promotion_report_ref=promotion_report_ref,
            outcome_refs=tuple(_parse_csv_items(outcome_refs)),
            production_since=_parse_date(production_since) if production_since else None,
        )
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    lifecycle_report_path = report_path or (
        PROJECT_ROOT
        / "outputs"
        / "reports"
        / f"rule_lifecycle_promote_{promotion_date.isoformat()}.md"
    )
    lifecycle_report_path = write_rule_lifecycle_action_report(
        report,
        lifecycle_report_path,
    )
    style = (
        "green"
        if report.status == "PASS"
        else "yellow" if report.validation_report.passed else "red"
    )
    console.print(f"[{style}]Rule promotion 状态：{report.status}[/{style}]")
    console.print(f"Rule cards：{report.output_path}")
    console.print(f"操作报告：{lifecycle_report_path}")
    if not report.validation_report.passed:
        raise typer.Exit(code=1)


@feedback_app.command("retire-rule-card")
def retire_rule_card_command(
    rule_id: Annotated[
        str,
        typer.Option("--id", help="production rule card id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="rule cards YAML 输入路径。"),
    ] = DEFAULT_RULE_CARDS_PATH,
    output_path: Annotated[
        Path | None,
        typer.Option(help="rule cards YAML 输出路径；不传则覆盖输入路径。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="操作日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    retired_at: Annotated[
        str | None,
        typer.Option(help="退役生效日期，格式为 YYYY-MM-DD；默认 as-of。"),
    ] = None,
    reason: Annotated[
        str,
        typer.Option(help="退役原因。"),
    ] = "",
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 生命周期操作报告输出路径。"),
    ] = None,
) -> None:
    """把 production rule card 标记为 retired。"""
    action_date = _parse_date(as_of) if as_of else date.today()
    output = output_path or input_path
    try:
        report = retire_rule_card(
            input_path=input_path,
            output_path=output,
            rule_id=rule_id,
            as_of=action_date,
            retired_at=_parse_date(retired_at) if retired_at else None,
            reason=reason,
        )
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    lifecycle_report_path = report_path or (
        PROJECT_ROOT / "outputs" / "reports" / f"rule_lifecycle_retire_{action_date.isoformat()}.md"
    )
    lifecycle_report_path = write_rule_lifecycle_action_report(
        report,
        lifecycle_report_path,
    )
    style = (
        "green"
        if report.status == "PASS"
        else "yellow" if report.validation_report.passed else "red"
    )
    console.print(f"[{style}]Rule retirement 状态：{report.status}[/{style}]")
    console.print(f"Rule cards：{report.output_path}")
    console.print(f"操作报告：{lifecycle_report_path}")
    if not report.validation_report.passed:
        raise typer.Exit(code=1)


@feedback_app.command("validate-benchmark-policy")
def validate_benchmark_policy_command(
    input_path: Annotated[
        Path,
        typer.Option(help="benchmark policy YAML 路径。"),
    ] = DEFAULT_BENCHMARK_POLICY_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    strategy_ticker: Annotated[
        str | None,
        typer.Option(help="可选：本次回测或校准使用的 AI proxy / strategy ticker。"),
    ] = None,
    benchmarks: Annotated[
        str | None,
        typer.Option(help="可选：逗号分隔的本次对比基准 ticker。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown benchmark policy 校验报告输出路径。"),
    ] = None,
) -> None:
    """校验 AI proxy 与 benchmark policy registry。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    benchmark_tickers = tuple(_parse_csv_items(benchmarks)) if benchmarks is not None else None
    report = validate_benchmark_policy(
        load_benchmark_policy(input_path),
        as_of=validation_date,
        selected_strategy_ticker=strategy_ticker,
        selected_benchmark_tickers=benchmark_tickers,
    )
    report_path = output_path or default_benchmark_policy_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    write_benchmark_policy_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]基准政策状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"Benchmark：{report.instrument_count}；" f"Custom AI basket：{report.custom_basket_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@feedback_app.command("lookup-benchmark-policy")
def lookup_benchmark_policy_command(
    entry_id: Annotated[
        str,
        typer.Option("--id", help="benchmark id、ticker 或 custom basket id。"),
    ],
    input_path: Annotated[
        Path,
        typer.Option(help="benchmark policy YAML 路径。"),
    ] = DEFAULT_BENCHMARK_POLICY_CONFIG_PATH,
) -> None:
    """按 ticker、benchmark id 或 basket id 反查 benchmark policy 条目。"""
    try:
        entry = lookup_benchmark_policy_entry(input_path, entry_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"benchmark policy 不存在：{input_path}") from exc
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_benchmark_policy_lookup(entry))


@feedback_app.command("loop-review")
def feedback_loop_review_command(
    evidence_path: Annotated[
        Path,
        typer.Option(help="market evidence YAML 文件或目录路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "market_evidence",
    decision_snapshot_path: Annotated[
        Path,
        typer.Option(help="decision_snapshot JSON 文件或目录路径。"),
    ] = DEFAULT_DECISION_SNAPSHOT_DIR,
    outcomes_path: Annotated[
        Path,
        typer.Option(help="decision_outcomes CSV 路径。"),
    ] = DEFAULT_DECISION_OUTCOMES_PATH,
    prediction_outcomes_path: Annotated[
        Path,
        typer.Option(help="prediction_outcomes CSV 路径，用于 production vs challenger 复核。"),
    ] = DEFAULT_PREDICTION_OUTCOMES_PATH,
    causal_chain_path: Annotated[
        Path,
        typer.Option(help="decision_causal_chain ledger JSON 路径。"),
    ] = DEFAULT_DECISION_CAUSAL_CHAIN_PATH,
    learning_queue_path: Annotated[
        Path,
        typer.Option(help="decision learning queue JSON 路径。"),
    ] = DEFAULT_DECISION_LEARNING_QUEUE_PATH,
    rule_experiment_path: Annotated[
        Path,
        typer.Option(help="rule experiment ledger JSON 路径。"),
    ] = DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
    task_register_path: Annotated[
        Path,
        typer.Option(help="任务登记 Markdown 路径。"),
    ] = PROJECT_ROOT
    / "docs"
    / "task_register.md",
    as_of: Annotated[
        str | None,
        typer.Option(help="复核日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(help="复核窗口起始日期，格式为 YYYY-MM-DD，默认 as_of 前 7 天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 闭环复核报告输出路径。"),
    ] = None,
) -> None:
    """生成反馈闭环周期复核报告。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    since_date = _parse_date(since) if since else None
    report = build_feedback_loop_review_report(
        as_of=review_date,
        since=since_date,
        evidence_path=evidence_path,
        decision_snapshot_path=decision_snapshot_path,
        outcomes_path=outcomes_path,
        prediction_outcomes_path=prediction_outcomes_path,
        causal_chain_path=causal_chain_path,
        learning_queue_path=learning_queue_path,
        rule_experiment_path=rule_experiment_path,
        task_register_path=task_register_path,
    )
    report_path = output_path or default_feedback_loop_review_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    write_feedback_loop_review_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{status_style}]反馈闭环复核状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"警告数：{report.warning_count}")


@feedback_app.command("build-parameter-replay")
def build_parameter_replay_command(
    robustness_summary_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "backtest robustness JSON 摘要路径；不传时读取 outputs/backtests 最新 "
                "backtest_robustness_*.json。"
            )
        ),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="复核日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 参数 replay 报告输出路径。"),
    ] = None,
    summary_output_path: Annotated[
        Path | None,
        typer.Option(help="JSON 参数 replay 摘要输出路径。"),
    ] = None,
) -> None:
    """把 backtest robustness 参数复测结果接入 feedback 闭环。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    selected_summary_path = robustness_summary_path or latest_backtest_robustness_summary_path(
        DEFAULT_BACKTEST_ROBUSTNESS_DIR
    )
    if selected_summary_path is None:
        console.print(
            "[red]未找到 backtest_robustness_*.json；"
            "请先运行 aits backtest --robustness-report。[/red]"
        )
        raise typer.Exit(code=1)
    try:
        report = build_parameter_replay_report(
            robustness_summary_path=selected_summary_path,
            as_of=review_date,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]参数 replay 构建失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    report_output = output_path or default_parameter_replay_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    summary_output = summary_output_path or default_parameter_replay_summary_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    write_parameter_replay_report(report, report_output)
    write_parameter_replay_summary(report, summary_output)

    status_style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{status_style}]参数 replay 状态：{report.status}[/{status_style}]")
    console.print(f"参数复测场景：{report.scenario_count}")
    console.print(f"Material delta：{report.material_delta_count}")
    console.print(f"报告：{report_output}")
    console.print(f"摘要：{summary_output}")
    console.print(
        "治理边界：本命令只读解释参数复测收益变化，" "不修改 production scoring 或仓位闸门。"
    )


@feedback_app.command("build-parameter-candidates")
def build_parameter_candidates_command(
    parameter_replay_summary_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "parameter replay JSON 摘要路径；默认 "
                "outputs/reports/parameter_replay_YYYY-MM-DD.json。"
            )
        ),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="复核日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path,
        typer.Option(help="参数候选 ledger JSON 输出路径。"),
    ] = DEFAULT_PARAMETER_CANDIDATE_LEDGER_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 参数候选报告输出路径。"),
    ] = None,
    candidate_gate_mode: Annotated[
        str,
        typer.Option(
            help=(
                "候选门禁模式：strict 或 flow_validation。flow_validation "
                "仅用于接线验证，不改变 production。"
            )
        ),
    ] = "strict",
) -> None:
    """从 parameter replay 摘要生成 candidate-only 参数候选台账。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    try:
        ledger = build_parameter_candidate_ledger(
            parameter_replay_summary_path=parameter_replay_summary_path,
            as_of=review_date,
            evaluation_mode=candidate_gate_mode,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]参数候选台账构建失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    ledger_output = write_parameter_candidate_ledger(ledger, output_path)
    report_output = report_path or default_parameter_candidate_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    report_output = write_parameter_candidate_report(
        ledger,
        ledger_output,
        report_output,
    )

    status_style = "green" if ledger.status == "PASS" else "yellow"
    console.print(f"[{status_style}]参数候选状态：{ledger.status}[/{status_style}]")
    console.print(f"Trial 数：{ledger.trial_count}")
    console.print(f"Candidate 数：{ledger.candidate_count}")
    console.print(f"Evaluation mode：{ledger.evaluation_mode}")
    console.print(f"Forward shadow ready：{ledger.ready_for_forward_shadow_count}")
    console.print(f"Blocked：{ledger.blocked_count}")
    console.print(f"Ledger：{ledger_output}")
    console.print(f"报告：{report_output}")
    console.print("治理边界：候选台账不批准参数上线，不修改 production scoring 或仓位闸门。")


@feedback_app.command("evaluate-parameter-governance")
def evaluate_parameter_governance_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="复核日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    manifest_path: Annotated[
        Path,
        typer.Option(help="参数治理 manifest 路径。"),
    ] = DEFAULT_PARAMETER_GOVERNANCE_MANIFEST_PATH,
    parameter_candidate_ledger_path: Annotated[
        Path,
        typer.Option(help="parameter candidates ledger JSON 路径。"),
    ] = DEFAULT_PARAMETER_CANDIDATE_LEDGER_PATH,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 参数治理报告输出路径。"),
    ] = None,
    summary_output_path: Annotated[
        Path | None,
        typer.Option(help="JSON 参数治理摘要输出路径。"),
    ] = None,
) -> None:
    """评估可调参数 manifest 与候选证据，输出只读治理建议。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    try:
        report = build_parameter_governance_report(
            as_of=review_date,
            manifest_path=manifest_path,
            candidate_ledger_path=parameter_candidate_ledger_path,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        console.print(f"[red]参数治理评估失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    report_output = output_path or default_parameter_governance_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    summary_output = summary_output_path or default_parameter_governance_summary_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    write_parameter_governance_report(report, report_output)
    write_parameter_governance_summary(report, summary_output)

    status_style = "green" if report.status == "PASS" else "yellow"
    if report.status == "FAIL":
        status_style = "red"
    console.print(f"[{status_style}]参数治理状态：{report.status}[/{status_style}]")
    console.print(f"Manifest：{report.manifest.version}")
    console.print(f"Owner quantitative input：{report.manifest.owner_quantitative_input_status}")
    console.print(f"Action 分布：{report.action_counts}")
    console.print(f"报告：{report_output}")
    console.print(f"摘要：{summary_output}")
    console.print("治理边界：本命令不修改 production 参数、overlay、rule card 或日报结论。")
    if report.status == "FAIL":
        raise typer.Exit(code=1)


@feedback_app.command("optimize-market-feedback")
def optimize_market_feedback_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="复核日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(help="复核窗口起始日期，格式为 YYYY-MM-DD，默认 as_of 前 7 天。"),
    ] = None,
    replay_start: Annotated[
        str,
        typer.Option(help="as-if 回放窗口起始日期；默认主研究窗口起点 2021-02-22。"),
    ] = DEFAULT_MARKET_FEEDBACK_REPLAY_START.isoformat(),
    replay_end: Annotated[
        str | None,
        typer.Option(help="as-if 回放窗口结束日期，默认 as_of。"),
    ] = None,
    data_quality_report_path: Annotated[
        Path | None,
        typer.Option(help="数据质量报告路径；默认 outputs/reports/data_quality_YYYY-MM-DD.md。"),
    ] = None,
    decision_outcomes_path: Annotated[
        Path,
        typer.Option(help="decision_outcomes CSV 路径。"),
    ] = DEFAULT_DECISION_OUTCOMES_PATH,
    prediction_outcomes_path: Annotated[
        Path,
        typer.Option(help="prediction_outcomes CSV 路径。"),
    ] = DEFAULT_PREDICTION_OUTCOMES_PATH,
    causal_chain_path: Annotated[
        Path,
        typer.Option(help="decision causal chain ledger JSON 路径。"),
    ] = DEFAULT_DECISION_CAUSAL_CHAIN_PATH,
    learning_queue_path: Annotated[
        Path,
        typer.Option(help="decision learning queue JSON 路径。"),
    ] = DEFAULT_DECISION_LEARNING_QUEUE_PATH,
    rule_experiment_path: Annotated[
        Path,
        typer.Option(help="rule experiment ledger JSON 路径。"),
    ] = DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
    parameter_replay_summary_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "parameter replay JSON 摘要路径；默认 "
                "outputs/reports/parameter_replay_YYYY-MM-DD.json。"
            )
        ),
    ] = None,
    parameter_candidate_ledger_path: Annotated[
        Path,
        typer.Option(help="parameter candidates ledger JSON 路径。"),
    ] = DEFAULT_PARAMETER_CANDIDATE_LEDGER_PATH,
    parameter_governance_summary_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "parameter governance JSON 摘要路径；默认 "
                "outputs/reports/parameter_governance_YYYY-MM-DD.json。"
            )
        ),
    ] = None,
    shadow_maturity_report_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "shadow maturity Markdown 报告路径；默认 "
                "outputs/reports/shadow_maturity_YYYY-MM-DD.md。"
            )
        ),
    ] = None,
    calibration_overlay_path: Annotated[
        Path,
        typer.Option(help="approved calibration overlay JSON 路径。"),
    ] = DEFAULT_APPROVED_CALIBRATION_OVERLAY_PATH,
    effective_weights_path: Annotated[
        Path,
        typer.Option(help="current effective weights JSON 路径。"),
    ] = DEFAULT_EFFECTIVE_WEIGHTS_PATH,
    sample_policy_path: Annotated[
        Path,
        typer.Option(help="反馈优化样本政策配置路径。"),
    ] = DEFAULT_FEEDBACK_SAMPLE_POLICY_PATH,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 市场反馈优化报告输出路径。"),
    ] = None,
) -> None:
    """生成独立市场反馈优化闭环报告。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    since_date = _parse_date(since) if since else None
    replay_start_date = _parse_date(replay_start)
    replay_end_date = _parse_date(replay_end) if replay_end else None
    report = build_market_feedback_optimization_report(
        as_of=review_date,
        since=since_date,
        replay_start=replay_start_date,
        replay_end=replay_end_date,
        data_quality_report_path=data_quality_report_path,
        decision_outcomes_path=decision_outcomes_path,
        prediction_outcomes_path=prediction_outcomes_path,
        causal_chain_path=causal_chain_path,
        learning_queue_path=learning_queue_path,
        rule_experiment_path=rule_experiment_path,
        parameter_replay_summary_path=parameter_replay_summary_path,
        parameter_candidate_ledger_path=parameter_candidate_ledger_path,
        parameter_governance_summary_path=parameter_governance_summary_path,
        shadow_maturity_report_path=shadow_maturity_report_path,
        calibration_overlay_path=calibration_overlay_path,
        effective_weights_path=effective_weights_path,
        sample_policy_path=sample_policy_path,
    )
    report_path = output_path or default_market_feedback_optimization_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    write_market_feedback_optimization_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{status_style}]市场反馈优化状态：{report.status}[/{status_style}]")
    console.print(f"Readiness：{report.readiness}")
    console.print(f"报告：{report_path}")
    console.print(f"警告数：{report.warning_count}")
