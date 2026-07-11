from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.data import load_standard_prices
from ai_trading_system.etf_portfolio.features import build_feature_store
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_PRICE_PATH,
    load_etf_config_bundle,
)
from ai_trading_system.etf_portfolio.trend_calibration import (
    DEFAULT_TREND_CALIBRATION_DATASET_DIR,
    DEFAULT_TREND_CALIBRATION_POLICY_CONFIG_PATH,
    DEFAULT_TREND_CALIBRATION_REGISTRY_DIR,
    DEFAULT_TREND_CALIBRATION_REPORT_DIR,
    DEFAULT_TREND_CALIBRATION_VALIDATION_DIR,
    TrendCalibrationError,
    build_trend_calibration_report,
    build_trend_calibration_validation_report,
    build_trend_signal_dataset,
    latest_trend_calibration_report_path,
    load_trend_calibration_policy_config,
    write_trend_calibration_report,
    write_trend_calibration_validation_report,
    write_trend_signal_config_registry,
    write_trend_signal_dataset,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    load_optional_json_payload,
    parse_date,
)
from ai_trading_system.interfaces.cli.etf_portfolio.data_quality import (
    run_cached_data_quality_gate_with_report,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import trend_calibration_app
from ai_trading_system.reports.report_index import DEFAULT_REPORT_REGISTRY_PATH


@trend_calibration_app.command("run")
def trend_calibration_run_command(
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="标准化 ETF daily price cache。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="标准化 FRED rates cache for validate-data gate。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="数据质量门禁日期，默认 today。"),
    ] = None,
    start: Annotated[
        str | None,
        typer.Option("--start", help="trend calibration requested start date。"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="trend calibration requested end date。"),
    ] = None,
    top: Annotated[int, typer.Option("--top", help="Top trend configs to retain。")] = 5,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="trend calibration policy config。"),
    ] = DEFAULT_TREND_CALIBRATION_POLICY_CONFIG_PATH,
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option("--data-quality-output-path", help="validate-data markdown output path。"),
    ] = None,
    dataset_output_dir: Annotated[
        Path,
        typer.Option("--dataset-output-dir", help="trend signal dataset 输出目录。"),
    ] = DEFAULT_TREND_CALIBRATION_DATASET_DIR,
    report_output_dir: Annotated[
        Path,
        typer.Option("--report-output-dir", help="trend calibration report 输出目录。"),
    ] = DEFAULT_TREND_CALIBRATION_REPORT_DIR,
    registry_output_dir: Annotated[
        Path,
        typer.Option("--registry-output-dir", help="trend signal config registry 输出目录。"),
    ] = DEFAULT_TREND_CALIBRATION_REGISTRY_DIR,
) -> None:
    """运行 TRADING-083 trend signal weight calibration；不输出 target weights。"""
    validation_date = parse_date(as_of) if as_of else date.today()
    quality_output, data_quality_report = run_cached_data_quality_gate_with_report(
        prices_path=prices_path,
        rates_path=rates_path,
        as_of=validation_date,
        output_path=data_quality_output_path,
    )
    try:
        policy = load_trend_calibration_policy_config(config_path)
        etf_config = load_etf_config_bundle()
        prices, etf_quality = load_standard_prices(
            prices_path,
            etf_config.assets,
            etf_config.strategy,
        )
        if not etf_quality.passed:
            raise TrendCalibrationError(
                f"ETF price validation failed before trend calibration: {etf_quality.status}"
            )
        start_date = parse_date(start) if start else None
        end_date = parse_date(end) if end else None
        features = build_feature_store(
            prices,
            assets=etf_config.assets,
            strategy=etf_config.strategy,
            start=None,
            end=end_date,
        )
        dataset = build_trend_signal_dataset(
            features=features,
            prices=prices,
            strategy=etf_config.strategy,
            policy=policy,
            start=start_date,
            end=end_date,
            data_quality_status=data_quality_report.status,
            data_quality_report=str(quality_output),
            price_source_path=str(prices_path),
        )
        report = build_trend_calibration_report(
            dataset=dataset,
            policy=policy,
            top=top,
        )
    except TrendCalibrationError as exc:
        raise typer.BadParameter(str(exc)) from exc
    dataset_paths = write_trend_signal_dataset(dataset, output_dir=dataset_output_dir)
    report_paths = write_trend_calibration_report(report, output_dir=report_output_dir)
    registry_paths = write_trend_signal_config_registry(
        report["trend_signal_config_registry"],
        output_dir=registry_output_dir,
    )
    summary = report["summary"]
    typer.echo(f"ETF trend signal dataset JSON：{dataset_paths['json']}")
    typer.echo(f"ETF trend calibration report JSON：{report_paths['json']}")
    typer.echo(f"ETF trend signal config registry JSON：{registry_paths['json']}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"top_config={summary['top_config']}")
    typer.echo(f"top_quality_score={summary['top_quality_score']}")
    typer.echo("evaluation_only=true")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@trend_calibration_app.command("report")
def trend_calibration_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取最新 trend calibration report。"),
    ] = True,
    report_path: Annotated[
        Path | None,
        typer.Option("--report-path", help="显式 report JSON path。"),
    ] = None,
    report_dir: Annotated[
        Path,
        typer.Option("--report-dir", help="report artifact directory。"),
    ] = DEFAULT_TREND_CALIBRATION_REPORT_DIR,
) -> None:
    """只读展示 latest TRADING-083 trend calibration report 摘要。"""
    resolved = report_path
    if resolved is None and latest:
        resolved = latest_trend_calibration_report_path(report_dir)
    if resolved is None:
        raise typer.BadParameter("trend calibration report not found")
    payload = load_optional_json_payload(resolved)
    summary = payload.get("summary")
    if not isinstance(summary, Mapping):
        raise typer.BadParameter(f"invalid trend calibration report: {resolved}")
    typer.echo(f"trend_calibration_report={resolved}")
    typer.echo(f"status={payload.get('status')}")
    typer.echo(f"top_config={summary.get('top_config')}")
    typer.echo(f"evidence_status={summary.get('evidence_status')}")
    typer.echo(f"redundancy_risk={summary.get('redundancy_risk')}")
    typer.echo(f"regime_stability={summary.get('regime_stability')}")
    typer.echo("evaluation_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@trend_calibration_app.command("validate")
def trend_calibration_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="trend calibration policy config。"),
    ] = DEFAULT_TREND_CALIBRATION_POLICY_CONFIG_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option("--report-registry-path", help="report registry YAML。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="validation report 输出目录。"),
    ] = DEFAULT_TREND_CALIBRATION_VALIDATION_DIR,
) -> None:
    """校验 TRADING-083 trend calibration workflow 和 safety boundary。"""
    payload = build_trend_calibration_validation_report(
        config_path=config_path,
        report_registry_path=report_registry_path,
    )
    paths = write_trend_calibration_validation_report(payload, output_dir=output_dir)
    typer.echo(f"ETF trend calibration validation JSON：{paths['json']}")
    typer.echo(f"ETF trend calibration validation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("evaluation_only=true")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "latest_trend_calibration_report_path",
    "trend_calibration_report_command",
    "trend_calibration_run_command",
    "trend_calibration_validate_command",
]
