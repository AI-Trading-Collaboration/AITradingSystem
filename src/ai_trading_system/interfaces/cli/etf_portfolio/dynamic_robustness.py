from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import (
    default_quality_report_path,
    validate_data_cache,
)
from ai_trading_system.data.quality import (
    write_data_quality_report as write_cache_data_quality_report,
)
from ai_trading_system.etf_portfolio.data import load_standard_prices
from ai_trading_system.etf_portfolio.dynamic_allocation import (
    DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    load_dynamic_allocation_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_robustness import (
    DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    DEFAULT_DYNAMIC_ROBUSTNESS_REPORT_DIR,
    DEFAULT_DYNAMIC_ROBUSTNESS_VALIDATION_DIR,
    DynamicRobustnessError,
    build_dynamic_robustness_report,
    build_dynamic_robustness_validation_report,
    latest_dynamic_robustness_report_path,
    load_dynamic_robustness_policy_config,
    load_latest_dynamic_calibration_report,
    write_dynamic_robustness_report,
    write_dynamic_robustness_validation_report,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH, load_etf_config_bundle
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    load_optional_json_payload,
    mapping_obj,
    parse_date,
)
from ai_trading_system.interfaces.cli.etf_portfolio.data_quality import (
    download_manifest_path,
    marketstack_prices_path,
    requires_marketstack_prices,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import dynamic_robustness_app


@dynamic_robustness_app.command("report")
def dynamic_robustness_report_command(
    candidate: Annotated[
        str | None,
        typer.Option(
            "--candidate",
            help="Dynamic candidate pack id；不填则读取 latest calibration top candidate。",
        ),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="只读展示 latest dynamic robustness report。"),
    ] = False,
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
        typer.Option("--start", help="robustness requested start date。"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="robustness requested end date。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic robustness policy config。"),
    ] = DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    dynamic_allocation_config_path: Annotated[
        Path,
        typer.Option(
            "--dynamic-allocation-config",
            help="TRADING-084 dynamic allocation policy config。",
        ),
    ] = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    dynamic_calibration_report_path: Annotated[
        Path | None,
        typer.Option(
            "--dynamic-calibration-report-path",
            help="Explicit TRADING-085 dynamic calibration report JSON。",
        ),
    ] = None,
    latest_dynamic_calibration_report: Annotated[
        bool,
        typer.Option(
            "--latest-dynamic-calibration-report/--no-latest-dynamic-calibration-report",
            help="没有显式 dynamic calibration report 时读取 latest TRADING-085 report。",
        ),
    ] = True,
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option("--data-quality-output-path", help="validate-data markdown output path。"),
    ] = None,
    report_output_dir: Annotated[
        Path,
        typer.Option("--report-output-dir", help="dynamic robustness report 输出目录。"),
    ] = DEFAULT_DYNAMIC_ROBUSTNESS_REPORT_DIR,
) -> None:
    """生成或只读展示 TRADING-086 dynamic robustness report；不写 production weights。"""
    if latest:
        resolved = latest_dynamic_robustness_report_path(report_output_dir)
        payload = load_optional_json_payload(resolved)
        if not payload:
            raise typer.BadParameter("dynamic robustness report not found")
        summary = mapping_obj(payload.get("summary"))
        typer.echo(f"dynamic_robustness_report={resolved}")
        typer.echo(f"status={payload.get('status')}")
        typer.echo(f"candidate={summary.get('dynamic_candidate_id')}")
        typer.echo(f"data_quality_status={summary.get('data_quality_status')}")
        typer.echo(f"dynamic_total_return={summary.get('dynamic_total_return')}")
        typer.echo(f"excess_vs_static_base={summary.get('excess_vs_static_base')}")
        typer.echo(f"false_risk_off_count={summary.get('false_risk_off_count')}")
        typer.echo(f"false_risk_on_count={summary.get('false_risk_on_count')}")
        typer.echo(f"overfit_status={summary.get('overfit_status')}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        typer.echo("official_target_weights_mutated=false")
        return

    validation_date = parse_date(as_of) if as_of else date.today()
    quality_output = data_quality_output_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    universe = load_universe()
    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(universe),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=validation_date,
        manifest_path=download_manifest_path(prices_path),
        secondary_prices_path=marketstack_prices_path(prices_path),
        require_secondary_prices=requires_marketstack_prices(prices_path),
    )
    write_cache_data_quality_report(data_quality_report, quality_output)
    typer.echo(f"validate_data_status={data_quality_report.status}")
    typer.echo(f"validate_data_report={quality_output}")
    if not data_quality_report.passed:
        raise typer.Exit(code=1)

    try:
        policy = load_dynamic_robustness_policy_config(config_path)
        dynamic_policy = load_dynamic_allocation_policy_config(dynamic_allocation_config_path)
        calibration_path: Path | None = None
        calibration_payload: dict[str, Any] = {}
        if dynamic_calibration_report_path is not None or latest_dynamic_calibration_report:
            calibration_path, calibration_payload = load_latest_dynamic_calibration_report(
                dynamic_calibration_report_path
            )
        etf_config = load_etf_config_bundle()
        prices, etf_quality = load_standard_prices(
            prices_path,
            etf_config.assets,
            etf_config.strategy,
        )
        if not etf_quality.passed:
            raise DynamicRobustnessError(
                f"ETF price validation failed before dynamic robustness: {etf_quality.status}"
            )
        report = build_dynamic_robustness_report(
            prices=prices,
            etf_config=etf_config,
            policy=policy,
            dynamic_policy=dynamic_policy,
            candidate_id=candidate,
            dynamic_calibration_report=calibration_payload,
            dynamic_calibration_report_path=calibration_path,
            start=parse_date(start) if start else None,
            end=parse_date(end) if end else None,
            data_quality_status=data_quality_report.status,
            data_quality_report=str(quality_output),
            prices_path=prices_path,
        )
    except DynamicRobustnessError as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_dynamic_robustness_report(report, output_dir=report_output_dir)
    summary = mapping_obj(report.get("summary"))
    typer.echo(f"ETF dynamic robustness report JSON：{paths['json']}")
    typer.echo(f"ETF dynamic robustness report Markdown：{paths['markdown']}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"candidate={summary.get('dynamic_candidate_id')}")
    typer.echo(f"data_quality_status={summary.get('data_quality_status')}")
    typer.echo(f"dynamic_total_return={summary.get('dynamic_total_return')}")
    typer.echo(f"excess_vs_static_base={summary.get('excess_vs_static_base')}")
    typer.echo(f"false_risk_off_count={summary.get('false_risk_off_count')}")
    typer.echo(f"false_risk_on_count={summary.get('false_risk_on_count')}")
    typer.echo(f"overfit_status={summary.get('overfit_status')}")
    typer.echo("shadow_enrollment_allowed=false")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@dynamic_robustness_app.command("validate")
def dynamic_robustness_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic robustness policy config。"),
    ] = DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    dynamic_allocation_config_path: Annotated[
        Path,
        typer.Option(
            "--dynamic-allocation-config",
            help="TRADING-084 dynamic allocation policy config。",
        ),
    ] = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="validation report 输出目录。"),
    ] = DEFAULT_DYNAMIC_ROBUSTNESS_VALIDATION_DIR,
) -> None:
    """校验 TRADING-086 dynamic robustness workflow 和 safety boundary。"""
    payload = build_dynamic_robustness_validation_report(
        policy_config_path=config_path,
        dynamic_policy_path=dynamic_allocation_config_path,
    )
    paths = write_dynamic_robustness_validation_report(payload, output_dir=output_dir)
    typer.echo(f"ETF dynamic robustness validation JSON：{paths['json']}")
    typer.echo(f"ETF dynamic robustness validation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("baseline_config_mutated=false")
    typer.echo("official_target_weights_mutated=false")
    typer.echo("automatic_candidate_promotion=false")
    typer.echo("auto_enrollment_without_owner_approval=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
