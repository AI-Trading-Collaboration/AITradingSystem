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
from ai_trading_system.data.quality import default_quality_report_path, validate_data_cache
from ai_trading_system.data.quality import (
    write_data_quality_report as write_cache_data_quality_report,
)
from ai_trading_system.etf_portfolio.data import load_standard_prices
from ai_trading_system.etf_portfolio.dynamic_allocation import (
    DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    load_dynamic_allocation_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_rescue import (
    DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH,
    DEFAULT_DYNAMIC_RESCUE_DATASET_DIR,
    DEFAULT_DYNAMIC_RESCUE_REPORT_DIR,
    DEFAULT_DYNAMIC_RESCUE_VALIDATION_DIR,
    DynamicRescueError,
    build_dynamic_rescue_batch_report,
    build_dynamic_rescue_validation_report,
    latest_dynamic_rescue_report_path,
    load_dynamic_failure_diagnostics_policy_config,
    load_dynamic_robustness_report,
    load_latest_failed_dynamic_package,
    write_dynamic_failure_dataset,
    write_dynamic_rescue_report,
    write_dynamic_rescue_validation_report,
)
from ai_trading_system.etf_portfolio.dynamic_robustness import (
    DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    DEFAULT_DYNAMIC_ROBUSTNESS_REPORT_DIR,
    load_dynamic_robustness_policy_config,
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
from ai_trading_system.interfaces.cli.etf_portfolio.registration import dynamic_rescue_app


@dynamic_rescue_app.command("run")
def dynamic_rescue_run_command(
    base_candidate: Annotated[
        str | None,
        typer.Option(
            "--base-candidate",
            help="Failed dynamic v0.1 candidate id；不填则读取 source report summary。",
        ),
    ] = None,
    latest_failed_package: Annotated[
        bool,
        typer.Option(
            "--latest-failed-package/--no-latest-failed-package",
            help="从 latest dynamic shadow package 回溯 failed robustness report。",
        ),
    ] = False,
    dynamic_robustness_report_path: Annotated[
        Path | None,
        typer.Option(
            "--dynamic-robustness-report-path",
            help="Explicit failed TRADING-086 dynamic robustness report JSON。",
        ),
    ] = None,
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
        typer.Option("--start", help="rescue requested start date。"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="rescue requested end date。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic failure diagnostics config。"),
    ] = DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH,
    dynamic_robustness_config_path: Annotated[
        Path,
        typer.Option(
            "--dynamic-robustness-config",
            help="TRADING-086 dynamic robustness policy config。",
        ),
    ] = DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    dynamic_allocation_config_path: Annotated[
        Path,
        typer.Option(
            "--dynamic-allocation-config",
            help="TRADING-084 dynamic allocation policy config。",
        ),
    ] = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option("--data-quality-output-path", help="validate-data markdown output path。"),
    ] = None,
    dataset_output_dir: Annotated[
        Path,
        typer.Option("--dataset-output-dir", help="dynamic failure dataset 输出目录。"),
    ] = DEFAULT_DYNAMIC_RESCUE_DATASET_DIR,
    report_output_dir: Annotated[
        Path,
        typer.Option("--report-output-dir", help="dynamic rescue report 输出目录。"),
    ] = DEFAULT_DYNAMIC_RESCUE_REPORT_DIR,
) -> None:
    """生成 TRADING-088 failure diagnostics 和 bounded rescue candidate report。"""
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
        package_path: Path | None = None
        package_payload: dict[str, Any] = {}
        resolved_robustness_path = dynamic_robustness_report_path
        if latest_failed_package:
            package_path, package_payload = load_latest_failed_dynamic_package()
            source = mapping_obj(package_payload.get("source_artifacts"))
            source_robustness = source.get("dynamic_robustness_report")
            if resolved_robustness_path is None and source_robustness:
                resolved_robustness_path = Path(str(source_robustness))
        loaded_path, failed_report = load_dynamic_robustness_report(
            resolved_robustness_path,
            report_dir=DEFAULT_DYNAMIC_ROBUSTNESS_REPORT_DIR,
        )
        policy = load_dynamic_failure_diagnostics_policy_config(config_path)
        robustness_policy = load_dynamic_robustness_policy_config(dynamic_robustness_config_path)
        dynamic_policy = load_dynamic_allocation_policy_config(dynamic_allocation_config_path)
        etf_config = load_etf_config_bundle()
        prices, etf_quality = load_standard_prices(
            prices_path,
            etf_config.assets,
            etf_config.strategy,
        )
        if not etf_quality.passed:
            raise DynamicRescueError(
                f"ETF price validation failed before dynamic rescue: {etf_quality.status}"
            )
        report = build_dynamic_rescue_batch_report(
            prices=prices,
            etf_config=etf_config,
            policy=policy,
            dynamic_robustness_policy=robustness_policy,
            dynamic_policy=dynamic_policy,
            failed_robustness_report=failed_report,
            shadow_review_package=package_payload,
            candidate_id=base_candidate,
            start=parse_date(start) if start else None,
            end=parse_date(end) if end else None,
            data_quality_status=data_quality_report.status,
            data_quality_report=str(quality_output),
            prices_path=prices_path,
        )
    except DynamicRescueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    dataset_paths = write_dynamic_failure_dataset(
        mapping_obj(report.get("failure_dataset")),
        output_dir=dataset_output_dir,
    )
    report_paths = write_dynamic_rescue_report(report, output_dir=report_output_dir)
    summary = mapping_obj(report.get("improvement_summary"))
    typer.echo(f"dynamic_failure_dataset_json={dataset_paths['json']}")
    typer.echo(f"dynamic_failure_dataset_markdown={dataset_paths['markdown']}")
    typer.echo(f"dynamic_rescue_report_json={report_paths['json']}")
    typer.echo(f"dynamic_rescue_report_markdown={report_paths['markdown']}")
    typer.echo(f"source_dynamic_robustness_report={loaded_path or ''}")
    typer.echo(f"source_dynamic_shadow_package={package_path or ''}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"best_rescue_candidate={summary.get('best_candidate')}")
    typer.echo(f"best_status={summary.get('best_status')}")
    typer.echo("shadow_enrollment_allowed=false")
    typer.echo("automatic_enrollment_allowed=false")
    typer.echo("owner_approval_executed=false")
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


@dynamic_rescue_app.command("report")
def dynamic_rescue_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="只读展示 latest dynamic rescue report。"),
    ] = False,
    report_output_dir: Annotated[
        Path,
        typer.Option("--report-output-dir", help="dynamic rescue report 输出目录。"),
    ] = DEFAULT_DYNAMIC_RESCUE_REPORT_DIR,
) -> None:
    """只读展示 latest TRADING-088 dynamic rescue report。"""
    if not latest:
        raise typer.BadParameter("dynamic-rescue report currently supports --latest")
    resolved = latest_dynamic_rescue_report_path(report_output_dir)
    payload = load_optional_json_payload(resolved)
    if not payload:
        raise typer.BadParameter("dynamic rescue report not found")
    summary = mapping_obj(payload.get("improvement_summary"))
    typer.echo(f"dynamic_rescue_report={resolved}")
    typer.echo(f"status={payload.get('status')}")
    typer.echo(f"best_rescue_candidate={summary.get('best_candidate')}")
    typer.echo(f"best_status={summary.get('best_status')}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("shadow_enrollment_allowed=false")


@dynamic_rescue_app.command("validate")
def dynamic_rescue_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic failure diagnostics config。"),
    ] = DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH,
    dynamic_robustness_config_path: Annotated[
        Path,
        typer.Option(
            "--dynamic-robustness-config",
            help="TRADING-086 dynamic robustness policy config。",
        ),
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
    ] = DEFAULT_DYNAMIC_RESCUE_VALIDATION_DIR,
) -> None:
    """校验 TRADING-088 dynamic rescue workflow 和 safety boundary。"""
    payload = build_dynamic_rescue_validation_report(
        policy_config_path=config_path,
        dynamic_robustness_policy_path=dynamic_robustness_config_path,
        dynamic_policy_path=dynamic_allocation_config_path,
    )
    paths = write_dynamic_rescue_validation_report(payload, output_dir=output_dir)
    typer.echo(f"ETF dynamic rescue validation JSON：{paths['json']}")
    typer.echo(f"ETF dynamic rescue validation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("shadow_enrollment_allowed=false")
    typer.echo("automatic_enrollment_allowed=false")
    typer.echo("owner_approval_executed=false")
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
