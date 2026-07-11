from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

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
    DynamicRescueError,
    load_dynamic_failure_diagnostics_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_robustness import (
    DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    DynamicRobustnessError,
    load_dynamic_robustness_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_v3_failure_attribution import (
    DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_POLICY_CONFIG_PATH,
    DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_REPORT_DIR,
    DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_VALIDATION_DIR,
    DynamicV3FailureAttributionError,
    build_dynamic_v3_failure_attribution_report,
    build_dynamic_v3_failure_attribution_validation_report,
    latest_dynamic_v3_failure_attribution_real_evaluation_path,
    latest_dynamic_v3_failure_attribution_report_path,
    load_dynamic_v3_failure_attribution_policy_config,
    write_dynamic_v3_failure_attribution_report,
    write_dynamic_v3_failure_attribution_validation_report,
)
from ai_trading_system.etf_portfolio.dynamic_v3_failure_attribution import (
    load_json_artifact as load_dynamic_v3_failure_attribution_json_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_real_evaluation import (
    DEFAULT_DYNAMIC_V3_REAL_EVALUATION_POLICY_CONFIG_PATH,
    DynamicV3RealEvaluationError,
    load_dynamic_v3_real_evaluation_policy_config,
)
from ai_trading_system.etf_portfolio.dynamic_v3_rescue import (
    DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH,
    DynamicV3RescueError,
    load_dynamic_v3_rescue_policy_config,
)
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_PRICE_PATH,
    load_etf_config_bundle,
)
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
from ai_trading_system.interfaces.cli.etf_portfolio.registration import dynamic_v3_rescue_app


@dynamic_v3_rescue_app.command("failure-attribution")
def dynamic_v3_rescue_failure_attribution_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="标准化 ETF daily price cache。")
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="标准化 FRED rates cache for validate-data gate。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="数据质量门禁日期，默认使用 real evaluation end date。"),
    ] = None,
    start: Annotated[
        str | None,
        typer.Option("--start", help="failure attribution requested start date override。"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="failure attribution requested end date override。"),
    ] = None,
    real_evaluation_report_path: Annotated[
        Path | None,
        typer.Option(
            "--real-evaluation-report",
            help="TRADING-091 dynamic v0.3 real evaluation report JSON；默认 latest。",
        ),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option(
            "--config-path",
            "--config",
            help="TRADING-092 dynamic v0.3 failure attribution policy config。",
        ),
    ] = DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_POLICY_CONFIG_PATH,
    real_evaluation_config_path: Annotated[
        Path,
        typer.Option("--real-evaluation-config", help="TRADING-091 real evaluation config。"),
    ] = DEFAULT_DYNAMIC_V3_REAL_EVALUATION_POLICY_CONFIG_PATH,
    v3_rescue_config_path: Annotated[
        Path,
        typer.Option("--v3-rescue-config", help="TRADING-090 dynamic v0.3 rescue config。"),
    ] = DEFAULT_DYNAMIC_V3_RESCUE_POLICY_CONFIG_PATH,
    dynamic_robustness_config_path: Annotated[
        Path,
        typer.Option("--dynamic-robustness-config", help="TRADING-086 robustness config。"),
    ] = DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    dynamic_allocation_config_path: Annotated[
        Path,
        typer.Option("--dynamic-allocation-config", help="TRADING-084 allocation config。"),
    ] = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    failure_diagnostics_config_path: Annotated[
        Path,
        typer.Option(
            "--failure-diagnostics-config",
            help="TRADING-088 dynamic failure diagnostics config。",
        ),
    ] = DEFAULT_DYNAMIC_FAILURE_DIAGNOSTICS_POLICY_CONFIG_PATH,
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option("--data-quality-output-path", help="validate-data markdown output path。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir",
            "--report-output-dir",
            help="failure attribution 输出目录。",
        ),
    ] = DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_REPORT_DIR,
) -> None:
    """生成 TRADING-092 v0.3 reject 归因和 v0.4 promotion review；不 promotion。"""
    try:
        attribution_policy = load_dynamic_v3_failure_attribution_policy_config(config_path)
        resolved_real_report = (
            real_evaluation_report_path
            or latest_dynamic_v3_failure_attribution_real_evaluation_path(attribution_policy)
        )
        if resolved_real_report is None:
            raise DynamicV3FailureAttributionError(
                "latest dynamic v0.3 real evaluation report not found"
            )
        real_report = load_dynamic_v3_failure_attribution_json_artifact(resolved_real_report)
    except DynamicV3FailureAttributionError as exc:
        raise typer.BadParameter(str(exc)) from exc
    requested = mapping_obj(real_report.get("requested_range"))
    default_as_of = (
        parse_date(as_of)
        if as_of
        else (
            parse_date(end)
            if end
            else parse_date(str(requested.get("end") or "")) or date.today()
        )
    )
    quality_output = data_quality_output_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports", default_as_of
    )
    universe = load_universe()
    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(universe),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=default_as_of,
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
        etf_config = load_etf_config_bundle()
        prices, etf_quality = load_standard_prices(
            prices_path, etf_config.assets, etf_config.strategy
        )
        if not etf_quality.passed:
            raise DynamicV3FailureAttributionError(
                f"ETF price validation failed before v0.3 failure attribution: {etf_quality.status}"
            )
        payload = build_dynamic_v3_failure_attribution_report(
            prices=prices,
            etf_config=etf_config,
            policy=attribution_policy,
            real_evaluation_report=real_report,
            real_evaluation_report_path=resolved_real_report,
            real_policy=load_dynamic_v3_real_evaluation_policy_config(real_evaluation_config_path),
            v3_rescue_policy=load_dynamic_v3_rescue_policy_config(v3_rescue_config_path),
            dynamic_robustness_policy=load_dynamic_robustness_policy_config(
                dynamic_robustness_config_path
            ),
            dynamic_policy=load_dynamic_allocation_policy_config(dynamic_allocation_config_path),
            failure_policy=load_dynamic_failure_diagnostics_policy_config(
                failure_diagnostics_config_path
            ),
            start=parse_date(start) if start else None,
            end=parse_date(end) if end else None,
            data_quality_status=data_quality_report.status,
            data_quality_report=str(quality_output),
            prices_path=prices_path,
        )
    except (
        DynamicV3FailureAttributionError,
        DynamicV3RealEvaluationError,
        DynamicV3RescueError,
        DynamicRobustnessError,
        DynamicRescueError,
    ) as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_dynamic_v3_failure_attribution_report(payload, output_dir=output_dir)
    summary = mapping_obj(payload.get("summary"))
    typer.echo(f"ETF dynamic v0.3 failure attribution JSON：{paths['json']}")
    typer.echo(f"ETF dynamic v0.3 failure attribution Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"v0_3_rejection_primary_reason={summary.get('v0_3_rejection_primary_reason')}")
    typer.echo(f"v0_4_promotion_review={summary.get('v0_4_promotion_review')}")
    typer.echo(f"v0_5_design_recommendation={summary.get('v0_5_design_recommendation')}")
    typer.echo(f"data_quality_status={summary.get('data_quality_status')}")
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


@dynamic_v3_rescue_app.command("failure-attribution-report")
def dynamic_v3_rescue_failure_attribution_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="只读展示 latest v0.3 attribution。")
    ] = True,
    report_output_dir: Annotated[
        Path,
        typer.Option("--report-output-dir", help="dynamic v0.3 attribution report 目录。"),
    ] = DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_REPORT_DIR,
) -> None:
    """只读展示 latest TRADING-092 dynamic v0.3 failure attribution report。"""
    if not latest:
        raise typer.BadParameter(
            "dynamic-v3-rescue failure-attribution-report currently supports --latest"
        )
    resolved = latest_dynamic_v3_failure_attribution_report_path(report_output_dir)
    payload = load_optional_json_payload(resolved)
    if not payload:
        raise typer.BadParameter("dynamic v0.3 failure attribution report not found")
    summary = mapping_obj(payload.get("summary"))
    typer.echo(f"dynamic_v3_failure_attribution_report={resolved}")
    typer.echo(f"status={payload.get('status')}")
    typer.echo(f"v0_3_rejection_primary_reason={summary.get('v0_3_rejection_primary_reason')}")
    typer.echo(f"v0_4_promotion_review={summary.get('v0_4_promotion_review')}")
    typer.echo(f"v0_5_design_recommendation={summary.get('v0_5_design_recommendation')}")
    typer.echo(f"data_quality_status={summary.get('data_quality_status')}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("shadow_enrollment_allowed=false")


@dynamic_v3_rescue_app.command("validate-attribution")
def dynamic_v3_rescue_validate_attribution_command(
    config_path: Annotated[
        Path,
        typer.Option(
            "--config-path",
            "--config",
            help="TRADING-092 dynamic v0.3 failure attribution policy config。",
        ),
    ] = DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="failure attribution validation report 输出目录。"),
    ] = DEFAULT_DYNAMIC_V3_FAILURE_ATTRIBUTION_VALIDATION_DIR,
) -> None:
    """校验 TRADING-092 dynamic failure attribution workflow 和 safety boundary。"""
    payload = build_dynamic_v3_failure_attribution_validation_report(config_path=config_path)
    paths = write_dynamic_v3_failure_attribution_validation_report(payload, output_dir=output_dir)
    typer.echo(f"ETF dynamic v0.3 failure attribution validation JSON：{paths['json']}")
    typer.echo(f"ETF dynamic v0.3 failure attribution validation Markdown：{paths['markdown']}")
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
