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
from ai_trading_system.etf_portfolio.dynamic_v3_real_evaluation import (
    DEFAULT_DYNAMIC_V3_REAL_EVALUATION_POLICY_CONFIG_PATH,
    DEFAULT_DYNAMIC_V3_REAL_EVALUATION_REPORT_DIR,
    DEFAULT_DYNAMIC_V3_REAL_EVALUATION_VALIDATION_DIR,
    DynamicV3RealEvaluationError,
    build_dynamic_v3_real_evaluation_report,
    build_dynamic_v3_real_evaluation_validation_report,
    latest_dynamic_v3_real_evaluation_report_path,
    load_dynamic_v3_real_evaluation_policy_config,
    write_dynamic_v3_real_evaluation_report,
    write_dynamic_v3_real_evaluation_validation_report,
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


@dynamic_v3_rescue_app.command("real-evaluate")
def dynamic_v3_rescue_real_evaluate_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="标准化 ETF daily price cache。")
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="标准化 FRED rates cache for validate-data gate。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    as_of: Annotated[
        str | None, typer.Option("--as-of", help="数据质量门禁日期，默认 today。")
    ] = None,
    start: Annotated[
        str | None, typer.Option("--start", help="real evaluation requested start date。")
    ] = None,
    end: Annotated[
        str | None, typer.Option("--end", help="real evaluation requested end date。")
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option(
            "--config-path",
            "--config",
            help="TRADING-091 dynamic v0.3 real evaluation policy config。",
        ),
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
        typer.Option("--output-dir", "--report-output-dir", help="real evaluation 输出目录。"),
    ] = DEFAULT_DYNAMIC_V3_REAL_EVALUATION_REPORT_DIR,
) -> None:
    """生成 TRADING-091 v0.3 rescue 真实历史评估和 promotion gate；不 promotion。"""
    validation_date = parse_date(as_of) if as_of else date.today()
    quality_output = data_quality_output_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports", validation_date
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
        policy = load_dynamic_v3_real_evaluation_policy_config(config_path)
        etf_config = load_etf_config_bundle()
        prices, etf_quality = load_standard_prices(
            prices_path, etf_config.assets, etf_config.strategy
        )
        if not etf_quality.passed:
            raise DynamicV3RealEvaluationError(
                f"ETF price validation failed before v0.3 real evaluation: {etf_quality.status}"
            )
        payload = build_dynamic_v3_real_evaluation_report(
            prices=prices,
            etf_config=etf_config,
            policy=policy,
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
        DynamicV3RealEvaluationError,
        DynamicV3RescueError,
        DynamicRobustnessError,
        DynamicRescueError,
    ) as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_dynamic_v3_real_evaluation_report(payload, output_dir=output_dir)
    summary = mapping_obj(payload.get("summary"))
    best = mapping_obj(payload.get("best_candidate"))
    typer.echo(f"ETF dynamic v0.3 real evaluation JSON：{paths['json']}")
    typer.echo(f"ETF dynamic v0.3 real evaluation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"promotion_gate_decision={payload['promotion_gate_decision']}")
    typer.echo(f"best_candidate={best.get('policy_id')}")
    typer.echo(
        f"constraint_hit_reduction_vs_v0_4={summary.get('constraint_hit_reduction_vs_v0_4')}"
    )
    typer.echo(f"false_risk_off_delta_vs_v0_4={summary.get('false_risk_off_delta_vs_v0_4')}")
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


@dynamic_v3_rescue_app.command("real-report")
def dynamic_v3_rescue_real_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="只读展示 latest v0.3 real evaluation。")
    ] = True,
    report_output_dir: Annotated[
        Path,
        typer.Option("--report-output-dir", help="dynamic v0.3 real evaluation report 目录。"),
    ] = DEFAULT_DYNAMIC_V3_REAL_EVALUATION_REPORT_DIR,
) -> None:
    """只读展示 latest TRADING-091 dynamic v0.3 real evaluation report。"""
    if not latest:
        raise typer.BadParameter("dynamic-v3-rescue real-report currently supports --latest")
    resolved = latest_dynamic_v3_real_evaluation_report_path(report_output_dir)
    payload = load_optional_json_payload(resolved)
    if not payload:
        raise typer.BadParameter("dynamic v0.3 real evaluation report not found")
    summary = mapping_obj(payload.get("summary"))
    typer.echo(f"dynamic_v3_real_evaluation_report={resolved}")
    typer.echo(f"status={payload.get('status')}")
    typer.echo(f"promotion_gate_decision={payload.get('promotion_gate_decision')}")
    typer.echo(f"best_candidate={summary.get('best_v0_3_candidate')}")
    typer.echo(f"data_quality_status={summary.get('data_quality_status')}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("shadow_enrollment_allowed=false")


@dynamic_v3_rescue_app.command("validate-real")
def dynamic_v3_rescue_validate_real_command(
    config_path: Annotated[
        Path,
        typer.Option(
            "--config-path",
            "--config",
            help="TRADING-091 dynamic v0.3 real evaluation policy config。",
        ),
    ] = DEFAULT_DYNAMIC_V3_REAL_EVALUATION_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="real evaluation validation report 输出目录。"),
    ] = DEFAULT_DYNAMIC_V3_REAL_EVALUATION_VALIDATION_DIR,
) -> None:
    """校验 TRADING-091 dynamic v0.3 real evaluation workflow 和 safety boundary。"""
    payload = build_dynamic_v3_real_evaluation_validation_report(config_path=config_path)
    paths = write_dynamic_v3_real_evaluation_validation_report(payload, output_dir=output_dir)
    typer.echo(f"ETF dynamic v0.3 real evaluation validation JSON：{paths['json']}")
    typer.echo(f"ETF dynamic v0.3 real evaluation validation Markdown：{paths['markdown']}")
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
