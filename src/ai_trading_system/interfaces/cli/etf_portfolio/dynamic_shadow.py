from __future__ import annotations

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
from ai_trading_system.etf_portfolio.dynamic_robustness import (
    DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    DEFAULT_DYNAMIC_ROBUSTNESS_REPORT_DIR,
    DEFAULT_DYNAMIC_ROBUSTNESS_VALIDATION_DIR,
    latest_dynamic_robustness_report_path,
    load_dynamic_robustness_policy_config,
    load_latest_dynamic_calibration_report,
)
from ai_trading_system.etf_portfolio.dynamic_shadow import (
    DEFAULT_DYNAMIC_SHADOW_APPROVAL_DIR,
    DEFAULT_DYNAMIC_SHADOW_ENROLLMENT_DIR,
    DEFAULT_DYNAMIC_SHADOW_FORWARD_UPDATE_DIR,
    DEFAULT_DYNAMIC_SHADOW_PACKAGE_DIR,
    DEFAULT_DYNAMIC_SHADOW_POLICY_CONFIG_PATH,
    DEFAULT_DYNAMIC_SHADOW_REGISTRY_PATH,
    DEFAULT_DYNAMIC_SHADOW_VALIDATION_DIR,
    DEFAULT_DYNAMIC_SHADOW_WEEKLY_REVIEW_DIR,
    DynamicShadowError,
    build_dynamic_shadow_approved_enrollment,
    build_dynamic_shadow_forward_update,
    build_dynamic_shadow_owner_approval,
    build_dynamic_shadow_review_package,
    build_dynamic_shadow_validation_report,
    build_dynamic_shadow_weekly_review,
    latest_dynamic_shadow_forward_update_path,
    latest_dynamic_shadow_owner_approval_path,
    latest_dynamic_shadow_review_package_path,
    load_dynamic_shadow_candidate_registry,
    load_dynamic_shadow_policy_config,
    upsert_dynamic_shadow_candidate_registry,
    write_dynamic_shadow_approved_enrollment,
    write_dynamic_shadow_candidate_registry,
    write_dynamic_shadow_forward_update,
    write_dynamic_shadow_owner_approval,
    write_dynamic_shadow_review_package,
    write_dynamic_shadow_validation_report,
    write_dynamic_shadow_weekly_review,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH, load_etf_config_bundle
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    latest_json_file as _latest_json_file,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    load_optional_json_payload as _load_optional_json_payload,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj as _mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date as _parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.common import resolve_date as _resolve_date
from ai_trading_system.interfaces.cli.etf_portfolio.data_quality import (
    download_manifest_path as _download_manifest_path,
)
from ai_trading_system.interfaces.cli.etf_portfolio.data_quality import (
    marketstack_prices_path as _marketstack_prices_path,
)
from ai_trading_system.interfaces.cli.etf_portfolio.data_quality import (
    requires_marketstack_prices as _requires_marketstack_prices,
)
from ai_trading_system.interfaces.cli.etf_portfolio.operations import (
    DEFAULT_ETF_OPERATIONS_VALIDATION_DIR,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import dynamic_shadow_app


@dynamic_shadow_app.command("package")
def dynamic_shadow_package_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest TRADING-086 robustness report。"),
    ] = True,
    top: Annotated[int, typer.Option("--top", help="review package 包含前 N 个 candidates。")] = 3,
    dynamic_robustness_report_path: Annotated[
        Path | None,
        typer.Option("--dynamic-robustness-report", "--report", help="TRADING-086 report JSON。"),
    ] = None,
    dynamic_calibration_report_path: Annotated[
        Path | None,
        typer.Option("--dynamic-calibration-report", help="TRADING-085 report JSON。"),
    ] = None,
    dynamic_calibration_validation_path: Annotated[
        Path | None,
        typer.Option("--dynamic-calibration-validation", help="TRADING-085 validation JSON。"),
    ] = None,
    dynamic_robustness_validation_path: Annotated[
        Path | None,
        typer.Option("--dynamic-robustness-validation", help="TRADING-086 validation JSON。"),
    ] = None,
    operations_validation_path: Annotated[
        Path | None,
        typer.Option("--operations-validation", help="ETF operations validation JSON。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic shadow policy config。"),
    ] = DEFAULT_DYNAMIC_SHADOW_POLICY_CONFIG_PATH,
    dynamic_robustness_report_dir: Annotated[
        Path,
        typer.Option("--dynamic-robustness-report-dir", help="TRADING-086 report 目录。"),
    ] = DEFAULT_DYNAMIC_ROBUSTNESS_REPORT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="dynamic shadow package 输出目录。"),
    ] = DEFAULT_DYNAMIC_SHADOW_PACKAGE_DIR,
) -> None:
    """生成 TRADING-087 dynamic shadow owner review package；不 enroll。"""
    if not latest and dynamic_robustness_report_path is None:
        raise typer.BadParameter("--dynamic-robustness-report or --latest is required")
    resolved_robustness_path = (
        dynamic_robustness_report_path
        or latest_dynamic_robustness_report_path(dynamic_robustness_report_dir)
    )
    if resolved_robustness_path is None:
        raise typer.BadParameter("dynamic robustness report not found")
    calibration_path = dynamic_calibration_report_path
    calibration_payload: dict[str, Any] = {}
    if calibration_path is None:
        calibration_path, calibration_payload = load_latest_dynamic_calibration_report()
    else:
        calibration_payload = _load_optional_json_payload(calibration_path)
    resolved_calibration_validation = dynamic_calibration_validation_path or _latest_json_file(
        PROJECT_ROOT / "reports" / "etf_portfolio" / "dynamic_calibration" / "validation",
        "dynamic-calibration-validation_*.json",
    )
    resolved_robustness_validation = dynamic_robustness_validation_path or _latest_json_file(
        DEFAULT_DYNAMIC_ROBUSTNESS_VALIDATION_DIR,
        "dynamic-robustness-validation_*.json",
    )
    resolved_operations_validation = operations_validation_path or _latest_json_file(
        DEFAULT_ETF_OPERATIONS_VALIDATION_DIR,
        "operations_validation_*.json",
    )
    try:
        package = build_dynamic_shadow_review_package(
            dynamic_robustness_report=_load_optional_json_payload(resolved_robustness_path),
            dynamic_calibration_report=calibration_payload,
            dynamic_calibration_validation=_load_optional_json_payload(
                resolved_calibration_validation
            ),
            dynamic_robustness_validation=_load_optional_json_payload(
                resolved_robustness_validation
            ),
            operations_validation=_load_optional_json_payload(resolved_operations_validation),
            source_paths={
                "dynamic_robustness_report": str(resolved_robustness_path),
                "dynamic_calibration_report": (
                    "" if calibration_path is None else str(calibration_path)
                ),
                "dynamic_calibration_validation": (
                    ""
                    if resolved_calibration_validation is None
                    else str(resolved_calibration_validation)
                ),
                "dynamic_robustness_validation": (
                    ""
                    if resolved_robustness_validation is None
                    else str(resolved_robustness_validation)
                ),
                "operations_validation": (
                    ""
                    if resolved_operations_validation is None
                    else str(resolved_operations_validation)
                ),
            },
            policy=load_dynamic_shadow_policy_config(config_path),
            top=top,
        )
    except DynamicShadowError as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_dynamic_shadow_review_package(package, output_dir=output_dir)
    summary = _mapping_obj(package.get("review_summary"))
    typer.echo(f"ETF dynamic shadow package JSON：{paths['json']}")
    typer.echo(f"ETF dynamic shadow package Markdown：{paths['markdown']}")
    typer.echo(f"status={summary.get('status')}")
    typer.echo(f"top_candidate={summary.get('top_candidate')}")
    typer.echo(
        f"ready_after_owner_approval_count={summary.get('ready_after_owner_approval_count')}"
    )
    typer.echo(f"blocked_count={summary.get('blocked_count')}")
    typer.echo("automatic_enrollment_allowed=false")
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


@dynamic_shadow_app.command("approve")
def dynamic_shadow_approve_command(
    review_package_path: Annotated[
        Path,
        typer.Option("--review-package-path", "--package", help="dynamic shadow package JSON。"),
    ],
    candidate_id: Annotated[
        str,
        typer.Option("--candidate", "--candidate-id", help="dynamic candidate id。"),
    ],
    owner_decision: Annotated[
        str,
        typer.Option("--owner-decision", help="owner decision。"),
    ] = "approved_for_dynamic_shadow",
    rationale: Annotated[str, typer.Option("--rationale", help="owner rationale。")] = "",
    confidence: Annotated[float, typer.Option("--confidence", help="0.0-1.0 confidence。")] = 0.5,
    decision_journal_link: Annotated[
        str | None,
        typer.Option("--decision-journal-link", help="decision journal entry/link。"),
    ] = None,
    reviewer: Annotated[str, typer.Option("--reviewer", help="reviewer id。")] = "project_owner",
    condition: Annotated[
        list[str] | None,
        typer.Option("--condition", help="approval condition; can be repeated。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic shadow policy config。"),
    ] = DEFAULT_DYNAMIC_SHADOW_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="owner approval 输出目录。"),
    ] = DEFAULT_DYNAMIC_SHADOW_APPROVAL_DIR,
) -> None:
    """捕获 dynamic shadow owner approval；不 enroll、不修改 production。"""
    try:
        approval = build_dynamic_shadow_owner_approval(
            review_package=_load_optional_json_payload(review_package_path),
            candidate_id=candidate_id,
            owner_decision=owner_decision,
            rationale=rationale,
            confidence=confidence,
            decision_journal_link=decision_journal_link,
            conditions=condition,
            reviewer=reviewer,
            policy=load_dynamic_shadow_policy_config(config_path),
        )
    except DynamicShadowError as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_dynamic_shadow_owner_approval(approval, output_dir=output_dir)
    typer.echo(f"ETF dynamic shadow approval JSON：{paths['json']}")
    typer.echo(f"ETF dynamic shadow approval Markdown：{paths['markdown']}")
    typer.echo(f"approval_id={approval['approval_id']}")
    typer.echo(f"owner_decision={approval['owner_decision']}")
    typer.echo(f"approved_for_enrollment={approval['approved_for_enrollment']}")
    typer.echo(f"candidate={approval['candidate_id']}")
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


@dynamic_shadow_app.command("enroll-approved")
def dynamic_shadow_enroll_approved_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest package 和 approval。"),
    ] = False,
    approval_path: Annotated[
        Path | None,
        typer.Option("--approval-path", "--approval", help="owner approval JSON。"),
    ] = None,
    review_package_path: Annotated[
        Path | None,
        typer.Option("--review-package-path", "--package", help="dynamic shadow package JSON。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic shadow policy config。"),
    ] = DEFAULT_DYNAMIC_SHADOW_POLICY_CONFIG_PATH,
    registry_path: Annotated[
        Path,
        typer.Option("--registry-path", help="dynamic shadow candidate registry path。"),
    ] = DEFAULT_DYNAMIC_SHADOW_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="approved enrollment 输出目录。"),
    ] = DEFAULT_DYNAMIC_SHADOW_ENROLLMENT_DIR,
) -> None:
    """只登记 owner-approved dynamic candidates 进入 forward shadow observation。"""
    resolved_approval = approval_path or (
        latest_dynamic_shadow_owner_approval_path() if latest else None
    )
    resolved_package = review_package_path or (
        latest_dynamic_shadow_review_package_path() if latest else None
    )
    if resolved_approval is None or resolved_package is None:
        raise typer.BadParameter("--approval/--package or --latest is required")
    try:
        enrollment = build_dynamic_shadow_approved_enrollment(
            approval=_load_optional_json_payload(resolved_approval),
            review_package=_load_optional_json_payload(resolved_package),
            policy=load_dynamic_shadow_policy_config(config_path),
        )
        registry = upsert_dynamic_shadow_candidate_registry(
            load_dynamic_shadow_candidate_registry(registry_path),
            enrollment,
        )
    except DynamicShadowError as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_dynamic_shadow_approved_enrollment(enrollment, output_dir=output_dir)
    write_dynamic_shadow_candidate_registry(registry, registry_path=registry_path)
    typer.echo(f"ETF dynamic shadow enrollment JSON：{paths['json']}")
    typer.echo(f"ETF dynamic shadow enrollment Markdown：{paths['markdown']}")
    typer.echo(f"dynamic_shadow_registry={registry_path}")
    typer.echo(f"enrollment_id={enrollment['enrollment_id']}")
    typer.echo(f"candidate={enrollment['candidate_id']}")
    typer.echo(f"tracking_status={enrollment['tracking_status']}")
    typer.echo(f"active_candidate_count={registry['candidate_count']}")
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


@dynamic_shadow_app.command("update")
def dynamic_shadow_update_command(
    date_option: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="forward update 日期 YYYY-MM-DD 或 latest。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用 ETF price cache 最新日期。"),
    ] = False,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="标准化 ETF daily price cache。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="标准化 FRED rates cache for validate-data gate。"),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    registry_path: Annotated[
        Path,
        typer.Option("--registry-path", help="dynamic shadow candidate registry path。"),
    ] = DEFAULT_DYNAMIC_SHADOW_REGISTRY_PATH,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic shadow policy config。"),
    ] = DEFAULT_DYNAMIC_SHADOW_POLICY_CONFIG_PATH,
    dynamic_robustness_config_path: Annotated[
        Path,
        typer.Option("--dynamic-robustness-config", help="TRADING-086 policy config。"),
    ] = DEFAULT_DYNAMIC_ROBUSTNESS_POLICY_CONFIG_PATH,
    dynamic_allocation_config_path: Annotated[
        Path,
        typer.Option("--dynamic-allocation-config", help="TRADING-084 policy config。"),
    ] = DEFAULT_DYNAMIC_ALLOCATION_POLICY_CONFIG_PATH,
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option("--data-quality-output-path", help="validate-data markdown output path。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="dynamic shadow forward update 输出目录。"),
    ] = DEFAULT_DYNAMIC_SHADOW_FORWARD_UPDATE_DIR,
) -> None:
    """更新 approved dynamic shadow candidates 的 forward tracking records。"""
    if latest and date_option is not None:
        raise typer.BadParameter("--latest and --date cannot be combined")
    run_date = _resolve_date(
        "latest" if latest or date_option is None else date_option,
        prices_path=prices_path,
    )
    quality_output = data_quality_output_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        run_date,
    )
    universe = load_universe()
    data_quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(universe),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=run_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_cache_data_quality_report(data_quality_report, quality_output)
    typer.echo(f"validate_data_status={data_quality_report.status}")
    typer.echo(f"validate_data_report={quality_output}")
    if not data_quality_report.passed:
        raise typer.Exit(code=1)
    try:
        etf_config = load_etf_config_bundle()
        prices, etf_quality = load_standard_prices(
            prices_path,
            etf_config.assets,
            etf_config.strategy,
        )
        if not etf_quality.passed:
            raise DynamicShadowError(f"ETF price validation failed: {etf_quality.status}")
        calibration_path, calibration_payload = load_latest_dynamic_calibration_report()
        payload = build_dynamic_shadow_forward_update(
            registry=load_dynamic_shadow_candidate_registry(registry_path),
            policy=load_dynamic_shadow_policy_config(config_path),
            as_of=run_date,
            data_quality_status=data_quality_report.status,
            data_quality_report=str(quality_output),
            prices=prices,
            etf_config=etf_config,
            dynamic_robustness_policy=load_dynamic_robustness_policy_config(
                dynamic_robustness_config_path
            ),
            dynamic_allocation_policy=load_dynamic_allocation_policy_config(
                dynamic_allocation_config_path
            ),
            dynamic_calibration_report=calibration_payload,
            dynamic_calibration_report_path=calibration_path,
            prices_path=prices_path,
        )
    except DynamicShadowError as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_dynamic_shadow_forward_update(payload, output_dir=output_dir)
    typer.echo(f"ETF dynamic shadow forward update JSON：{paths['json']}")
    typer.echo(f"ETF dynamic shadow forward update Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"active_candidate_count={payload['active_candidate_count']}")
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


@dynamic_shadow_app.command("weekly-review")
def dynamic_shadow_weekly_review_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest forward update。"),
    ] = True,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="weekly review 日期 YYYY-MM-DD。"),
    ] = None,
    forward_update_path: Annotated[
        Path | None,
        typer.Option("--forward-update", help="dynamic shadow forward update JSON。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic shadow policy config。"),
    ] = DEFAULT_DYNAMIC_SHADOW_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="dynamic shadow weekly review 输出目录。"),
    ] = DEFAULT_DYNAMIC_SHADOW_WEEKLY_REVIEW_DIR,
) -> None:
    """生成 weekly dynamic shadow review；不 promotion、不写 production。"""
    resolved_update = forward_update_path or (
        latest_dynamic_shadow_forward_update_path() if latest else None
    )
    if resolved_update is None:
        raise typer.BadParameter("--forward-update or --latest is required")
    try:
        payload = build_dynamic_shadow_weekly_review(
            forward_update=_load_optional_json_payload(resolved_update),
            policy=load_dynamic_shadow_policy_config(config_path),
            as_of=_parse_date(as_of) if as_of else None,
        )
    except DynamicShadowError as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_dynamic_shadow_weekly_review(payload, output_dir=output_dir)
    summary = _mapping_obj(payload.get("summary"))
    typer.echo(f"ETF dynamic shadow weekly review JSON：{paths['json']}")
    typer.echo(f"ETF dynamic shadow weekly review Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={summary.get('candidate_count')}")
    typer.echo(f"watch_count={summary.get('watch_count')}")
    typer.echo(f"reject_pending_review_count={summary.get('reject_pending_review_count')}")
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


@dynamic_shadow_app.command("validate")
def dynamic_shadow_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="dynamic shadow policy config。"),
    ] = DEFAULT_DYNAMIC_SHADOW_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="validation report 输出目录。"),
    ] = DEFAULT_DYNAMIC_SHADOW_VALIDATION_DIR,
) -> None:
    """校验 TRADING-087 dynamic shadow workflow 和 approved-only safety boundary。"""
    payload = build_dynamic_shadow_validation_report(config_path=config_path)
    paths = write_dynamic_shadow_validation_report(payload, output_dir=output_dir)
    typer.echo(f"ETF dynamic shadow validation JSON：{paths['json']}")
    typer.echo(f"ETF dynamic shadow validation Markdown：{paths['markdown']}")
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
