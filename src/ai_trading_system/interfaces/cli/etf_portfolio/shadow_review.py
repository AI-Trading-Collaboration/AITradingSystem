from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.shadow_ready_review import (
    DEFAULT_SHADOW_READY_REVIEW_APPROVAL_DIR,
    DEFAULT_SHADOW_READY_REVIEW_ENROLLMENT_DIR,
    DEFAULT_SHADOW_READY_REVIEW_PACKAGE_DIR,
    DEFAULT_SHADOW_READY_REVIEW_POLICY_CONFIG_PATH,
    DEFAULT_SHADOW_READY_REVIEW_VALIDATION_DIR,
    DEFAULT_WEIGHT_SEARCH_DIAGNOSTICS_DIR,
    ShadowReadyReviewError,
    aggregate_shadow_ready_review_candidates,
    build_near_shadow_review_summary,
    build_shadow_candidate_approved_enrollment,
    build_shadow_candidate_owner_approval,
    build_shadow_candidate_review_package,
    build_shadow_candidate_review_validation_report,
    load_shadow_ready_review_policy_config,
    load_shadow_review_diagnostics_artifacts,
    rank_shadow_ready_review_candidates,
    write_shadow_candidate_approved_enrollment,
    write_shadow_candidate_owner_approval,
    write_shadow_candidate_review_package,
    write_shadow_candidate_review_validation_report,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    artifact_stem,
    load_optional_json_payload,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import shadow_review_app
from ai_trading_system.reports.report_index import DEFAULT_REPORT_REGISTRY_PATH


@shadow_review_app.command("package")
def shadow_review_package_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取最新 diagnostics artifacts。"),
    ] = True,
    diagnostics_json_path: Annotated[
        Path | None,
        typer.Option("--diagnostics-json-path", help="historical diagnostics JSON。"),
    ] = None,
    stable_shapes_csv_path: Annotated[
        Path | None,
        typer.Option("--stable-shapes-csv-path", help="stable shapes CSV。"),
    ] = None,
    near_shadow_csv_path: Annotated[
        Path | None,
        typer.Option("--near-shadow-csv-path", help="near-shadow CSV。"),
    ] = None,
    diagnostics_dir: Annotated[
        Path,
        typer.Option("--diagnostics-dir", help="diagnostics artifact directory。"),
    ] = DEFAULT_WEIGHT_SEARCH_DIAGNOSTICS_DIR,
    top: Annotated[int, typer.Option("--top", help="review package top N candidates。")] = 3,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="shadow-ready review policy config。"),
    ] = DEFAULT_SHADOW_READY_REVIEW_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="review package 输出目录。"),
    ] = DEFAULT_SHADOW_READY_REVIEW_PACKAGE_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 TRADING-082 shadow-ready candidate owner review package。"""
    try:
        policy = load_shadow_ready_review_policy_config(config_path)
        artifacts = load_shadow_review_diagnostics_artifacts(
            diagnostics_json_path=diagnostics_json_path,
            stable_shapes_csv_path=stable_shapes_csv_path,
            near_shadow_csv_path=near_shadow_csv_path,
            diagnostics_dir=diagnostics_dir,
            latest=latest,
        )
        aggregation = aggregate_shadow_ready_review_candidates(artifacts, policy=policy)
        ranking = rank_shadow_ready_review_candidates(aggregation, policy=policy)
        near_shadow = build_near_shadow_review_summary(artifacts)
        payload = build_shadow_candidate_review_package(
            artifacts=artifacts,
            aggregation=aggregation,
            ranking=ranking,
            near_shadow_summary=near_shadow,
            policy=policy,
            top=top,
        )
    except ShadowReadyReviewError as exc:
        raise typer.BadParameter(str(exc)) from exc
    stem = artifact_stem(str(payload["review_package_id"]))
    paths = write_shadow_candidate_review_package(
        payload,
        json_path=json_path or output_dir / f"{stem}.json",
        markdown_path=markdown_path or output_dir / f"{stem}.md",
    )
    summary = payload["review_summary"]
    typer.echo(f"ETF shadow candidate review package JSON：{paths['json']}")
    typer.echo(f"ETF shadow candidate review package Markdown：{paths['markdown']}")
    typer.echo(f"review_package_id={payload['review_package_id']}")
    typer.echo(f"top_candidate={summary['top_candidate']}")
    typer.echo(f"pending_review_count={summary['pending_review_count']}")
    typer.echo(f"blocked_count={summary['blocked_count']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["diagnostics_source_summary"]["artifact_status"] == "FAIL":
        raise typer.Exit(code=1)


@shadow_review_app.command("approve")
def shadow_review_approve_command(
    review_package_path: Annotated[
        Path,
        typer.Option("--review-package-path", "--package", help="review package JSON。"),
    ],
    shape_id: Annotated[str, typer.Option("--shape", "--shape-id", help="shape id。")],
    owner_decision: Annotated[str, typer.Option("--owner-decision", help="owner decision。")],
    rationale: Annotated[str, typer.Option("--rationale", help="owner rationale。")],
    confidence: Annotated[float, typer.Option("--confidence", help="0.0-1.0 confidence。")],
    selected_weight_set_id: Annotated[
        str | None,
        typer.Option("--selected-weight-set-id", "--weight-set", help="selected weight set id。"),
    ] = None,
    condition: Annotated[
        list[str] | None,
        typer.Option("--condition", help="approval condition; can be repeated。"),
    ] = None,
    decision_journal_link: Annotated[
        str | None,
        typer.Option("--decision-journal-link", help="decision journal link/id。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="shadow-ready review policy config。"),
    ] = DEFAULT_SHADOW_READY_REVIEW_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="approval 输出目录。"),
    ] = DEFAULT_SHADOW_READY_REVIEW_APPROVAL_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """捕获 owner shadow-review approval；不 enroll、不修改 production。"""
    try:
        package = load_optional_json_payload(review_package_path)
        payload = build_shadow_candidate_owner_approval(
            review_package=package,
            shape_id=shape_id,
            selected_weight_set_id=selected_weight_set_id,
            owner_decision=owner_decision,
            rationale=rationale,
            confidence=confidence,
            conditions=condition,
            decision_journal_link=decision_journal_link,
            policy=load_shadow_ready_review_policy_config(config_path),
        )
    except ShadowReadyReviewError as exc:
        raise typer.BadParameter(str(exc)) from exc
    stem = artifact_stem(str(payload["approval_id"]))
    paths = write_shadow_candidate_owner_approval(
        payload,
        json_path=json_path or output_dir / f"{stem}.json",
        markdown_path=markdown_path or output_dir / f"{stem}.md",
    )
    typer.echo(f"ETF shadow candidate owner approval JSON：{paths['json']}")
    typer.echo(f"ETF shadow candidate owner approval Markdown：{paths['markdown']}")
    typer.echo(f"approval_id={payload['approval_id']}")
    typer.echo(f"owner_decision={payload['owner_decision']}")
    typer.echo(f"shape_id={payload['shape_id']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@shadow_review_app.command("enroll-approved")
def shadow_review_enroll_approved_command(
    approval_path: Annotated[
        Path,
        typer.Option("--approval-path", "--approval", help="owner approval JSON。"),
    ],
    review_package_path: Annotated[
        Path,
        typer.Option("--review-package-path", "--package", help="review package JSON。"),
    ],
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="shadow-ready review policy config。"),
    ] = DEFAULT_SHADOW_READY_REVIEW_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="approved enrollment 输出目录。"),
    ] = DEFAULT_SHADOW_READY_REVIEW_ENROLLMENT_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """只对 owner-approved candidates 建立 forward shadow tracking enrollment。"""
    try:
        approval = load_optional_json_payload(approval_path)
        package = load_optional_json_payload(review_package_path)
        payload = build_shadow_candidate_approved_enrollment(
            approval=approval,
            review_package=package,
            policy=load_shadow_ready_review_policy_config(config_path),
        )
    except ShadowReadyReviewError as exc:
        raise typer.BadParameter(str(exc)) from exc
    stem = artifact_stem(str(payload["enrollment_id"]))
    paths = write_shadow_candidate_approved_enrollment(
        payload,
        json_path=json_path or output_dir / f"{stem}.json",
        markdown_path=markdown_path or output_dir / f"{stem}.md",
    )
    typer.echo(f"ETF shadow candidate enrollment JSON：{paths['json']}")
    typer.echo(f"ETF shadow candidate enrollment Markdown：{paths['markdown']}")
    typer.echo(f"enrollment_id={payload['enrollment_id']}")
    typer.echo(f"shadow_candidate_id={payload['shadow_candidate_id']}")
    typer.echo(f"forward_tracking_status={payload['forward_tracking_status']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@shadow_review_app.command("validate")
def shadow_review_validate_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="shadow-ready review policy config。"),
    ] = DEFAULT_SHADOW_READY_REVIEW_POLICY_CONFIG_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option("--report-registry-path", help="report registry YAML。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="validation report 输出目录。"),
    ] = DEFAULT_SHADOW_READY_REVIEW_VALIDATION_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 TRADING-082 shadow candidate review/enrollment workflow 和 safety boundary。"""
    payload = build_shadow_candidate_review_validation_report(
        config_path=config_path,
        report_registry_path=report_registry_path,
    )
    stem = artifact_stem(str(payload["validation_id"]))
    paths = write_shadow_candidate_review_validation_report(
        payload,
        json_path=json_path or output_dir / f"{stem}.json",
        markdown_path=markdown_path or output_dir / f"{stem}.md",
    )
    typer.echo(f"ETF shadow candidate review validation JSON：{paths['json']}")
    typer.echo(f"ETF shadow candidate review validation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
