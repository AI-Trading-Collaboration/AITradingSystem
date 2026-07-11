from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.baseline_review import (
    DEFAULT_BASELINE_REVIEW_DECISION_DIR,
    DEFAULT_BASELINE_REVIEW_OUTCOME_DIR,
    DEFAULT_BASELINE_REVIEW_PACKAGE_DIR,
    DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    DEFAULT_BASELINE_REVIEW_PROPOSAL_DIR,
    DEFAULT_BASELINE_REVIEW_REPORT_DIR,
    DEFAULT_BASELINE_REVIEW_VALIDATION_DIR,
    BaselineReviewError,
    build_baseline_change_proposal_draft,
    build_baseline_review_eligibility,
    build_baseline_review_evidence_matrix,
    build_baseline_review_package,
    build_baseline_review_validation_report,
    build_candidate_review_outcome,
    build_owner_review_decision,
    link_baseline_review_decision_to_journal,
    write_baseline_change_proposal_draft,
    write_baseline_review_outcome,
    write_baseline_review_package,
    write_baseline_review_validation_report,
    write_owner_review_decision,
)
from ai_trading_system.etf_portfolio.decision_journal import DEFAULT_DECISION_JOURNAL_PATH
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    artifact_stem,
    load_optional_json_payload,
    parse_date,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import baseline_review_app
from ai_trading_system.platform.artifacts.writer import write_text_atomic
from ai_trading_system.reports.report_index import DEFAULT_REPORT_REGISTRY_PATH


@baseline_review_app.command("eligibility")
def baseline_review_eligibility_command(
    candidate: Annotated[str, typer.Option("--candidate", help="Baseline review candidate ID。")],
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD；省略时使用今天。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="baseline review policy config。"),
    ] = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    report_index_path: Annotated[
        Path | None,
        typer.Option("--report-index-path", help="可选 report index JSON。"),
    ] = None,
    report_registry_path: Annotated[
        Path,
        typer.Option("--report-registry-path", help="report registry YAML。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    root_path: Annotated[
        Path,
        typer.Option("--root-path", help="扫描既有 artifacts 的根目录。"),
    ] = PROJECT_ROOT,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="eligibility JSON 输出目录。"),
    ] = DEFAULT_BASELINE_REVIEW_REPORT_DIR / "eligibility",
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
) -> None:
    """评估 ETF baseline review candidate 是否可进入 owner manual review。"""
    run_date = date.today() if as_of is None else parse_date(as_of)
    try:
        payload = build_baseline_review_eligibility(
            candidate_id=candidate,
            as_of=run_date,
            config_path=config_path,
            report_index_path=report_index_path,
            report_registry_path=report_registry_path,
            root_path=root_path,
        )
    except BaselineReviewError as exc:
        raise typer.BadParameter(str(exc)) from exc
    output = json_path or output_dir / f"baseline_review_eligibility_{run_date.isoformat()}.json"
    write_text_atomic(
        output,
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )
    typer.echo(f"ETF baseline review eligibility JSON：{output}")
    typer.echo(f"candidate_id={payload['candidate_id']}")
    typer.echo(f"eligibility_status={payload['eligibility_status']}")
    typer.echo(f"blocker_count={len(payload['blockers'])}")
    typer.echo(f"warning_count={len(payload['warnings'])}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["eligibility_status"] in {"blocked", "rejected_by_policy"}:
        raise typer.Exit(code=1)


@baseline_review_app.command("matrix")
def baseline_review_matrix_command(
    candidate: Annotated[str, typer.Option("--candidate", help="Baseline review candidate ID。")],
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD；省略时使用今天。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="baseline review policy config。"),
    ] = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    report_index_path: Annotated[
        Path | None,
        typer.Option("--report-index-path", help="可选 report index JSON。"),
    ] = None,
    report_registry_path: Annotated[
        Path,
        typer.Option("--report-registry-path", help="report registry YAML。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    root_path: Annotated[
        Path,
        typer.Option("--root-path", help="扫描既有 artifacts 的根目录。"),
    ] = PROJECT_ROOT,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="evidence matrix JSON 输出目录。"),
    ] = DEFAULT_BASELINE_REVIEW_REPORT_DIR / "matrix",
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
) -> None:
    """生成 ETF baseline review evidence requirement matrix。"""
    run_date = date.today() if as_of is None else parse_date(as_of)
    try:
        matrix = build_baseline_review_evidence_matrix(
            candidate_id=candidate,
            as_of=run_date,
            config_path=config_path,
            report_index_path=report_index_path,
            report_registry_path=report_registry_path,
            root_path=root_path,
        )
    except BaselineReviewError as exc:
        raise typer.BadParameter(str(exc)) from exc
    payload = matrix.model_dump(mode="json")
    output = json_path or output_dir / f"baseline_review_matrix_{run_date.isoformat()}.json"
    write_text_atomic(
        output,
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )
    typer.echo(f"ETF baseline review matrix JSON：{output}")
    typer.echo(f"candidate_id={matrix.candidate_id}")
    typer.echo(f"row_count={len(matrix.rows)}")
    typer.echo(f"blocking_row_count={len([row for row in matrix.rows if row.blocking])}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@baseline_review_app.command("package")
def baseline_review_package_command(
    candidate: Annotated[str, typer.Option("--candidate", help="Baseline review candidate ID。")],
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD；省略时使用今天。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="baseline review policy config。"),
    ] = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    report_index_path: Annotated[
        Path | None,
        typer.Option("--report-index-path", help="可选 report index JSON。"),
    ] = None,
    report_registry_path: Annotated[
        Path,
        typer.Option("--report-registry-path", help="report registry YAML。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    root_path: Annotated[
        Path,
        typer.Option("--root-path", help="扫描既有 artifacts 的根目录。"),
    ] = PROJECT_ROOT,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="review package 输出目录。"),
    ] = DEFAULT_BASELINE_REVIEW_PACKAGE_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 ETF baseline candidate human review package。"""
    run_date = date.today() if as_of is None else parse_date(as_of)
    try:
        payload = build_baseline_review_package(
            candidate_id=candidate,
            as_of=run_date,
            config_path=config_path,
            report_index_path=report_index_path,
            report_registry_path=report_registry_path,
            root_path=root_path,
        )
    except BaselineReviewError as exc:
        raise typer.BadParameter(str(exc)) from exc
    paths = write_baseline_review_package(
        payload,
        json_path=json_path or output_dir / f"baseline_review_package_{run_date.isoformat()}.json",
        markdown_path=markdown_path
        or output_dir / f"baseline_review_package_{run_date.isoformat()}.md",
    )
    typer.echo(f"ETF baseline review package JSON：{paths['json']}")
    typer.echo(f"ETF baseline review package Markdown：{paths['markdown']}")
    typer.echo(f"candidate_id={payload['candidate_id']}")
    typer.echo(f"eligibility_status={payload['eligibility']['eligibility_status']}")
    typer.echo(f"blocker_count={len(payload['blockers'])}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@baseline_review_app.command("capture-decision")
def baseline_review_capture_decision_command(
    review_package_path: Annotated[
        Path,
        typer.Option("--review-package-path", help="Baseline review package JSON。"),
    ],
    owner_decision: Annotated[str, typer.Option("--owner-decision", help="Owner decision。")],
    rationale: Annotated[str, typer.Option("--rationale", help="Owner rationale。")],
    confidence: Annotated[float, typer.Option("--confidence", help="0.0-1.0 confidence。")],
    condition: Annotated[
        list[str] | None,
        typer.Option("--condition", help="Decision condition; can be repeated。"),
    ] = None,
    follow_up_task: Annotated[
        list[str] | None,
        typer.Option("--follow-up-task", help="Follow-up task; can be repeated。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="baseline review policy config。"),
    ] = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    journal_path: Annotated[
        Path,
        typer.Option("--journal-path", help="ETF decision journal path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
    link_journal: Annotated[
        bool,
        typer.Option("--link-journal/--skip-journal-link", help="是否写入 decision journal。"),
    ] = True,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="decision capture 输出目录。"),
    ] = DEFAULT_BASELINE_REVIEW_DECISION_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """捕获 owner baseline review decision，并可链接到 decision journal。"""
    try:
        package = load_optional_json_payload(review_package_path)
        decision = build_owner_review_decision(
            review_package=package,
            owner_decision=owner_decision,
            rationale=rationale,
            confidence=confidence,
            conditions=condition,
            follow_up_tasks=follow_up_task,
            config_path=config_path,
        )
        if link_journal:
            decision = link_baseline_review_decision_to_journal(
                decision,
                review_package_path=review_package_path,
                journal_path=journal_path,
            )
    except BaselineReviewError as exc:
        raise typer.BadParameter(str(exc)) from exc
    stem = artifact_stem(decision["decision_id"])
    paths = write_owner_review_decision(
        decision,
        json_path=json_path or output_dir / f"{stem}.json",
        markdown_path=markdown_path or output_dir / f"{stem}.md",
    )
    typer.echo(f"ETF baseline review decision JSON：{paths['json']}")
    typer.echo(f"ETF baseline review decision Markdown：{paths['markdown']}")
    typer.echo(f"decision_id={decision['decision_id']}")
    typer.echo(f"owner_decision={decision['owner_decision']}")
    typer.echo(f"decision_journal_status={decision['decision_journal_linkage']['status']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@baseline_review_app.command("proposal-draft")
def baseline_review_proposal_draft_command(
    review_package_path: Annotated[
        Path,
        typer.Option("--review-package-path", help="Baseline review package JSON。"),
    ],
    decision_path: Annotated[
        Path,
        typer.Option("--decision-path", help="Owner decision JSON。"),
    ],
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="baseline review policy config。"),
    ] = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="proposal draft 输出目录。"),
    ] = DEFAULT_BASELINE_REVIEW_PROPOSAL_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """仅在 owner approve 后生成 baseline change proposal draft；不应用变更。"""
    try:
        package = load_optional_json_payload(review_package_path)
        decision = load_optional_json_payload(decision_path)
        payload = build_baseline_change_proposal_draft(
            review_package=package,
            owner_decision=decision,
            config_path=config_path,
        )
    except BaselineReviewError as exc:
        raise typer.BadParameter(str(exc)) from exc
    stem = artifact_stem(payload["proposal_id"])
    paths = write_baseline_change_proposal_draft(
        payload,
        json_path=json_path or output_dir / f"{stem}.json",
        markdown_path=markdown_path or output_dir / f"{stem}.md",
    )
    typer.echo(f"ETF baseline change proposal draft JSON：{paths['json']}")
    typer.echo(f"ETF baseline change proposal draft Markdown：{paths['markdown']}")
    typer.echo(f"proposal_id={payload['proposal_id']}")
    typer.echo("proposal_is_draft_only=true")
    typer.echo("baseline_config_mutated=false")
    typer.echo("target_weights_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@baseline_review_app.command("outcome")
def baseline_review_outcome_command(
    candidate: Annotated[str, typer.Option("--candidate", help="Baseline review candidate ID。")],
    decision_path: Annotated[
        Path | None,
        typer.Option("--decision-path", help="Optional owner decision JSON。"),
    ] = None,
    proposal_path: Annotated[
        Path | None,
        typer.Option("--proposal-path", help="Optional proposal draft JSON。"),
    ] = None,
    previous_outcome_path: Annotated[
        Path | None,
        typer.Option("--previous-outcome-path", help="Optional prior outcome JSON。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="baseline review policy config。"),
    ] = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="review outcome 输出目录。"),
    ] = DEFAULT_BASELINE_REVIEW_OUTCOME_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """记录 baseline review outcome；不修改 production state。"""
    try:
        decision = load_optional_json_payload(decision_path) if decision_path else None
        proposal = load_optional_json_payload(proposal_path) if proposal_path else None
        previous = (
            load_optional_json_payload(previous_outcome_path) if previous_outcome_path else None
        )
        payload = build_candidate_review_outcome(
            candidate_id=candidate,
            decision=decision,
            proposal=proposal,
            previous_outcome=previous,
            config_path=config_path,
        )
    except BaselineReviewError as exc:
        raise typer.BadParameter(str(exc)) from exc
    stem = artifact_stem(f"baseline-review-outcome-{candidate}")
    paths = write_baseline_review_outcome(
        payload,
        json_path=json_path or output_dir / f"{stem}.json",
        markdown_path=markdown_path or output_dir / f"{stem}.md",
    )
    typer.echo(f"ETF baseline review outcome JSON：{paths['json']}")
    typer.echo(f"ETF baseline review outcome Markdown：{paths['markdown']}")
    typer.echo(f"candidate_id={payload['candidate_id']}")
    typer.echo(f"latest_review_status={payload['latest_review_status']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@baseline_review_app.command("validate")
def baseline_review_validate_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD；省略时使用今天。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="baseline review policy config。"),
    ] = DEFAULT_BASELINE_REVIEW_POLICY_CONFIG_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option("--report-registry-path", help="report registry YAML。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="validation report 输出目录。"),
    ] = DEFAULT_BASELINE_REVIEW_VALIDATION_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 TRADING-077 baseline review playbook 完整性和安全边界。"""
    run_date = date.today() if as_of is None else parse_date(as_of)
    payload = build_baseline_review_validation_report(
        as_of=run_date,
        config_path=config_path,
        report_registry_path=report_registry_path,
    )
    paths = write_baseline_review_validation_report(
        payload,
        json_path=json_path
        or output_dir / f"baseline_review_validation_{run_date.isoformat()}.json",
        markdown_path=markdown_path
        or output_dir / f"baseline_review_validation_{run_date.isoformat()}.md",
    )
    typer.echo(f"ETF baseline review validation JSON：{paths['json']}")
    typer.echo(f"ETF baseline review validation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo(f"warning_check_count={payload['warning_check_count']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
