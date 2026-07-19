from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.decision_journal import (
    DEFAULT_DECISION_JOURNAL_PATH,
    DEFAULT_DECISION_JOURNAL_PROPOSAL_DIR,
    DEFAULT_DECISION_JOURNAL_REPORT_DIR,
    DEFAULT_DECISION_JOURNAL_VALIDATION_DIR,
    DecisionJournalError,
    add_decision_entry,
    build_candidate_state_update_proposals,
    build_decision_entry_from_weekly_review,
    build_decision_journal_analytics,
    build_decision_journal_report,
    build_decision_journal_validation_report,
    decision_entries,
    load_decision_journal,
    remove_decision_entry,
    update_decision_entry,
    write_decision_journal,
    write_decision_journal_analytics,
    write_decision_journal_report,
    write_decision_journal_validation_report,
    write_decision_state_update_proposals,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import decision_journal_app
from ai_trading_system.interfaces.cli.etf_portfolio.weekly_review import (
    weekly_review_date as _weekly_review_date,
)


@decision_journal_app.command("add")
def decision_journal_add_command(
    weekly_review_path: Annotated[
        Path,
        typer.Option(help="TRADING-068 weekly review JSON path。"),
    ],
    action_item_id: Annotated[
        str,
        typer.Option(help="weekly review manual_review_actions[].action_id。"),
    ],
    human_decision: Annotated[
        str,
        typer.Option(help="人工决策摘要。"),
    ],
    decision_status: Annotated[
        str,
        typer.Option(help="decision_status enum value。"),
    ],
    rationale: Annotated[
        str,
        typer.Option(help="人工决策依据。"),
    ],
    confidence: Annotated[
        float,
        typer.Option(help="人工信心 0.0-1.0。"),
    ],
    follow_up_task: Annotated[
        str,
        typer.Option(help="后续人工任务。"),
    ],
    linked_candidate: Annotated[
        str,
        typer.Option(help="关联 candidate / portfolio review target。"),
    ],
    linked_report: Annotated[
        Path | None,
        typer.Option(help="可选关联报告；默认使用 weekly review JSON。"),
    ] = None,
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
) -> None:
    """追加人工 portfolio decision journal entry。"""
    try:
        journal = load_decision_journal(journal_path)
        entry = build_decision_entry_from_weekly_review(
            weekly_review_path=weekly_review_path,
            action_item_id=action_item_id,
            human_decision=human_decision,
            decision_status=decision_status,
            rationale=rationale,
            confidence=confidence,
            follow_up_task=follow_up_task,
            linked_candidate=linked_candidate,
            linked_report=linked_report,
        )
        updated = add_decision_entry(journal, entry)
        write_decision_journal(updated, journal_path)
    except DecisionJournalError as exc:
        typer.echo(f"ETF decision journal blocked：{exc}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF decision journal entry added：{journal_path}")
    typer.echo(f"decision_id={entry['decision_id']}")
    typer.echo(f"review_id={entry['review_id']}")
    typer.echo(f"action_item_id={entry['action_item_id']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@decision_journal_app.command("update")
def decision_journal_update_command(
    decision_id: Annotated[
        str,
        typer.Option(help="decision_id to update。"),
    ],
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
    human_decision: Annotated[
        str | None,
        typer.Option(help="更新人工决策摘要。"),
    ] = None,
    decision_status: Annotated[
        str | None,
        typer.Option(help="更新 decision_status enum value。"),
    ] = None,
    rationale: Annotated[
        str | None,
        typer.Option(help="更新 rationale。"),
    ] = None,
    confidence: Annotated[
        float | None,
        typer.Option(help="更新 confidence 0.0-1.0。"),
    ] = None,
    follow_up_task: Annotated[
        str | None,
        typer.Option(help="更新 follow-up task。"),
    ] = None,
    linked_candidate: Annotated[
        str | None,
        typer.Option(help="更新 linked candidate。"),
    ] = None,
    linked_report: Annotated[
        Path | None,
        typer.Option(help="更新 linked report。"),
    ] = None,
) -> None:
    """更新人工 portfolio decision journal entry。"""
    updates = {
        "human_decision": human_decision,
        "decision_status": decision_status,
        "rationale": rationale,
        "confidence": confidence,
        "follow_up_task": follow_up_task,
        "linked_candidate": linked_candidate,
        "linked_report": None if linked_report is None else str(linked_report),
    }
    try:
        if not any(value is not None for value in updates.values()):
            raise DecisionJournalError("update requires at least one field")
        journal = load_decision_journal(journal_path)
        updated = update_decision_entry(journal, decision_id=decision_id, updates=updates)
        write_decision_journal(updated, journal_path)
    except DecisionJournalError as exc:
        typer.echo(f"ETF decision journal blocked：{exc}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF decision journal entry updated：{journal_path}")
    typer.echo(f"decision_id={decision_id}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@decision_journal_app.command("list")
def decision_journal_list_command(
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
    as_json: Annotated[
        bool,
        typer.Option("--json", help="输出 JSON payload。"),
    ] = False,
) -> None:
    """列出 active portfolio decision journal entries。"""
    try:
        journal = load_decision_journal(journal_path)
        entries = decision_entries(journal)
    except DecisionJournalError as exc:
        typer.echo(f"ETF decision journal blocked：{exc}")
        raise typer.Exit(code=1) from exc
    if as_json:
        typer.echo(
            json.dumps(
                {"journal_path": str(journal_path), "entries": entries},
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return
    typer.echo(f"ETF decision journal entries：{len(entries)}")
    for entry in entries:
        typer.echo(
            " | ".join(
                [
                    str(entry.get("decision_id")),
                    str(entry.get("review_date")),
                    str(entry.get("decision_status")),
                    str(entry.get("action_item_id")),
                    str(entry.get("linked_candidate")),
                ]
            )
        )
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@decision_journal_app.command("remove")
def decision_journal_remove_command(
    decision_id: Annotated[
        str,
        typer.Option(help="decision_id to remove from active entries。"),
    ],
    reason: Annotated[
        str,
        typer.Option(help="remove reason；entry is moved to removed_entries audit trail。"),
    ],
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
) -> None:
    """从 active journal 移除 entry，并保留 removed_entries audit trail。"""
    try:
        journal = load_decision_journal(journal_path)
        updated = remove_decision_entry(journal, decision_id=decision_id, reason=reason)
        write_decision_journal(updated, journal_path)
    except DecisionJournalError as exc:
        typer.echo(f"ETF decision journal blocked：{exc}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF decision journal entry removed：{journal_path}")
    typer.echo(f"decision_id={decision_id}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@decision_journal_app.command("report")
def decision_journal_report_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="decision journal report 日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期生成 decision journal report。"),
    ] = False,
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="decision journal report 输出目录。"),
    ] = DEFAULT_DECISION_JOURNAL_REPORT_DIR,
) -> None:
    """生成 portfolio decision journal JSON/Markdown/HTML summary。"""
    run_date = _weekly_review_date(as_of=as_of, latest=latest)
    try:
        journal = load_decision_journal(journal_path)
        payload = build_decision_journal_report(
            journal,
            as_of=run_date,
            journal_path=journal_path,
        )
        json_path = output_dir / f"decision_journal_{run_date.isoformat()}.json"
        md_path = output_dir / f"decision_journal_{run_date.isoformat()}.md"
        html_path = output_dir / f"decision_journal_{run_date.isoformat()}.html"
        write_decision_journal_report(
            payload,
            json_path=json_path,
            markdown_path=md_path,
            html_path=html_path,
        )
    except DecisionJournalError as exc:
        typer.echo(f"ETF decision journal report blocked：{exc}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF decision journal report：{md_path}")
    typer.echo(f"json={json_path}")
    typer.echo(f"html={html_path}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@decision_journal_app.command("analytics")
def decision_journal_analytics_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="decision journal analytics 日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期生成 analytics。"),
    ] = False,
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="decision journal analytics 输出目录。"),
    ] = DEFAULT_DECISION_JOURNAL_REPORT_DIR,
) -> None:
    """生成 portfolio decision journal outcome analytics JSON。"""
    run_date = _weekly_review_date(as_of=as_of, latest=latest)
    try:
        journal = load_decision_journal(journal_path)
        payload = build_decision_journal_analytics(journal)
        output_path = output_dir / f"decision_journal_analytics_{run_date.isoformat()}.json"
        write_decision_journal_analytics(payload, output_path)
    except DecisionJournalError as exc:
        typer.echo(f"ETF decision journal analytics blocked：{exc}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF decision journal analytics：{output_path}")
    typer.echo(f"entry_count={payload['entry_count']}")
    typer.echo(f"follow_up_task_count={payload['follow_up_task_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@decision_journal_app.command("propose-state-updates")
def decision_journal_propose_state_updates_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="proposal 日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期生成 proposal。"),
    ] = False,
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="decision state proposal 输出目录。"),
    ] = DEFAULT_DECISION_JOURNAL_PROPOSAL_DIR,
) -> None:
    """生成 candidate state update proposal；不修改 candidate registry。"""
    run_date = _weekly_review_date(as_of=as_of, latest=latest)
    try:
        journal = load_decision_journal(journal_path)
        payload = build_candidate_state_update_proposals(journal)
        json_path = output_dir / f"decision_state_update_proposals_{run_date.isoformat()}.json"
        md_path = output_dir / f"decision_state_update_proposals_{run_date.isoformat()}.md"
        write_decision_state_update_proposals(
            payload,
            json_path=json_path,
            markdown_path=md_path,
        )
    except DecisionJournalError as exc:
        typer.echo(f"ETF decision state proposal blocked：{exc}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF decision state update proposal：{md_path}")
    typer.echo(f"proposal_count={payload['proposal_count']}")
    typer.echo("state_mutation_performed=false")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@decision_journal_app.command("validate")
def decision_journal_validate_command(
    journal_path: Annotated[
        Path,
        typer.Option(help="decision journal state path。"),
    ] = DEFAULT_DECISION_JOURNAL_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="decision journal validation 输出目录。"),
    ] = DEFAULT_DECISION_JOURNAL_VALIDATION_DIR,
) -> None:
    """生成 TRADING-069 decision journal validation gate；失败时 fail closed。"""
    generated = datetime.now(UTC)
    payload = build_decision_journal_validation_report(
        journal_path=journal_path,
        generated_at=generated,
    )
    stem = f"decision_journal_validation_{generated.date().isoformat()}"
    json_path = output_dir / f"{stem}.json"
    md_path = output_dir / f"{stem}.md"
    write_decision_journal_validation_report(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF decision journal validation gate：{md_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
