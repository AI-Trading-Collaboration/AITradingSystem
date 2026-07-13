from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Any, cast

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_cycle import (
    DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH,
    DEFAULT_RULE_REVIEW_CYCLE_DIR,
    create_rule_owner_decision,
    list_rule_owner_decisions,
    record_rule_owner_decision,
    rule_owner_decision_report_payload,
    validate_rule_owner_decision_artifact,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_rescue_app,
    dynamic_v3_rule_owner_decision_app,
)


@dynamic_v3_rule_owner_decision_app.command("create")
def dynamic_v3_rule_owner_decision_create_command(
    cycle_id: Annotated[
        str,
        typer.Option("--cycle-id", "--cycle_id", help="cycle id。"),
    ],
    cycle_dir: Annotated[
        Path,
        typer.Option("--cycle-dir", help="rule review cycle artifact root。"),
    ] = DEFAULT_RULE_REVIEW_CYCLE_DIR,
    journal_path: Annotated[
        Path,
        typer.Option("--journal-path", help="owner decision journal JSONL path。"),
    ] = DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH,
) -> None:
    """创建 TRADING-178 pending owner decision record。"""
    result = create_rule_owner_decision(
        cycle_id=cycle_id,
        cycle_dir=cycle_dir,
        journal_path=journal_path,
    )
    record = result["record"]
    typer.echo(f"decision_id={result['decision_id']}")
    typer.echo(f"journal_path={result['journal_path']}")
    typer.echo(f"owner_decision={record['owner_decision']}")
    typer.echo("auto_apply=false")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_rule_owner_decision_app.command("list")
def dynamic_v3_rule_owner_decision_list_command(
    journal_path: Annotated[
        Path,
        typer.Option("--journal-path", help="owner decision journal JSONL path。"),
    ] = DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH,
) -> None:
    """列出 TRADING-178 owner decision journal。"""
    payload = list_rule_owner_decisions(journal_path=journal_path)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"decision_count={payload['decision_count']}")
    typer.echo(f"pending_count={payload['pending_count']}")
    for record in payload["records"]:
        typer.echo(
            "decision="
            + json.dumps(
                {
                    "decision_id": record.get("decision_id"),
                    "cycle_id": record.get("cycle_id"),
                    "owner_decision": record.get("owner_decision"),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
    typer.echo("production_effect=none")


@dynamic_v3_rule_owner_decision_app.command("record")
def dynamic_v3_rule_owner_decision_record_command(
    decision_id: Annotated[
        str,
        typer.Option("--decision-id", "--decision_id", help="decision id。"),
    ],
    decision: Annotated[
        str,
        typer.Option("--decision", help="owner decision value。"),
    ],
    notes: Annotated[str, typer.Option("--notes", help="owner decision note。")] = "",
    journal_path: Annotated[
        Path,
        typer.Option("--journal-path", help="owner decision journal JSONL path。"),
    ] = DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH,
) -> None:
    """记录 TRADING-178 owner decision。"""
    result = record_rule_owner_decision(
        decision_id=decision_id,
        decision=cast(Any, decision),
        notes=notes,
        journal_path=journal_path,
    )
    record = result["record"]
    typer.echo(f"decision_id={result['decision_id']}")
    typer.echo(f"owner_decision={record['owner_decision']}")
    typer.echo("auto_apply=false")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_rule_owner_decision_app.command("report")
def dynamic_v3_rule_owner_decision_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest owner decision。"),
    ] = False,
    decision_id: Annotated[
        str | None,
        typer.Option("--decision-id", "--decision_id", help="decision id。"),
    ] = None,
    journal_path: Annotated[
        Path,
        typer.Option("--journal-path", help="owner decision journal JSONL path。"),
    ] = DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH,
) -> None:
    """展示 TRADING-178 owner decision 摘要。"""
    payload = rule_owner_decision_report_payload(
        decision_id=decision_id,
        latest=latest,
        journal_path=journal_path,
    )
    record = payload["record"]
    typer.echo(f"decision_id={payload['decision_id']}")
    typer.echo(f"owner_decision={payload['owner_decision']}")
    typer.echo(f"target_count={len(record.get('target_ids') or [])}")
    typer.echo(f"report_path={payload['rule_owner_decision_report_path']}")
    typer.echo("auto_apply=false")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-rule-owner-decision")
def dynamic_v3_validate_rule_owner_decision_command(
    decision_id: Annotated[
        str,
        typer.Option("--decision-id", "--decision_id", help="decision id。"),
    ],
    journal_path: Annotated[
        Path,
        typer.Option("--journal-path", help="owner decision journal JSONL path。"),
    ] = DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH,
) -> None:
    """校验 TRADING-178 owner decision journal record。"""
    payload = validate_rule_owner_decision_artifact(
        decision_id=decision_id,
        journal_path=journal_path,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("auto_apply=false")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "dynamic_v3_rule_owner_decision_create_command",
    "dynamic_v3_rule_owner_decision_list_command",
    "dynamic_v3_rule_owner_decision_record_command",
    "dynamic_v3_rule_owner_decision_report_command",
    "dynamic_v3_validate_rule_owner_decision_command",
]
