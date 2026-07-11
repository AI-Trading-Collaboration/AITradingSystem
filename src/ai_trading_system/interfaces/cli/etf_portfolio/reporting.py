from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.strategy_evidence_dashboard import (
    DEFAULT_STRATEGY_EVIDENCE_AGGREGATION_DIR,
    DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH,
    DEFAULT_STRATEGY_EVIDENCE_REPORT_DIR,
    DEFAULT_STRATEGY_EVIDENCE_VALIDATION_DIR,
    build_strategy_evidence_aggregation,
    build_strategy_evidence_dashboard,
    build_strategy_evidence_validation_report,
    write_strategy_evidence_dashboard_report,
    write_strategy_evidence_validation_report,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.registration import evidence_dashboard_app
from ai_trading_system.platform.artifacts.writer import write_text_atomic
from ai_trading_system.reports.report_index import DEFAULT_REPORT_REGISTRY_PATH


@evidence_dashboard_app.command("aggregate")
def evidence_dashboard_aggregate_command(
    as_of: Annotated[
        str,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD。"),
    ],
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="evidence dashboard source registry。"),
    ] = DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH,
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
        typer.Option("--output-dir", help="aggregation JSON 输出目录。"),
    ] = DEFAULT_STRATEGY_EVIDENCE_AGGREGATION_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
) -> None:
    """聚合既有 ETF strategy evidence sources，不运行上游、不补造结论。"""
    run_date = parse_date(as_of)
    payload = build_strategy_evidence_aggregation(
        as_of=run_date,
        config_path=config_path,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        root_path=root_path,
    )
    output = json_path or output_dir / f"strategy_evidence_aggregation_{run_date.isoformat()}.json"
    write_text_atomic(
        output,
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )
    typer.echo(f"ETF strategy evidence aggregation JSON：{output}")
    typer.echo(f"aggregation_status={payload['aggregation_status']}")
    typer.echo(f"loaded_sources={len(payload['loaded_sources'])}")
    typer.echo(f"missing_sources={len(payload['missing_sources'])}")
    typer.echo(f"stale_sources={len(payload['stale_sources'])}")
    typer.echo(f"blocked_sources={len(payload['blocked_sources'])}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@evidence_dashboard_app.command("report")
def evidence_dashboard_report_command(
    as_of: Annotated[
        str,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD。"),
    ],
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="evidence dashboard source registry。"),
    ] = DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH,
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
        typer.Option("--output-dir", help="dashboard report 输出目录。"),
    ] = DEFAULT_STRATEGY_EVIDENCE_REPORT_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 ETF Strategy Evidence Dashboard JSON / Markdown。"""
    run_date = parse_date(as_of)
    dashboard = build_strategy_evidence_dashboard(
        as_of=run_date,
        config_path=config_path,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        root_path=root_path,
    )
    paths = write_strategy_evidence_dashboard_report(
        dashboard,
        json_path=json_path
        or output_dir / f"strategy_evidence_dashboard_{run_date.isoformat()}.json",
        markdown_path=markdown_path
        or output_dir / f"strategy_evidence_dashboard_{run_date.isoformat()}.md",
    )
    typer.echo(f"ETF strategy evidence dashboard JSON：{paths['json']}")
    typer.echo(f"ETF strategy evidence dashboard Markdown：{paths['markdown']}")
    typer.echo(f"overall_status={dashboard.overall_status}")
    typer.echo(f"evidence_card_count={len(dashboard.evidence_cards)}")
    typer.echo(f"candidate_ranking_count={len(dashboard.candidate_rankings)}")
    typer.echo(f"conflict_count={len(dashboard.conflicts)}")
    typer.echo(f"manual_review_priority_count={len(dashboard.manual_review_priorities)}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@evidence_dashboard_app.command("validate")
def evidence_dashboard_validate_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD；省略时使用今天。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="evidence dashboard source registry。"),
    ] = DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option("--report-registry-path", help="report registry YAML。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="validation report 输出目录。"),
    ] = DEFAULT_STRATEGY_EVIDENCE_VALIDATION_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 TRADING-076 evidence dashboard workflow 和 safety boundary。"""
    run_date = date.today() if as_of is None else parse_date(as_of)
    payload = build_strategy_evidence_validation_report(
        as_of=run_date,
        config_path=config_path,
        report_registry_path=report_registry_path,
    )
    paths = write_strategy_evidence_validation_report(
        payload,
        json_path=json_path
        or output_dir / f"strategy_evidence_validation_{run_date.isoformat()}.json",
        markdown_path=markdown_path
        or output_dir / f"strategy_evidence_validation_{run_date.isoformat()}.md",
    )
    typer.echo(f"ETF strategy evidence validation JSON：{paths['json']}")
    typer.echo(f"ETF strategy evidence validation Markdown：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "DEFAULT_REPORT_REGISTRY_PATH",
    "DEFAULT_STRATEGY_EVIDENCE_AGGREGATION_DIR",
    "DEFAULT_STRATEGY_EVIDENCE_CONFIG_PATH",
    "DEFAULT_STRATEGY_EVIDENCE_REPORT_DIR",
    "DEFAULT_STRATEGY_EVIDENCE_VALIDATION_DIR",
    "evidence_dashboard_aggregate_command",
    "evidence_dashboard_report_command",
    "evidence_dashboard_validate_command",
]
