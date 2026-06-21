from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.data_foundation import (
    DEFAULT_RESEARCH_CASE_LIBRARY_OUTPUT_ROOT,
    DEFAULT_RESEARCH_EXECUTION_OUTPUT_ROOT,
    DEFAULT_RESEARCH_LABEL_OUTPUT_ROOT,
    DEFAULT_RESEARCH_RUN_OUTPUT_ROOT,
    audit_research_case_library,
    audit_research_execution_cache,
    audit_research_labels,
    audit_research_runs,
    build_cases_from_regret_casebook,
    build_cluster_labels,
    build_event_labels,
    build_oracle_diagnostic_set,
    build_regime_labels,
    compare_research_runs,
    plan_research_execution,
    prune_research_execution_cache,
    query_research_cases,
    query_research_runs,
    register_research_case,
    register_research_run,
    resume_research_execution,
    run_research_execution_batch,
)

console = Console()


labels_app = typer.Typer(help="Research regime/event/cluster label store。", no_args_is_help=True)
runs_app = typer.Typer(
    help="Research run registry and experiment warehouse。", no_args_is_help=True
)
research_execution_app = typer.Typer(
    help="Research execution cache/checkpoint engine。", no_args_is_help=True
)
cases_app = typer.Typer(help="Research case library。", no_args_is_help=True)


def register_research_foundation_commands(research_app: typer.Typer) -> None:
    research_app.add_typer(labels_app, name="labels")
    research_app.add_typer(runs_app, name="runs")
    research_app.add_typer(research_execution_app, name="execution")
    research_app.add_typer(cases_app, name="cases")


@labels_app.command("build-regime-labels")
def labels_build_regime_labels_command(
    as_of_date: Annotated[
        str,
        typer.Option("--as-of-date", "--as-of", help="Label as-of date。"),
    ] = "2022-12-01",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research labels 输出目录。"),
    ] = DEFAULT_RESEARCH_LABEL_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: build_regime_labels(as_of_date=as_of_date, output_root=output_root)
    )
    _print_status("Regime labels", str(payload["status"]))
    _print_summary(payload)


@labels_app.command("build-event-labels")
def labels_build_event_labels_command(
    as_of_date: Annotated[
        str,
        typer.Option("--as-of-date", "--as-of", help="Label as-of date。"),
    ] = "2022-12-01",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research labels 输出目录。"),
    ] = DEFAULT_RESEARCH_LABEL_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: build_event_labels(as_of_date=as_of_date, output_root=output_root)
    )
    _print_status("Event labels", str(payload["status"]))
    _print_summary(payload)


@labels_app.command("build-cluster-labels")
def labels_build_cluster_labels_command(
    as_of_date: Annotated[
        str,
        typer.Option("--as-of-date", "--as-of", help="Label as-of date。"),
    ] = "2022-12-01",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research labels 输出目录。"),
    ] = DEFAULT_RESEARCH_LABEL_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: build_cluster_labels(as_of_date=as_of_date, output_root=output_root)
    )
    _print_status("Cluster labels", str(payload["status"]))
    _print_summary(payload)


@labels_app.command("audit")
def labels_audit_command(
    as_of_date: Annotated[
        str,
        typer.Option("--as-of-date", "--as-of", help="Label as-of date。"),
    ] = "2022-12-01",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research labels 输出目录。"),
    ] = DEFAULT_RESEARCH_LABEL_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: audit_research_labels(as_of_date=as_of_date, output_root=output_root)
    )
    _print_status("Research label audit", str(payload["status"]))
    _print_summary(payload)


@runs_app.command("register")
def runs_register_command(
    research_id: Annotated[
        str,
        typer.Option("--research-id", help="Research id。"),
    ] = "portfolio_decision_problem_v1",
    strategy_id: Annotated[
        str,
        typer.Option("--strategy-id", help="Strategy id。"),
    ] = "value_surface_baseline",
    run_type: Annotated[
        str,
        typer.Option("--run-type", help="Run type。"),
    ] = "validation_only_baseline",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research runs 输出目录。"),
    ] = DEFAULT_RESEARCH_RUN_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: register_research_run(
            research_id=research_id,
            strategy_id=strategy_id,
            run_type=run_type,
            output_root=output_root,
        )
    )
    _print_status("Research run register", str(payload["status"]))
    _print_summary(payload)


@runs_app.command("query")
def runs_query_command(
    research_id: Annotated[str, typer.Option("--research-id", help="Research id。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research runs 输出目录。"),
    ] = DEFAULT_RESEARCH_RUN_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: query_research_runs(research_id=research_id, output_root=output_root)
    )
    _print_status("Research run query", str(payload["status"]))
    _print_summary(payload)


@runs_app.command("compare")
def runs_compare_command(
    run_id: Annotated[
        list[str] | None,
        typer.Option("--run-id", help="Run id，可重复。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research runs 输出目录。"),
    ] = DEFAULT_RESEARCH_RUN_OUTPUT_ROOT,
) -> None:
    ids = tuple(run_id or [])
    if not ids:
        registered = register_research_run(output_root=output_root)
        ids = (str(registered["run_record"]["run_id"]),)
    payload = _build_research_payload(
        lambda: compare_research_runs(run_ids=ids, output_root=output_root)
    )
    _print_status("Research run compare", str(payload["status"]))
    _print_summary(payload)


@runs_app.command("audit")
def runs_audit_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research runs 输出目录。"),
    ] = DEFAULT_RESEARCH_RUN_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(lambda: audit_research_runs(output_root=output_root))
    _print_status("Research run audit", str(payload["status"]))
    _print_summary(payload)


@research_execution_app.command("plan")
def research_execution_plan_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research execution 输出目录。"),
    ] = DEFAULT_RESEARCH_EXECUTION_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(lambda: plan_research_execution(output_root=output_root))
    _print_status("Research execution plan", str(payload["status"]))
    _print_summary(payload)


@research_execution_app.command("run-batch")
def research_execution_run_batch_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research execution 输出目录。"),
    ] = DEFAULT_RESEARCH_EXECUTION_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(lambda: run_research_execution_batch(output_root=output_root))
    _print_status("Research execution batch", str(payload["status"]))
    _print_summary(payload)


@research_execution_app.command("resume")
def research_execution_resume_command(
    checkpoint_id: Annotated[str, typer.Option("--checkpoint-id", help="Checkpoint id。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research execution 输出目录。"),
    ] = DEFAULT_RESEARCH_EXECUTION_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: resume_research_execution(checkpoint_id=checkpoint_id, output_root=output_root)
    )
    _print_status("Research execution resume", str(payload["status"]))
    _print_summary(payload)


@research_execution_app.command("cache-audit")
def research_execution_cache_audit_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research execution 输出目录。"),
    ] = DEFAULT_RESEARCH_EXECUTION_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: audit_research_execution_cache(output_root=output_root)
    )
    _print_status("Research execution cache audit", str(payload["status"]))
    _print_summary(payload)


@research_execution_app.command("cache-prune")
def research_execution_cache_prune_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research execution 输出目录。"),
    ] = DEFAULT_RESEARCH_EXECUTION_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: prune_research_execution_cache(output_root=output_root)
    )
    _print_status("Research execution cache prune", str(payload["status"]))
    _print_summary(payload)


@cases_app.command("register")
def cases_register_command(
    case_id: Annotated[
        str,
        typer.Option("--case-id", help="Case id。"),
    ] = "baseline_false_risk_off_placeholder",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research case library 输出目录。"),
    ] = DEFAULT_RESEARCH_CASE_LIBRARY_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: register_research_case(case_id=case_id, output_root=output_root)
    )
    _print_status("Research case register", str(payload["status"]))
    _print_summary(payload)


@cases_app.command("query")
def cases_query_command(
    case_type: Annotated[
        str | None,
        typer.Option("--case-type", help="Case type。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research case library 输出目录。"),
    ] = DEFAULT_RESEARCH_CASE_LIBRARY_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: query_research_cases(case_type=case_type, output_root=output_root)
    )
    _print_status("Research case query", str(payload["status"]))
    _print_summary(payload)


@cases_app.command("build-from-regret-casebook")
def cases_build_from_regret_casebook_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research case library 输出目录。"),
    ] = DEFAULT_RESEARCH_CASE_LIBRARY_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(
        lambda: build_cases_from_regret_casebook(output_root=output_root)
    )
    _print_status("Research cases from regret casebook", str(payload["status"]))
    _print_summary(payload)


@cases_app.command("build-oracle-diagnostic-set")
def cases_build_oracle_diagnostic_set_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research case library 输出目录。"),
    ] = DEFAULT_RESEARCH_CASE_LIBRARY_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(lambda: build_oracle_diagnostic_set(output_root=output_root))
    _print_status("Oracle diagnostic case set", str(payload["status"]))
    _print_summary(payload)


@cases_app.command("audit")
def cases_audit_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Research case library 输出目录。"),
    ] = DEFAULT_RESEARCH_CASE_LIBRARY_OUTPUT_ROOT,
) -> None:
    payload = _build_research_payload(lambda: audit_research_case_library(output_root=output_root))
    _print_status("Research case library audit", str(payload["status"]))
    _print_summary(payload)


def _build_research_payload(builder: Callable[[], dict[str, object]]) -> dict[str, object]:
    try:
        return builder()
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc


def _print_summary(payload: dict[str, object]) -> None:
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        return
    compact = "; ".join(f"{key}={value}" for key, value in list(summary.items())[:6])
    if compact:
        console.print(compact)


def _print_status(label: str, status: str) -> None:
    style = "green" if status in {"PASS", "READY"} else "yellow"
    console.print(f"[{style}]{label}: {status}[/{style}]")
