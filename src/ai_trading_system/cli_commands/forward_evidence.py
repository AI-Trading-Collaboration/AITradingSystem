from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.controlled_strategy_batch import (
    DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    run_forward_evidence_continuity_extension,
    run_forward_evidence_daily_continuity_maturity_tracker,
    run_forward_evidence_daily_continuity_review,
    run_forward_evidence_maturity_tracker,
)
from ai_trading_system.current_subscription_qualification import (
    DEFAULT_CONTROL_AUDIT_REPORT_PATH,
    DEFAULT_CONTROLLED_BENCHMARK_BATCH_REPORT_PATH,
    DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_REPORT_PATH,
    DEFAULT_FORWARD_CAPTURE_CONTRACT_PATH,
    DEFAULT_FORWARD_DAILY_DRY_RUN_LEDGER_PATH,
    DEFAULT_FORWARD_DRY_RUN_ARCHIVE_OUTPUT_ROOT,
    DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
    DEFAULT_SOURCE_REQUIREMENT_MATRIX_PATH,
    capture_forward_evidence_daily_dry_run,
    capture_forward_evidence_dry_run_archive,
    classify_forward_evidence_requirement,
    validate_forward_capture_contract,
)
from ai_trading_system.data_foundation import (
    DEFAULT_FORWARD_EVIDENCE_OUTPUT_ROOT,
    PRIMARY_RESEARCH_START,
    audit_forward_evidence,
    capture_forward_evidence,
    report_forward_evidence,
    update_forward_outcomes,
)


def _parse_optional_date(raw: str | None) -> date | None:
    if not raw:
        return None
    return date.fromisoformat(raw)


console = Console()
forward_evidence_app = typer.Typer(help="Forward evidence capture and daily archive。")


@forward_evidence_app.command("capture-daily")
def forward_evidence_capture_daily_command(
    as_of_date: Annotated[
        str,
        typer.Option("--as-of-date", "--as-of", help="Archive as-of date。"),
    ] = PRIMARY_RESEARCH_START,
    feature_snapshot_id: Annotated[
        str,
        typer.Option("--feature-snapshot-id", help="Linked PIT feature snapshot id。"),
    ] = "pit_snapshot_required",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Forward evidence 输出目录。"),
    ] = DEFAULT_FORWARD_EVIDENCE_OUTPUT_ROOT,
) -> None:
    payload = capture_forward_evidence(
        as_of_date=as_of_date,
        feature_snapshot_id=feature_snapshot_id,
        output_root=output_root,
    )
    _print_payload(payload)


@forward_evidence_app.command("capture-dry-run")
def forward_evidence_capture_dry_run_command(
    benchmark_report: Annotated[
        Path,
        typer.Option("--benchmark-report", help="TRADING-760 benchmark report JSON。"),
    ] = DEFAULT_CONTROLLED_BENCHMARK_BATCH_REPORT_PATH,
    control_audit: Annotated[
        Path,
        typer.Option("--control-audit", help="TRADING-760 control audit report JSON。"),
    ] = DEFAULT_CONTROL_AUDIT_REPORT_PATH,
    feature_snapshot_reference: Annotated[
        str,
        typer.Option("--feature-snapshot-reference", help="PIT feature snapshot reference。"),
    ] = "pit_snapshot_required",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-760 forward dry-run archive 输出目录。"),
    ] = DEFAULT_FORWARD_DRY_RUN_ARCHIVE_OUTPUT_ROOT,
) -> None:
    payload = capture_forward_evidence_dry_run_archive(
        benchmark_report_path=benchmark_report,
        control_audit_path=control_audit,
        feature_snapshot_reference=feature_snapshot_reference,
        output_root=output_root,
    )
    _print_payload(payload)


@forward_evidence_app.command("capture-dry-run-daily")
def forward_evidence_capture_dry_run_daily_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--as-of-date", help="Daily dry-run archive as-of date。"),
    ] = None,
    benchmark_expansion: Annotated[
        Path,
        typer.Option("--benchmark-expansion", help="TRADING-765 benchmark expansion JSON。"),
    ] = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_REPORT_PATH,
    control_audit: Annotated[
        Path,
        typer.Option("--control-audit", help="TRADING-760 control audit report JSON。"),
    ] = DEFAULT_CONTROL_AUDIT_REPORT_PATH,
    feature_snapshot_reference: Annotated[
        str,
        typer.Option("--feature-snapshot-reference", help="Daily PIT feature snapshot reference。"),
    ] = "daily_score_decision_snapshot",
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-768 daily dry-run archive 输出目录。"),
    ] = DEFAULT_FORWARD_DRY_RUN_ARCHIVE_OUTPUT_ROOT,
    ledger_path: Annotated[
        Path,
        typer.Option("--ledger-path", help="TRADING-768 forward evidence ledger JSONL。"),
    ] = DEFAULT_FORWARD_DAILY_DRY_RUN_LEDGER_PATH,
) -> None:
    payload = capture_forward_evidence_daily_dry_run(
        as_of_date=_parse_optional_date(as_of),
        benchmark_report_path=benchmark_expansion,
        control_audit_path=control_audit,
        feature_snapshot_reference=feature_snapshot_reference,
        output_root=output_root,
        ledger_path=ledger_path,
    )
    _print_payload(payload)


@forward_evidence_app.command("maturity-tracker")
def forward_evidence_maturity_tracker_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-777 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="FMP 主价格缓存 CSV。"),
    ] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="Marketstack 第二源价格缓存 CSV。"),
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_PATH,
    ledger_path: Annotated[
        Path,
        typer.Option("--ledger-path", help="TRADING-768 forward evidence ledger JSONL。"),
    ] = DEFAULT_FORWARD_DAILY_DRY_RUN_LEDGER_PATH,
    benchmark_expansion: Annotated[
        Path,
        typer.Option("--benchmark-expansion", help="TRADING-765 benchmark expansion JSON。"),
    ] = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_REPORT_PATH,
    control_audit: Annotated[
        Path,
        typer.Option("--control-audit", help="TRADING-760 control audit JSON。"),
    ] = DEFAULT_CONTROL_AUDIT_REPORT_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-777 maturity tracker 输出目录。"),
    ] = DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT,
) -> None:
    payload = run_forward_evidence_maturity_tracker(
        config_path=config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        ledger_path=ledger_path,
        benchmark_expansion_path=benchmark_expansion,
        control_audit_path=control_audit,
        value_surface_expansion_path=value_surface_expansion,
        output_root=output_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_payload(payload)


@forward_evidence_app.command("daily-continuity-maturity-tracker")
def forward_evidence_daily_continuity_maturity_tracker_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-782 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="FMP 主价格缓存 CSV。"),
    ] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="Marketstack 第二源价格缓存 CSV。"),
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_PATH,
    ledger_path: Annotated[
        Path,
        typer.Option("--ledger-path", help="TRADING-768 forward evidence ledger JSONL。"),
    ] = DEFAULT_FORWARD_DAILY_DRY_RUN_LEDGER_PATH,
    forward_maturity: Annotated[
        Path,
        typer.Option("--forward-maturity", help="TRADING-777 maturity tracker JSON。"),
    ] = DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT
    / "forward_evidence_maturity_tracker.json",
    benchmark_expansion: Annotated[
        Path,
        typer.Option("--benchmark-expansion", help="TRADING-765 benchmark expansion JSON。"),
    ] = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_REPORT_PATH,
    control_audit: Annotated[
        Path,
        typer.Option("--control-audit", help="TRADING-760 control audit JSON。"),
    ] = DEFAULT_CONTROL_AUDIT_REPORT_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-782 continuity tracker 输出目录。"),
    ] = DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT,
) -> None:
    payload = run_forward_evidence_daily_continuity_maturity_tracker(
        config_path=config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        ledger_path=ledger_path,
        forward_maturity_path=forward_maturity,
        benchmark_expansion_path=benchmark_expansion,
        control_audit_path=control_audit,
        value_surface_expansion_path=value_surface_expansion,
        output_root=output_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_payload(payload)


@forward_evidence_app.command("daily-continuity-review")
def forward_evidence_daily_continuity_review_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-787 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="FMP 主价格缓存 CSV。"),
    ] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="Marketstack 第二源价格缓存 CSV。"),
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_PATH,
    ledger_path: Annotated[
        Path,
        typer.Option("--ledger-path", help="TRADING-768 forward evidence ledger JSONL。"),
    ] = DEFAULT_FORWARD_DAILY_DRY_RUN_LEDGER_PATH,
    benchmark_expansion: Annotated[
        Path,
        typer.Option("--benchmark-expansion", help="TRADING-765 benchmark expansion JSON。"),
    ] = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_REPORT_PATH,
    control_audit: Annotated[
        Path,
        typer.Option("--control-audit", help="TRADING-760 control audit JSON。"),
    ] = DEFAULT_CONTROL_AUDIT_REPORT_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-787 continuity review 输出目录。"),
    ] = DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT,
) -> None:
    payload = run_forward_evidence_daily_continuity_review(
        config_path=config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        ledger_path=ledger_path,
        benchmark_expansion_path=benchmark_expansion,
        control_audit_path=control_audit,
        value_surface_expansion_path=value_surface_expansion,
        output_root=output_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_payload(payload)


@forward_evidence_app.command("continuity-extension")
def forward_evidence_continuity_extension_command(
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="TRADING-793 next-stage controlled config。"),
    ] = DEFAULT_CONTROLLED_STRATEGY_NEXT_STAGE_CONFIG_PATH,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="FMP 主价格缓存 CSV。"),
    ] = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Annotated[
        Path,
        typer.Option("--marketstack-prices-path", help="Marketstack 第二源价格缓存 CSV。"),
    ] = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Annotated[
        Path,
        typer.Option("--rates-path", help="FRED rates cache for validate-data gate。"),
    ] = DEFAULT_RATES_PATH,
    ledger_path: Annotated[
        Path,
        typer.Option("--ledger-path", help="TRADING-768 forward evidence ledger JSONL。"),
    ] = DEFAULT_FORWARD_DAILY_DRY_RUN_LEDGER_PATH,
    benchmark_expansion: Annotated[
        Path,
        typer.Option("--benchmark-expansion", help="TRADING-765 benchmark expansion JSON。"),
    ] = DEFAULT_CONTROLLED_BENCHMARK_EXPANSION_REPORT_PATH,
    control_audit: Annotated[
        Path,
        typer.Option("--control-audit", help="TRADING-760 control audit JSON。"),
    ] = DEFAULT_CONTROL_AUDIT_REPORT_PATH,
    value_surface_expansion: Annotated[
        Path,
        typer.Option(
            "--value-surface-expansion", help="TRADING-775 value surface expansion JSON。"
        ),
    ] = DEFAULT_VALUE_SURFACE_EXPANSION_PATH,
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", help="validate-data as-of date；默认使用价格缓存最大日期。"),
    ] = None,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-793 continuity extension 输出目录。"),
    ] = DEFAULT_FORWARD_MATURITY_OUTPUT_ROOT,
) -> None:
    payload = run_forward_evidence_continuity_extension(
        config_path=config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        ledger_path=ledger_path,
        benchmark_expansion_path=benchmark_expansion,
        control_audit_path=control_audit,
        value_surface_expansion_path=value_surface_expansion,
        output_root=output_root,
        as_of_date=_parse_optional_date(as_of),
    )
    _print_payload(payload)


@forward_evidence_app.command("update-outcomes")
def forward_evidence_update_outcomes_command(
    archive_id: Annotated[str, typer.Option("--archive-id", help="Archive id。")],
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Forward evidence 输出目录。"),
    ] = DEFAULT_FORWARD_EVIDENCE_OUTPUT_ROOT,
) -> None:
    payload = update_forward_outcomes(archive_id=archive_id, output_root=output_root)
    _print_payload(payload)


@forward_evidence_app.command("audit")
def forward_evidence_audit_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Forward evidence 输出目录。"),
    ] = DEFAULT_FORWARD_EVIDENCE_OUTPUT_ROOT,
) -> None:
    payload = audit_forward_evidence(output_root=output_root)
    _print_payload(payload)


@forward_evidence_app.command("report")
def forward_evidence_report_command(
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="Forward evidence 输出目录。"),
    ] = DEFAULT_FORWARD_EVIDENCE_OUTPUT_ROOT,
) -> None:
    payload = report_forward_evidence(output_root=output_root)
    _print_payload(payload)


@forward_evidence_app.command("classify-requirement")
def forward_evidence_classify_requirement_command(
    source_requirement_matrix: Annotated[
        Path,
        typer.Option("--source-requirement-matrix", help="TRADING-737 requirement matrix JSON。"),
    ] = DEFAULT_SOURCE_REQUIREMENT_MATRIX_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-743 reclassification 输出目录。"),
    ] = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
) -> None:
    payload = classify_forward_evidence_requirement(
        source_requirement_matrix_path=source_requirement_matrix,
        output_root=output_root,
    )
    _print_payload(payload)


@forward_evidence_app.command("validate-capture-contract")
def forward_evidence_validate_capture_contract_command(
    capture_contract: Annotated[
        Path,
        typer.Option("--capture-contract", help="Forward evidence capture contract JSON。"),
    ] = DEFAULT_FORWARD_CAPTURE_CONTRACT_PATH,
    output_root: Annotated[
        Path,
        typer.Option("--output-root", help="TRADING-743 contract validation 输出目录。"),
    ] = DEFAULT_SOURCE_QUALIFICATION_V2_OUTPUT_ROOT,
) -> None:
    payload = validate_forward_capture_contract(
        capture_contract_path=capture_contract,
        output_root=output_root,
    )
    _print_payload(payload)


def _print_payload(payload: dict[str, object]) -> None:
    status = str(payload.get("status", "UNKNOWN"))
    style = "green" if status == "PASS" else "yellow" if "WARNING" in status else "red"
    console.print(
        f"[{style}]{payload.get('title', payload.get('report_type'))}：{status}[/{style}]"
    )
    summary = payload.get("summary")
    if isinstance(summary, dict):
        for key in sorted(summary):
            console.print(f"{key}={summary[key]}")
    console.print("production_effect=none；broker_action=none")
