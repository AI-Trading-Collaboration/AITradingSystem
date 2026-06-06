from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from ai_trading_system.config import PROJECT_ROOT, load_universe, load_watchlist
from ai_trading_system.fmp_forward_pit import DEFAULT_FMP_FORWARD_PIT_NORMALIZED_DIR
from ai_trading_system.valuation import (
    build_valuation_review_report,
    default_valuation_review_report_path,
    default_valuation_validation_report_path,
    load_valuation_snapshot_store,
    validate_valuation_snapshot_store,
    write_valuation_review_report,
    write_valuation_validation_report,
)
from ai_trading_system.valuation_sources import (
    default_eodhd_earnings_trends_fetch_report_path,
    default_eodhd_earnings_trends_raw_dir,
    default_fmp_analyst_estimate_history_dir,
    default_fmp_analyst_history_validation_report_path,
    default_fmp_historical_valuation_fetch_report_path,
    default_fmp_historical_valuation_raw_dir,
    default_fmp_valuation_fetch_report_path,
    fetch_eodhd_earnings_trend_snapshots,
    fetch_fmp_historical_valuation_snapshots,
    fetch_fmp_valuation_snapshots,
    import_valuation_snapshots_from_csv,
    validate_fmp_analyst_estimate_history,
    write_eodhd_earnings_trends_fetch_report,
    write_eodhd_earnings_trends_raw_payload,
    write_fmp_analyst_estimate_history_snapshots,
    write_fmp_analyst_history_validation_report,
    write_fmp_historical_valuation_fetch_report,
    write_fmp_historical_valuation_raw_payloads,
    write_fmp_valuation_fetch_report,
    write_valuation_csv_import_report,
    write_valuation_snapshots_as_yaml,
)

console = Console()
valuation_app = typer.Typer(help="估值、预期和拥挤度快照管理。", no_args_is_help=True)

DEFAULT_FMP_ANALYST_ESTIMATE_HISTORY_DIR = default_fmp_analyst_estimate_history_dir(
    PROJECT_ROOT / "data" / "raw"
)
DEFAULT_FMP_HISTORICAL_VALUATION_RAW_DIR = default_fmp_historical_valuation_raw_dir(
    PROJECT_ROOT / "data" / "raw"
)
DEFAULT_EODHD_EARNINGS_TRENDS_RAW_DIR = default_eodhd_earnings_trends_raw_dir(
    PROJECT_ROOT / "data" / "raw"
)


@valuation_app.command("fetch-fmp")
def fetch_fmp_valuations(
    tickers: Annotated[
        str | None,
        typer.Option(help="逗号分隔 ticker；未提供时使用 universe 的 AI core_watchlist。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="写入 FMP 估值快照 YAML 的目录。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "valuation_snapshots",
    analyst_history_dir: Annotated[
        Path,
        typer.Option(help="读取并写入 FMP analyst-estimates 原始历史快照的目录。"),
    ] = DEFAULT_FMP_ANALYST_ESTIMATE_HISTORY_DIR,
    pit_normalized_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "读取 FMP forward-only PIT 标准化 CSV 的文件或目录；"
                "默认使用 data/processed/pit_snapshots。"
            )
        ),
    ] = DEFAULT_FMP_FORWARD_PIT_NORMALIZED_DIR,
    as_of: Annotated[
        str | None,
        typer.Option(help="估值评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown FMP 拉取报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 估值快照校验报告输出路径。"),
    ] = None,
    api_key_env: Annotated[
        str,
        typer.Option(help="读取 FMP API key 的环境变量名。"),
    ] = "FMP_API_KEY",
    analyst_estimate_limit: Annotated[
        int,
        typer.Option(help="每个 ticker 拉取的 annual analyst estimate 记录数。"),
    ] = 10,
) -> None:
    """从 Financial Modeling Prep 拉取估值和预期快照。"""
    fetch_date = _parse_date(as_of) if as_of else date.today()
    selected_tickers = (
        _parse_csv_items(tickers) if tickers else load_universe().ai_chain.get("core_watchlist", [])
    )
    api_key = os.getenv(api_key_env)
    if not api_key:
        console.print(f"[red]未找到环境变量 {api_key_env}，已停止 FMP 拉取。[/red]")
        raise typer.Exit(code=1)

    fetch_report_output = output_path or default_fmp_valuation_fetch_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )
    validation_output = validation_report_path or default_valuation_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )

    try:
        fetch_report = fetch_fmp_valuation_snapshots(
            selected_tickers,
            api_key,
            fetch_date,
            analyst_history_dir=analyst_history_dir,
            pit_normalized_path=pit_normalized_path,
            valuation_history_dir=output_dir,
            captured_at=fetch_date,
            analyst_estimate_limit=analyst_estimate_limit,
        )
    except ValueError as exc:
        console.print(f"[red]FMP 参数错误：{exc}[/red]")
        raise typer.Exit(code=1) from exc

    write_fmp_valuation_fetch_report(fetch_report, fetch_report_output)
    status_style = (
        "green" if fetch_report.status == "PASS" else "yellow" if fetch_report.passed else "red"
    )
    console.print(f"[{status_style}]FMP 估值拉取状态：{fetch_report.status}[/{status_style}]")
    console.print(f"拉取报告：{fetch_report_output}")
    console.print(
        f"请求标的：{', '.join(fetch_report.requested_tickers)}；"
        f"返回记录：{fetch_report.row_count}；生成快照：{fetch_report.imported_count}"
    )
    console.print(f"错误数：{fetch_report.error_count}；警告数：{fetch_report.warning_count}")
    if not fetch_report.passed:
        raise typer.Exit(code=1)

    history_paths = write_fmp_analyst_estimate_history_snapshots(
        fetch_report.analyst_estimate_history_snapshots,
        analyst_history_dir,
    )
    written_paths = write_valuation_snapshots_as_yaml(fetch_report.snapshots, output_dir)
    validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(output_dir),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=fetch_date,
    )
    write_valuation_validation_report(validation_report, validation_output)

    validation_style = (
        "green"
        if validation_report.status == "PASS"
        else "yellow" if validation_report.passed else "red"
    )
    console.print(f"写入 YAML：{len(written_paths)} 个文件 -> {output_dir}")
    console.print(f"写入 analyst history：{len(history_paths)} 个文件 -> {analyst_history_dir}")
    console.print(
        f"[{validation_style}]估值快照校验状态：" f"{validation_report.status}[/{validation_style}]"
    )
    console.print(f"校验报告：{validation_output}")
    if not validation_report.passed:
        raise typer.Exit(code=1)


@valuation_app.command("fetch-fmp-valuation-history")
def fetch_fmp_valuation_history(
    tickers: Annotated[
        str | None,
        typer.Option(help="逗号分隔 ticker；未提供时使用 universe 的 AI core_watchlist。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="写入历史估值快照 YAML 的目录。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "valuation_snapshots",
    raw_output_dir: Annotated[
        Path,
        typer.Option(help="写入 FMP historical key-metrics/ratios 原始 JSON 的目录。"),
    ] = DEFAULT_FMP_HISTORICAL_VALUATION_RAW_DIR,
    as_of: Annotated[
        str | None,
        typer.Option(help="拉取评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown FMP 历史估值拉取报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 估值快照校验报告输出路径。"),
    ] = None,
    api_key_env: Annotated[
        str,
        typer.Option(help="读取 FMP API key 的环境变量名。"),
    ] = "FMP_API_KEY",
    period: Annotated[
        str,
        typer.Option(help="FMP historical key-metrics/ratios period，annual 或 quarter。"),
    ] = "annual",
    limit: Annotated[
        int,
        typer.Option(help="每个 ticker 拉取的历史记录数，至少 3。"),
    ] = 5,
) -> None:
    """从 FMP 拉取 historical key-metrics/ratios，回填估值分位历史。"""
    fetch_date = _parse_date(as_of) if as_of else date.today()
    selected_tickers = (
        _parse_csv_items(tickers) if tickers else load_universe().ai_chain.get("core_watchlist", [])
    )
    api_key = os.getenv(api_key_env)
    if not api_key:
        console.print(f"[red]未找到环境变量 {api_key_env}，已停止 FMP 历史估值拉取。[/red]")
        raise typer.Exit(code=1)

    fetch_report_output = output_path or default_fmp_historical_valuation_fetch_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )
    validation_output = validation_report_path or default_valuation_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )

    try:
        fetch_report = fetch_fmp_historical_valuation_snapshots(
            selected_tickers,
            api_key,
            fetch_date,
            captured_at=fetch_date,
            period=period,
            limit=limit,
        )
    except ValueError as exc:
        console.print(f"[red]FMP 历史估值参数错误：{exc}[/red]")
        raise typer.Exit(code=1) from exc

    write_fmp_historical_valuation_fetch_report(fetch_report, fetch_report_output)
    status_style = (
        "green" if fetch_report.status == "PASS" else "yellow" if fetch_report.passed else "red"
    )
    console.print(
        f"[{status_style}]FMP 历史估值拉取状态：" f"{fetch_report.status}[/{status_style}]"
    )
    console.print(f"拉取报告：{fetch_report_output}")
    console.print(
        f"请求标的：{', '.join(fetch_report.requested_tickers)}；"
        f"返回记录：{fetch_report.row_count}；生成历史快照：{fetch_report.imported_count}"
    )
    console.print(f"错误数：{fetch_report.error_count}；警告数：{fetch_report.warning_count}")
    if not fetch_report.passed:
        raise typer.Exit(code=1)

    raw_paths = write_fmp_historical_valuation_raw_payloads(
        fetch_report.raw_payloads,
        raw_output_dir,
    )
    written_paths = write_valuation_snapshots_as_yaml(fetch_report.snapshots, output_dir)
    validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(output_dir),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=fetch_date,
    )
    write_valuation_validation_report(validation_report, validation_output)

    validation_style = (
        "green"
        if validation_report.status == "PASS"
        else "yellow" if validation_report.passed else "red"
    )
    console.print(f"写入原始历史 payload：{len(raw_paths)} 个文件 -> {raw_output_dir}")
    console.print(f"写入历史估值 YAML：{len(written_paths)} 个文件 -> {output_dir}")
    console.print(
        f"[{validation_style}]估值快照校验状态：" f"{validation_report.status}[/{validation_style}]"
    )
    console.print(f"校验报告：{validation_output}")
    if not validation_report.passed:
        raise typer.Exit(code=1)


@valuation_app.command("fetch-eodhd-trends")
def fetch_eodhd_valuation_trends(
    tickers: Annotated[
        str | None,
        typer.Option(help="逗号分隔 ticker；未提供时使用 universe 的 AI core_watchlist。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="写入 EODHD trend 合并估值快照 YAML 的目录。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "valuation_snapshots",
    base_valuation_dir: Annotated[
        Path | None,
        typer.Option(help="读取基础估值快照的目录；默认与 output_dir 相同。"),
    ] = None,
    raw_output_dir: Annotated[
        Path,
        typer.Option(help="写入 EODHD Earnings Trends 原始 JSON 的目录。"),
    ] = DEFAULT_EODHD_EARNINGS_TRENDS_RAW_DIR,
    as_of: Annotated[
        str | None,
        typer.Option(help="拉取评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown EODHD trends 拉取报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 估值快照校验报告输出路径。"),
    ] = None,
    api_key_env: Annotated[
        str,
        typer.Option(help="读取 EODHD API key 的环境变量名。"),
    ] = "EODHD_API_KEY",
) -> None:
    """从 EODHD Earnings Trends 拉取当前 EPS trend，合并进估值快照。"""
    fetch_date = _parse_date(as_of) if as_of else date.today()
    selected_tickers = (
        _parse_csv_items(tickers) if tickers else load_universe().ai_chain.get("core_watchlist", [])
    )
    api_key = os.getenv(api_key_env)
    if not api_key:
        console.print(f"[red]未找到环境变量 {api_key_env}，已停止 EODHD trends 拉取。[/red]")
        raise typer.Exit(code=1)

    fetch_report_output = output_path or default_eodhd_earnings_trends_fetch_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )
    validation_output = validation_report_path or default_valuation_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )
    base_input_dir = base_valuation_dir or output_dir

    try:
        fetch_report = fetch_eodhd_earnings_trend_snapshots(
            selected_tickers,
            api_key,
            fetch_date,
            base_valuation_dir=base_input_dir,
            captured_at=fetch_date,
        )
    except ValueError as exc:
        console.print(f"[red]EODHD trends 参数错误：{exc}[/red]")
        raise typer.Exit(code=1) from exc

    write_eodhd_earnings_trends_fetch_report(fetch_report, fetch_report_output)
    status_style = (
        "green" if fetch_report.status == "PASS" else "yellow" if fetch_report.passed else "red"
    )
    console.print(
        f"[{status_style}]EODHD trends 拉取状态：" f"{fetch_report.status}[/{status_style}]"
    )
    console.print(f"拉取报告：{fetch_report_output}")
    console.print(
        f"请求标的：{', '.join(fetch_report.requested_tickers)}；"
        f"返回记录：{fetch_report.row_count}；生成合并快照：{fetch_report.imported_count}"
    )
    console.print(f"错误数：{fetch_report.error_count}；警告数：{fetch_report.warning_count}")
    if not fetch_report.passed:
        raise typer.Exit(code=1)

    raw_paths = write_eodhd_earnings_trends_raw_payload(
        fetch_report.raw_payload,
        raw_output_dir,
    )
    written_paths = write_valuation_snapshots_as_yaml(fetch_report.snapshots, output_dir)
    validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(output_dir),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=fetch_date,
    )
    write_valuation_validation_report(validation_report, validation_output)

    validation_style = (
        "green"
        if validation_report.status == "PASS"
        else "yellow" if validation_report.passed else "red"
    )
    console.print(f"写入原始 trend payload：{len(raw_paths)} 个文件 -> {raw_output_dir}")
    console.print(f"写入合并估值 YAML：{len(written_paths)} 个文件 -> {output_dir}")
    console.print(
        f"[{validation_style}]估值快照校验状态：" f"{validation_report.status}[/{validation_style}]"
    )
    console.print(f"校验报告：{validation_output}")
    if not validation_report.passed:
        raise typer.Exit(code=1)


@valuation_app.command("validate-fmp-history")
def validate_fmp_analyst_history_command(
    input_path: Annotated[
        Path,
        typer.Option(help="FMP analyst-estimates 原始历史快照目录或 JSON 文件。"),
    ] = DEFAULT_FMP_ANALYST_ESTIMATE_HISTORY_DIR,
    tickers: Annotated[
        str | None,
        typer.Option(help="逗号分隔 ticker；未提供时校验全部历史快照。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown FMP analyst history 校验报告输出路径。"),
    ] = None,
    max_snapshot_age_days: Annotated[
        int,
        typer.Option(help="历史快照新鲜度警告阈值，单位天。"),
    ] = 7,
) -> None:
    """校验 FMP analyst-estimates 原始历史缓存。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_fmp_analyst_history_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    selected_tickers = _parse_csv_items(tickers) if tickers else None
    try:
        report = validate_fmp_analyst_estimate_history(
            input_path,
            validation_date,
            tickers=selected_tickers,
            max_snapshot_age_days=max_snapshot_age_days,
        )
    except ValueError as exc:
        console.print(f"[red]FMP analyst history 参数错误：{exc}[/red]")
        raise typer.Exit(code=1) from exc
    write_fmp_analyst_history_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]FMP analyst history 校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"快照数量：{report.snapshot_count}；覆盖标的：{report.ticker_count}；"
        f"记录数：{report.record_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)


@valuation_app.command("import-csv")
def import_valuation_csv(
    input_path: Annotated[
        Path,
        typer.Option(help="估值、预期和拥挤度结构化 CSV 输入路径。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option(help="写入估值快照 YAML 的目录。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "valuation_snapshots",
    as_of: Annotated[
        str | None,
        typer.Option(help="导入后校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown CSV 导入报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 估值快照校验报告输出路径。"),
    ] = None,
) -> None:
    """导入结构化估值 CSV，并写入可审计估值快照 YAML。"""
    import_date = _parse_date(as_of) if as_of else date.today()
    import_report_output = output_path or (
        PROJECT_ROOT / "outputs" / "reports" / f"valuation_import_{import_date}.md"
    )
    validation_output = validation_report_path or default_valuation_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        import_date,
    )
    import_report = import_valuation_snapshots_from_csv(input_path)
    write_valuation_csv_import_report(import_report, import_report_output)

    status_style = (
        "green" if import_report.status == "PASS" else "yellow" if import_report.passed else "red"
    )
    console.print(f"[{status_style}]估值 CSV 导入状态：{import_report.status}[/{status_style}]")
    console.print(f"导入报告：{import_report_output}")
    console.print(f"CSV 行数：{import_report.row_count}；导入快照：{import_report.imported_count}")
    console.print(f"错误数：{import_report.error_count}；警告数：{import_report.warning_count}")
    if not import_report.passed:
        raise typer.Exit(code=1)

    written_paths = write_valuation_snapshots_as_yaml(import_report.snapshots, output_dir)
    validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(output_dir),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=import_date,
    )
    write_valuation_validation_report(validation_report, validation_output)

    validation_style = (
        "green"
        if validation_report.status == "PASS"
        else "yellow" if validation_report.passed else "red"
    )
    console.print(f"写入 YAML：{len(written_paths)} 个文件 -> {output_dir}")
    console.print(
        f"[{validation_style}]估值快照校验状态：" f"{validation_report.status}[/{validation_style}]"
    )
    console.print(f"校验报告：{validation_output}")
    if not validation_report.passed:
        raise typer.Exit(code=1)


@valuation_app.command("list")
def list_valuations(
    input_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "valuation_snapshots",
) -> None:
    """列出估值、预期和拥挤度快照。"""
    store = load_valuation_snapshot_store(input_path)

    table = Table(title="估值与拥挤度快照")
    table.add_column("Snapshot")
    table.add_column("Ticker")
    table.add_column("日期")
    table.add_column("来源")
    table.add_column("估值分位")
    table.add_column("评估")
    table.add_column("文件")

    for loaded in sorted(store.loaded, key=lambda item: item.snapshot.snapshot_id):
        snapshot = loaded.snapshot
        percentile = (
            "n/a"
            if snapshot.valuation_percentile is None
            else f"{snapshot.valuation_percentile:.0f}"
        )
        table.add_row(
            snapshot.snapshot_id,
            snapshot.ticker,
            snapshot.as_of.isoformat(),
            _valuation_source_type_label(snapshot.source_type),
            percentile,
            _valuation_assessment_label(snapshot.overall_assessment),
            str(loaded.path),
        )

    console.print(table)
    if not store.loaded:
        console.print("未发现可读取的估值快照。")
    if store.load_errors:
        console.print(
            f"[red]存在 {len(store.load_errors)} 个加载错误，请运行 validate 查看。[/red]"
        )


@valuation_app.command("validate")
def validate_valuations(
    input_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "valuation_snapshots",
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 估值校验报告输出路径。"),
    ] = None,
) -> None:
    """校验估值、预期和拥挤度快照。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_valuation_validation_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(input_path),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=validation_date,
    )
    write_valuation_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]估值快照校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"快照数量：{report.snapshot_count}；覆盖标的：{report.ticker_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@valuation_app.command("review")
def review_valuations(
    input_path: Annotated[
        Path,
        typer.Option(help="估值快照 YAML 文件或目录路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "external"
    / "valuation_snapshots",
    as_of: Annotated[
        str | None,
        typer.Option(help="复核日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 估值复核报告输出路径。"),
    ] = None,
) -> None:
    """复核估值、预期和拥挤度快照。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_valuation_review_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(input_path),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=review_date,
    )
    review_report = build_valuation_review_report(validation_report)
    write_valuation_review_report(review_report, report_path)

    status_style = (
        "green"
        if review_report.status == "PASS"
        else ("yellow" if validation_report.passed else "red")
    )
    console.print(f"[{status_style}]估值复核状态：{review_report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"快照数量：{validation_report.snapshot_count}；"
        f"覆盖标的：{validation_report.ticker_count}"
    )
    console.print(
        f"校验错误数：{validation_report.error_count}；"
        f"校验警告数：{validation_report.warning_count}"
    )

    if not validation_report.passed:
        raise typer.Exit(code=1)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须是 YYYY-MM-DD") from exc


def _parse_csv_items(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _valuation_source_type_label(value: str) -> str:
    return {
        "primary_filing": "一手披露",
        "paid_vendor": "付费供应商",
        "manual_input": "手工录入",
        "public_convenience": "公开便利源",
    }.get(value, value)


def _valuation_assessment_label(value: str) -> str:
    return {
        "cheap": "偏便宜",
        "reasonable": "合理",
        "expensive": "偏贵",
        "extreme": "极端",
        "unknown": "未知",
    }.get(value, value)
