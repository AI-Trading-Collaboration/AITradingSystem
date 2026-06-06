from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from ai_trading_system.cli_commands.risk_event_artifacts import (
    DEFAULT_OPENAI_REQUEST_CACHE_PATH,
    DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
    DEFAULT_RISK_EVENT_PREREVIEW_QUEUE_PATH,
    DEFAULT_RISK_EVENT_SINGLE_PREREVIEW_PROFILE,
    DEFAULT_RISK_EVENT_TRIAGED_PREREVIEW_PROFILE,
    _coalesce_profile_value,
    _load_llm_request_profile,
    _parse_csv_items,
    _parse_date,
)
from ai_trading_system.config import (
    DEFAULT_DATA_SOURCES_CONFIG_PATH,
    DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    DEFAULT_RISK_EVENTS_CONFIG_PATH,
    DEFAULT_WATCHLIST_CONFIG_PATH,
    PROJECT_ROOT,
    load_data_sources,
    load_industry_chain,
    load_risk_events,
    load_universe,
    load_watchlist,
)
from ai_trading_system.llm_precheck import load_llm_claim_precheck_input
from ai_trading_system.llm_request_profiles import DEFAULT_LLM_REQUEST_PROFILES_CONFIG_PATH
from ai_trading_system.official_policy_sources import (
    DEFAULT_OFFICIAL_POLICY_PROCESSED_DIR,
    DEFAULT_OFFICIAL_POLICY_RAW_DIR,
    default_official_policy_candidates_path,
    default_official_policy_fetch_report_path,
    fetch_official_policy_sources,
    load_official_policy_candidates_csv,
    write_official_policy_fetch_report,
)
from ai_trading_system.risk_event_candidate_triage import (
    default_risk_event_candidate_triage_csv_path,
    default_risk_event_candidate_triage_input_path,
    default_risk_event_candidate_triage_report_path,
    load_triaged_candidate_ids,
    triage_official_policy_candidates,
    write_risk_event_candidate_triage_csv,
    write_risk_event_candidate_triage_report,
)
from ai_trading_system.risk_event_llm_formal import (
    build_llm_formal_assessment_report,
    default_llm_formal_assessment_report_path,
    write_llm_formal_assessment_outputs,
    write_llm_formal_assessment_report,
)
from ai_trading_system.risk_event_prereview import (
    default_risk_event_openai_prereview_report_path,
    default_risk_event_prereview_report_path,
    import_risk_event_prereview_csv,
    run_openai_risk_event_prereview,
    run_openai_risk_event_prereview_for_official_candidates,
    write_risk_event_prereview_import_report,
    write_risk_event_prereview_queue,
)
from ai_trading_system.risk_event_sources import (
    import_risk_event_occurrences_csv,
    write_risk_event_occurrence_import_report,
    write_risk_event_occurrences_yaml,
)
from ai_trading_system.risk_events import (
    build_risk_event_occurrence_review_report,
    build_risk_event_review_attestation,
    default_risk_event_occurrence_report_path,
    default_risk_events_report_path,
    load_risk_event_occurrence_store,
    validate_risk_event_occurrence_store,
    validate_risk_events_config,
    write_risk_event_occurrence_review_report,
    write_risk_event_review_attestation,
    write_risk_events_validation_report,
)

risk_events_app = typer.Typer(help="风险事件分级和动作规则管理。", no_args_is_help=True)
console = Console()


@risk_events_app.command("list")
def list_risk_events(
    config_path: Annotated[
        Path,
        typer.Option(help="风险事件配置文件路径。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    active_only: Annotated[
        bool,
        typer.Option("--active-only/--all", help="只显示活跃规则，或显示全部规则。"),
    ] = True,
) -> None:
    """列出风险事件分级规则。"""
    risk_events = load_risk_events(config_path)

    levels_table = Table(title="风险等级")
    levels_table.add_column("等级")
    levels_table.add_column("名称")
    levels_table.add_column("AI 仓位乘数")
    levels_table.add_column("人工复核")
    levels_table.add_column("默认动作")
    for level in sorted(risk_events.levels, key=lambda item: item.level):
        levels_table.add_row(
            level.level,
            level.name,
            f"{level.target_ai_exposure_multiplier:.0%}",
            "需要" if level.requires_manual_review else "不需要",
            level.default_action,
        )
    console.print(levels_table)

    rules_table = Table(title="风险事件规则")
    rules_table.add_column("事件")
    rules_table.add_column("等级")
    rules_table.add_column("活跃")
    rules_table.add_column("影响节点")
    rules_table.add_column("相关标的")
    for rule in sorted(risk_events.event_rules, key=lambda item: item.event_id):
        if active_only and not rule.active:
            continue
        rules_table.add_row(
            rule.event_id,
            rule.level,
            "是" if rule.active else "否",
            ", ".join(rule.affected_nodes),
            ", ".join(rule.related_tickers),
        )
    console.print(rules_table)


@risk_events_app.command("validate")
def validate_risk_events(
    config_path: Annotated[
        Path,
        typer.Option(help="风险事件配置文件路径。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    industry_chain_path: Annotated[
        Path,
        typer.Option(help="产业链配置文件路径。"),
    ] = DEFAULT_INDUSTRY_CHAIN_CONFIG_PATH,
    watchlist_path: Annotated[
        Path,
        typer.Option(help="观察池配置文件路径。"),
    ] = DEFAULT_WATCHLIST_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件校验报告输出路径。"),
    ] = None,
) -> None:
    """校验风险事件等级、产业链引用和动作规则。"""
    universe = load_universe()
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_risk_events_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    report = validate_risk_events_config(
        risk_events=load_risk_events(config_path),
        industry_chain=load_industry_chain(industry_chain_path),
        watchlist=load_watchlist(watchlist_path),
        universe=universe,
        as_of=validation_date,
    )
    write_risk_events_validation_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]风险事件校验状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"风险事件规则数：{len(report.config.event_rules)}；活跃：{report.active_rule_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


@risk_events_app.command("list-occurrences")
def list_risk_event_occurrences(
    input_path: Annotated[
        Path,
        typer.Option(help="风险事件发生记录 YAML 文件或目录路径。"),
    ] = DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
) -> None:
    """列出本地风险事件发生记录。"""
    store = load_risk_event_occurrence_store(input_path)

    table = Table(title="风险事件发生记录")
    table.add_column("Occurrence", overflow="fold")
    table.add_column("事件", overflow="fold")
    table.add_column("状态")
    table.add_column("触发日期")
    table.add_column("最近确认")
    table.add_column("证据数")
    table.add_column("文件", overflow="fold")
    for loaded in sorted(store.loaded, key=lambda item: item.occurrence.occurrence_id):
        occurrence = loaded.occurrence
        table.add_row(
            occurrence.occurrence_id,
            occurrence.event_id,
            occurrence.status,
            occurrence.triggered_at.isoformat(),
            occurrence.last_confirmed_at.isoformat(),
            str(len(occurrence.evidence_sources)),
            str(loaded.path),
        )
    console.print(table)
    if not store.loaded:
        console.print("未发现可读取的风险事件发生记录。")
    if store.review_attestations:
        attestation_table = Table(title="风险事件复核声明")
        attestation_table.add_column("Attestation", overflow="fold")
        attestation_table.add_column("复核日期")
        attestation_table.add_column("覆盖窗口")
        attestation_table.add_column("复核人", overflow="fold")
        attestation_table.add_column("结论", overflow="fold")
        attestation_table.add_column("文件", overflow="fold")
        for loaded in sorted(
            store.review_attestations,
            key=lambda item: item.attestation.attestation_id,
        ):
            attestation = loaded.attestation
            attestation_table.add_row(
                attestation.attestation_id,
                attestation.review_date.isoformat(),
                (
                    f"{attestation.coverage_start.isoformat()} 至 "
                    f"{attestation.coverage_end.isoformat()}"
                ),
                attestation.reviewer,
                attestation.review_decision,
                str(loaded.path),
            )
        console.print(attestation_table)
    if store.load_errors:
        console.print(
            "[red]存在 "
            f"{len(store.load_errors)} 个加载错误，请运行 validate-occurrences 查看。[/red]"
        )


@risk_events_app.command("record-review-attestation")
def record_risk_event_review_attestation_command(
    output_dir: Annotated[
        Path,
        typer.Option(help="写入风险事件复核声明 YAML 的目录。"),
    ] = DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
    config_path: Annotated[
        Path,
        typer.Option(help="风险事件规则配置文件路径，用于写入后校验。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="复核日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    reviewer: Annotated[
        str,
        typer.Option(help="复核人或复核角色，必须是真实人工复核责任方。"),
    ] = "",
    rationale: Annotated[
        str,
        typer.Option(help="复核结论理由，说明为何确认没有未记录重大风险事件。"),
    ] = "",
    checked_sources: Annotated[
        str,
        typer.Option(
            help=(
                "逗号分隔的已检查来源范围，例如 official_sources,"
                "paid_vendor_queue,openai_prereview_queue。"
            )
        ),
    ] = "manual_daily_risk_review",
    review_scope: Annotated[
        str,
        typer.Option(help="逗号分隔的复核范围。"),
    ] = "policy_event_occurrences,geopolitical_event_occurrences,risk_event_prereview_queue",
    coverage_start: Annotated[
        str | None,
        typer.Option(help="复核覆盖窗口开始日期，格式为 YYYY-MM-DD，默认等于 as_of。"),
    ] = None,
    coverage_end: Annotated[
        str | None,
        typer.Option(help="复核覆盖窗口结束日期，格式为 YYYY-MM-DD，默认等于 as_of。"),
    ] = None,
    reviewed_at: Annotated[
        str | None,
        typer.Option(help="人工复核日期，格式为 YYYY-MM-DD，默认等于 as_of。"),
    ] = None,
    next_review_due: Annotated[
        str | None,
        typer.Option(help="下次复核日期，格式为 YYYY-MM-DD，默认 as_of 后 1 天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件发生记录校验报告输出路径。"),
    ] = None,
) -> None:
    """记录“已复核且未发现未记录重大风险事件”的人工声明。"""
    review_date = _parse_date(as_of) if as_of else date.today()
    if not reviewer.strip():
        raise typer.BadParameter("必须提供 --reviewer，不能由系统匿名生成复核声明。")
    if not rationale.strip():
        raise typer.BadParameter("必须提供 --rationale，说明人工复核结论依据。")
    checked_source_names = tuple(_parse_csv_items(checked_sources))
    if not checked_source_names:
        raise typer.BadParameter("至少需要一个 --checked-sources 来源范围。")
    scope_items = tuple(_parse_csv_items(review_scope))
    if not scope_items:
        raise typer.BadParameter("至少需要一个 --review-scope 复核范围。")

    attestation = build_risk_event_review_attestation(
        as_of=review_date,
        reviewer=reviewer.strip(),
        rationale=rationale.strip(),
        checked_source_names=checked_source_names,
        coverage_start=_parse_date(coverage_start) if coverage_start else review_date,
        coverage_end=_parse_date(coverage_end) if coverage_end else review_date,
        reviewed_at=_parse_date(reviewed_at) if reviewed_at else review_date,
        next_review_due=(
            _parse_date(next_review_due) if next_review_due else review_date + timedelta(days=1)
        ),
        review_scope=scope_items,
    )
    written_path = write_risk_event_review_attestation(attestation, output_dir)

    report_path = output_path or default_risk_event_occurrence_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        review_date,
    )
    validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(output_dir),
        risk_events=load_risk_events(config_path),
        as_of=review_date,
    )
    review_report = build_risk_event_occurrence_review_report(validation_report)
    write_risk_event_occurrence_review_report(review_report, report_path)

    status_style = (
        "green"
        if review_report.status == "PASS"
        else "yellow" if validation_report.passed else "red"
    )
    console.print(f"风险事件复核声明：{written_path}")
    console.print(f"[{status_style}]风险事件发生记录状态：{review_report.status}[/{status_style}]")
    console.print(f"校验报告：{report_path}")
    console.print(
        f"复核声明数：{validation_report.review_attestation_count}；"
        f"当前有效：{validation_report.current_review_attestation_count}"
    )
    if not validation_report.passed:
        raise typer.Exit(code=1)


@risk_events_app.command("import-occurrences-csv")
def import_risk_event_occurrences_csv_command(
    input_path: Annotated[
        Path,
        typer.Option(help="人工复核后的风险事件发生记录 CSV 输入路径。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option(help="写入风险事件发生记录 YAML 的目录。"),
    ] = DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于导入后校验 event_id。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="导入和校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown CSV 导入报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件发生记录校验报告输出路径。"),
    ] = None,
) -> None:
    """导入人工复核后的风险事件发生记录 CSV，并写入可审计 YAML。"""
    import_date = _parse_date(as_of) if as_of else date.today()
    import_report_output = output_path or (
        PROJECT_ROOT / "outputs" / "reports" / f"risk_event_occurrence_import_{import_date}.md"
    )
    validation_output = validation_report_path or default_risk_event_occurrence_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        import_date,
    )
    import_report = import_risk_event_occurrences_csv(input_path)
    write_risk_event_occurrence_import_report(import_report, import_report_output)

    status_style = (
        "green" if import_report.status == "PASS" else "yellow" if import_report.passed else "red"
    )
    console.print(
        f"[{status_style}]风险事件发生记录 CSV 导入状态："
        f"{import_report.status}[/{status_style}]"
    )
    console.print(f"导入报告：{import_report_output}")
    console.print(
        f"CSV 行数：{import_report.row_count}；" f"发生记录：{import_report.occurrence_count}"
    )
    console.print(f"错误数：{import_report.error_count}；警告数：{import_report.warning_count}")
    if not import_report.passed:
        raise typer.Exit(code=1)

    written_paths = write_risk_event_occurrences_yaml(import_report, output_dir)
    validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(output_dir),
        risk_events=load_risk_events(risk_events_path),
        as_of=import_date,
    )
    review_report = build_risk_event_occurrence_review_report(validation_report)
    write_risk_event_occurrence_review_report(review_report, validation_output)

    validation_style = (
        "green"
        if validation_report.status == "PASS"
        else "yellow" if validation_report.passed else "red"
    )
    console.print(f"写入 YAML：{len(written_paths)} 个文件 -> {output_dir}")
    console.print(
        f"[{validation_style}]风险事件发生记录校验状态："
        f"{validation_report.status}[/{validation_style}]"
    )
    console.print(f"校验报告：{validation_output}")
    if not validation_report.passed:
        raise typer.Exit(code=1)


@risk_events_app.command("fetch-official-sources")
def fetch_official_policy_sources_command(
    raw_dir: Annotated[
        Path,
        typer.Option(help="官方政策/地缘来源 raw payload 输出目录。"),
    ] = DEFAULT_OFFICIAL_POLICY_RAW_DIR,
    processed_dir: Annotated[
        Path,
        typer.Option(help="官方来源待人工复核候选 CSV 输出目录。"),
    ] = DEFAULT_OFFICIAL_POLICY_PROCESSED_DIR,
    as_of: Annotated[
        str | None,
        typer.Option(help="抓取评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(help="抓取窗口起始日期，格式为 YYYY-MM-DD，默认 as_of 前 3 天。"),
    ] = None,
    source_ids: Annotated[
        str,
        typer.Option(help="可选：逗号分隔的 source_id 白名单；为空时抓取全部官方来源。"),
    ] = "",
    limit: Annotated[
        int,
        typer.Option(help="每个可分页来源最多请求的记录数。"),
    ] = 50,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 官方来源抓取报告输出路径。"),
    ] = None,
    download_manifest_path: Annotated[
        Path,
        typer.Option(help="统一 download_manifest.csv 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "download_manifest.csv",
    congress_api_key_env: Annotated[
        str,
        typer.Option(help="读取 Congress.gov API key 的环境变量名。"),
    ] = "CONGRESS_API_KEY",
    govinfo_api_key_env: Annotated[
        str,
        typer.Option(help="读取 GovInfo API key 的环境变量名。"),
    ] = "GOVINFO_API_KEY",
) -> None:
    """抓取低成本官方政策/地缘来源，生成待人工复核候选。"""
    fetch_date = _parse_date(as_of) if as_of else date.today()
    since_date = _parse_date(since) if since else None
    selected_source_ids = _parse_csv_items(source_ids) if source_ids else None
    report_path = output_path or default_official_policy_fetch_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        fetch_date,
    )
    report = fetch_official_policy_sources(
        as_of=fetch_date,
        since=since_date,
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        api_keys={
            "CONGRESS_API_KEY": os.getenv(congress_api_key_env, ""),
            "GOVINFO_API_KEY": os.getenv(govinfo_api_key_env, ""),
        },
        selected_source_ids=selected_source_ids,
        limit=limit,
        download_manifest_path=download_manifest_path,
    )
    write_official_policy_fetch_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]官方政策/地缘来源抓取状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"Raw payload：{report.payload_count}；待复核候选：{report.candidate_count}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    console.print("候选记录保持 pending_review，未写入评分或仓位闸门。")
    if not report.passed:
        raise typer.Exit(code=1)


@risk_events_app.command("triage-official-candidates")
def triage_official_policy_candidates_command(
    processed_dir: Annotated[
        Path,
        typer.Option(help="官方来源候选和 triage CSV 所在 processed 目录。"),
    ] = DEFAULT_OFFICIAL_POLICY_PROCESSED_DIR,
    input_path: Annotated[
        Path | None,
        typer.Option(help="官方来源待复核候选 CSV 输入路径；为空时按 as_of 推导。"),
    ] = None,
    as_of: Annotated[
        str | None,
        typer.Option(help="triage 评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    triage_output_path: Annotated[
        Path | None,
        typer.Option(help="AI 模块相关性 triage CSV 输出路径。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown triage 报告输出路径。"),
    ] = None,
) -> None:
    """按 AI 模块相关性分类官方政策/地缘候选，降低无明显联系项优先级。"""
    triage_date = _parse_date(as_of) if as_of else date.today()
    candidate_input_path = input_path or default_risk_event_candidate_triage_input_path(
        processed_dir,
        triage_date,
    )
    report = triage_official_policy_candidates(candidate_input_path, as_of=triage_date)
    report_path = output_path or default_risk_event_candidate_triage_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        report.as_of,
    )
    csv_path = triage_output_path or default_risk_event_candidate_triage_csv_path(
        processed_dir,
        report.as_of,
    )
    write_risk_event_candidate_triage_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(
        f"[{status_style}]官方候选 AI 模块 triage 状态：" f"{report.status}[/{status_style}]"
    )
    console.print(f"报告：{report_path}")
    console.print(f"输入候选：{candidate_input_path}")
    console.print(
        "Bucket："
        f"must_review={report.bucket_counts.get('must_review', 0)}；"
        f"review_next={report.bucket_counts.get('review_next', 0)}；"
        f"sample_review={report.bucket_counts.get('sample_review', 0)}；"
        f"auto_low_relevance={report.bucket_counts.get('auto_low_relevance', 0)}；"
        f"duplicate_or_noise={report.bucket_counts.get('duplicate_or_noise', 0)}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)

    write_risk_event_candidate_triage_csv(report, csv_path)
    console.print(f"Triage CSV：{csv_path}")
    console.print("Triage 结果保持 production_effect=none，未写入评分或仓位闸门。")


@risk_events_app.command("precheck-triaged-official-candidates")
def precheck_triaged_official_candidates_with_openai_command(
    processed_dir: Annotated[
        Path,
        typer.Option(help="官方来源候选、triage CSV 所在 processed 目录。"),
    ] = DEFAULT_OFFICIAL_POLICY_PROCESSED_DIR,
    candidate_input_path: Annotated[
        Path | None,
        typer.Option(help="官方来源待复核候选 CSV 输入路径；为空时按 as_of 推导。"),
    ] = None,
    triage_input_path: Annotated[
        Path | None,
        typer.Option(help="官方候选 AI 模块 triage CSV 输入路径；为空时按 as_of 推导。"),
    ] = None,
    triage_buckets: Annotated[
        str,
        typer.Option(help="逗号分隔的 triage bucket 白名单。"),
    ] = "must_review,review_next",
    max_candidates: Annotated[
        int | None,
        typer.Option(help="覆盖 profile 中本次最多送入 OpenAI 预审的高优先级候选数。"),
    ] = None,
    queue_path: Annotated[
        Path,
        typer.Option(help="写入风险事件预审待复核队列 JSON 的路径。"),
    ] = DEFAULT_RISK_EVENT_PREREVIEW_QUEUE_PATH,
    data_sources_path: Annotated[
        Path,
        typer.Option(help="数据源目录路径，用于解析 provider LLM 权限。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于检查 matched_risk_ids。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    llm_request_profiles_path: Annotated[
        Path,
        typer.Option(help="LLM request profile 配置路径。"),
    ] = DEFAULT_LLM_REQUEST_PROFILES_CONFIG_PATH,
    llm_request_profile: Annotated[
        str,
        typer.Option(help="本次 LLM 请求使用的 profile_id。"),
    ] = DEFAULT_RISK_EVENT_TRIAGED_PREREVIEW_PROFILE,
    as_of: Annotated[
        str | None,
        typer.Option(help="预审和校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown triage OpenAI 预审报告输出路径。"),
    ] = None,
    api_key_env: Annotated[
        str,
        typer.Option(help="读取 OpenAI API key 的环境变量名。"),
    ] = "OPENAI_API_KEY",
    model: Annotated[
        str | None,
        typer.Option(help="覆盖 profile 中的 OpenAI Responses API 模型。"),
    ] = None,
    reasoning_effort: Annotated[
        str | None,
        typer.Option(help="覆盖 profile 中的 OpenAI Responses API reasoning.effort。"),
    ] = None,
    timeout_seconds: Annotated[
        float | None,
        typer.Option(help="覆盖 profile 中的 OpenAI Responses API 请求读超时秒数。"),
    ] = None,
    openai_http_client: Annotated[
        str | None,
        typer.Option(
            help="覆盖 profile 中的 OpenAI Responses API HTTP 客户端：requests 或 urllib。"
        ),
    ] = None,
    openai_cache_dir: Annotated[
        Path,
        typer.Option(help="OpenAI 请求/响应本地缓存与审计归档目录。"),
    ] = DEFAULT_OPENAI_REQUEST_CACHE_PATH,
    openai_cache_ttl_hours: Annotated[
        float | None,
        typer.Option(help="覆盖 profile 中完全相同 OpenAI 请求的本地缓存复用时长，单位小时。"),
    ] = None,
) -> None:
    """只对 triage 高优先级官方候选调用 OpenAI，输出风险等级预审建议。"""
    profile = _load_llm_request_profile(llm_request_profiles_path, llm_request_profile)
    effective_max_candidates = _coalesce_profile_value(
        max_candidates,
        profile.max_candidates,
    )
    effective_model = _coalesce_profile_value(model, profile.model)
    effective_reasoning_effort = _coalesce_profile_value(
        reasoning_effort,
        profile.reasoning_effort,
    )
    effective_timeout_seconds = _coalesce_profile_value(
        timeout_seconds,
        profile.timeout_seconds,
    )
    effective_http_client = _coalesce_profile_value(openai_http_client, profile.http_client)
    effective_cache_ttl_hours = _coalesce_profile_value(
        openai_cache_ttl_hours,
        profile.cache_ttl_hours,
    )
    if effective_max_candidates is None:
        raise typer.BadParameter("LLM request profile 必须设置 max_candidates，或显式传入。")
    if effective_max_candidates < 0:
        raise typer.BadParameter("OpenAI 预审候选上限不能为负数。")
    if effective_timeout_seconds <= 0:
        raise typer.BadParameter("OpenAI 请求超时秒数必须为正数。")
    if effective_cache_ttl_hours <= 0:
        raise typer.BadParameter("OpenAI 请求缓存 TTL 小时数必须为正数。")
    api_key = os.getenv(api_key_env, "")
    if not api_key:
        console.print("[red]缺少 OpenAI API key，已停止高优先级候选风险等级预审。[/red]")
        console.print(f"需要环境变量：{api_key_env}")
        raise typer.Exit(code=1)

    precheck_date = _parse_date(as_of) if as_of else date.today()
    official_candidates_path = candidate_input_path or default_official_policy_candidates_path(
        processed_dir,
        precheck_date,
    )
    triage_path = triage_input_path or default_risk_event_candidate_triage_csv_path(
        processed_dir,
        precheck_date,
    )
    selected_buckets = tuple(_parse_csv_items(triage_buckets))
    report_path = output_path or (
        PROJECT_ROOT
        / "outputs"
        / "reports"
        / f"risk_event_prereview_triaged_openai_{precheck_date.isoformat()}.md"
    )

    try:
        candidates = load_official_policy_candidates_csv(official_candidates_path)
        selected_ids = set(load_triaged_candidate_ids(triage_path, buckets=selected_buckets))
    except (OSError, ValueError) as exc:
        console.print(f"[red]高优先级官方候选输入无法读取或校验失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc

    candidates_by_id = {candidate.candidate_id: candidate for candidate in candidates}
    selected_candidates = tuple(
        candidates_by_id[candidate_id]
        for candidate_id in selected_ids
        if candidate_id in candidates_by_id
    )
    missing_ids = sorted(selected_ids - set(candidates_by_id))
    if missing_ids:
        console.print(
            "[yellow]triage CSV 中有候选未在官方候选 CSV 找到："
            f"{len(missing_ids)} 条；已跳过。[/yellow]"
        )

    report = run_openai_risk_event_prereview_for_official_candidates(
        selected_candidates,
        api_key=api_key,
        data_sources=load_data_sources(data_sources_path),
        risk_events=load_risk_events(risk_events_path),
        input_path=triage_path,
        as_of=precheck_date,
        model=effective_model,
        reasoning_effort=effective_reasoning_effort,
        endpoint=profile.endpoint,
        timeout_seconds=effective_timeout_seconds,
        http_client=effective_http_client,
        openai_cache_dir=openai_cache_dir,
        openai_cache_ttl_seconds=effective_cache_ttl_hours * 3600,
        max_retries=profile.max_retries,
        max_candidates=effective_max_candidates,
    )
    write_risk_event_prereview_import_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(
        f"[{status_style}]高优先级官方候选 OpenAI 预审状态：" f"{report.status}[/{status_style}]"
    )
    console.print(f"报告：{report_path}")
    console.print(f"官方候选 CSV：{official_candidates_path}")
    console.print(f"Triage CSV：{triage_path}")
    console.print(f"Triage buckets：{', '.join(selected_buckets)}")
    console.print(
        f"LLM request profile：{profile.profile_id}；"
        f"model={effective_model}；reasoning={effective_reasoning_effort}"
    )
    console.print(
        f"送入 OpenAI 候选：{len(selected_candidates)}；"
        f"待复核队列：{report.record_count}；"
        f"L2/L3 候选：{report.high_level_candidate_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)

    written_path = write_risk_event_prereview_queue(report, queue_path)
    console.print(f"预审待复核队列：{written_path}")
    console.print("LLM 风险等级仅作为 pending_review 建议，未写入正式风险事件或仓位闸门。")


@risk_events_app.command("apply-llm-formal-assessment")
def apply_llm_formal_assessment_command(
    queue_path: Annotated[
        Path,
        typer.Option(help="风险事件 OpenAI 预审队列 JSON 输入路径。"),
    ] = DEFAULT_RISK_EVENT_PREREVIEW_QUEUE_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="写入正式风险事件 occurrence YAML 的目录。"),
    ] = DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于检查 matched_risk_ids。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="正式评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown LLM 正式评估导入报告输出路径。"),
    ] = None,
    validation_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件发生记录校验报告输出路径。"),
    ] = None,
    min_confidence: Annotated[
        float,
        typer.Option(help="低于该 confidence 的 LLM 预审记录不写入正式 occurrence。"),
    ] = 0.0,
    next_review_days: Annotated[
        int,
        typer.Option(help="LLM formal assessment 的下次复核间隔天数。"),
    ] = 1,
    include_attestation: Annotated[
        bool,
        typer.Option(help="同时写入 LLM formal attestation。"),
    ] = True,
    overwrite: Annotated[
        bool,
        typer.Option(help="允许覆盖同名 LLM formal occurrence/attestation YAML。"),
    ] = False,
) -> None:
    """把 LLM 预审结果作为正式风险评估输入写入 occurrence/attestation。"""
    assessment_date = _parse_date(as_of) if as_of else date.today()
    if min_confidence < 0 or min_confidence > 1:
        raise typer.BadParameter("min_confidence 必须在 0 到 1 之间。")
    if next_review_days < 0:
        raise typer.BadParameter("next_review_days 不能为负数。")
    report_path = output_path or default_llm_formal_assessment_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        assessment_date,
    )
    validation_output = validation_report_path or default_risk_event_occurrence_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        assessment_date,
    )
    try:
        report = build_llm_formal_assessment_report(
            queue_path,
            as_of=assessment_date,
            risk_events=load_risk_events(risk_events_path),
            include_attestation=include_attestation,
            next_review_days=next_review_days,
            min_confidence=min_confidence,
        )
        write_llm_formal_assessment_report(report, report_path)
    except (OSError, ValueError) as exc:
        console.print(f"[red]LLM 正式评估输入无法读取或校验失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]LLM 正式风险评估状态：{report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(f"输入队列：{queue_path}")
    console.print(
        f"预审记录：{report.record_count}；写入 occurrence：{report.occurrence_count}；"
        f"active={report.active_occurrence_count}；watch={report.watch_occurrence_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)

    try:
        written_paths = write_llm_formal_assessment_outputs(
            report,
            output_dir,
            overwrite=overwrite,
        )
    except FileExistsError as exc:
        console.print(f"[red]{exc}[/red]")
        console.print("如确认要更新同名 LLM formal 记录，请显式传入 --overwrite。")
        raise typer.Exit(code=1) from exc

    validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(output_dir),
        risk_events=load_risk_events(risk_events_path),
        as_of=assessment_date,
    )
    review_report = build_risk_event_occurrence_review_report(validation_report)
    write_risk_event_occurrence_review_report(review_report, validation_output)
    validation_style = (
        "green"
        if validation_report.status == "PASS"
        else "yellow" if validation_report.passed else "red"
    )
    console.print(f"写入 YAML：{len(written_paths)} 个文件 -> {output_dir}")
    console.print(
        f"[{validation_style}]风险事件发生记录校验状态："
        f"{validation_report.status}[/{validation_style}]"
    )
    console.print(f"校验报告：{validation_output}")
    console.print("LLM formal assessment 已作为正式评估输入，但不会被标记为人工复核。")
    if not validation_report.passed:
        raise typer.Exit(code=1)


@risk_events_app.command("precheck-openai")
def precheck_risk_events_with_openai_command(
    input_path: Annotated[
        Path,
        typer.Option(help="LLM 预审输入 JSON/YAML，包含 source_id 或 source_permission envelope。"),
    ],
    queue_path: Annotated[
        Path,
        typer.Option(help="写入风险事件预审待复核队列 JSON 的路径。"),
    ] = DEFAULT_RISK_EVENT_PREREVIEW_QUEUE_PATH,
    data_sources_path: Annotated[
        Path,
        typer.Option(help="数据源目录路径，用于解析 provider LLM 权限。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于检查 matched_risk_ids。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    llm_request_profiles_path: Annotated[
        Path,
        typer.Option(help="LLM request profile 配置路径。"),
    ] = DEFAULT_LLM_REQUEST_PROFILES_CONFIG_PATH,
    llm_request_profile: Annotated[
        str,
        typer.Option(help="本次 LLM 请求使用的 profile_id。"),
    ] = DEFAULT_RISK_EVENT_SINGLE_PREREVIEW_PROFILE,
    as_of: Annotated[
        str | None,
        typer.Option(help="预审和校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件 OpenAI 预审报告输出路径。"),
    ] = None,
    api_key_env: Annotated[
        str,
        typer.Option(help="读取 OpenAI API key 的环境变量名。"),
    ] = "OPENAI_API_KEY",
    model: Annotated[
        str | None,
        typer.Option(help="覆盖 profile 中的 OpenAI Responses API 模型。"),
    ] = None,
    reasoning_effort: Annotated[
        str | None,
        typer.Option(help="覆盖 profile 中的 OpenAI Responses API reasoning.effort。"),
    ] = None,
    timeout_seconds: Annotated[
        float | None,
        typer.Option(help="覆盖 profile 中的 OpenAI Responses API 请求读超时秒数。"),
    ] = None,
    openai_http_client: Annotated[
        str | None,
        typer.Option(
            help="覆盖 profile 中的 OpenAI Responses API HTTP 客户端：requests 或 urllib。"
        ),
    ] = None,
    openai_cache_dir: Annotated[
        Path,
        typer.Option(help="OpenAI 请求/响应本地缓存与审计归档目录。"),
    ] = DEFAULT_OPENAI_REQUEST_CACHE_PATH,
    openai_cache_ttl_hours: Annotated[
        float | None,
        typer.Option(help="覆盖 profile 中完全相同 OpenAI 请求的本地缓存复用时长，单位小时。"),
    ] = None,
) -> None:
    """调用 OpenAI API 整理风险事件候选，并写入人工复核队列。"""
    profile = _load_llm_request_profile(llm_request_profiles_path, llm_request_profile)
    effective_model = _coalesce_profile_value(model, profile.model)
    effective_reasoning_effort = _coalesce_profile_value(
        reasoning_effort,
        profile.reasoning_effort,
    )
    effective_timeout_seconds = _coalesce_profile_value(
        timeout_seconds,
        profile.timeout_seconds,
    )
    effective_http_client = _coalesce_profile_value(openai_http_client, profile.http_client)
    effective_cache_ttl_hours = _coalesce_profile_value(
        openai_cache_ttl_hours,
        profile.cache_ttl_hours,
    )
    if effective_timeout_seconds <= 0:
        raise typer.BadParameter("OpenAI 请求超时秒数必须为正数。")
    if effective_cache_ttl_hours <= 0:
        raise typer.BadParameter("OpenAI 请求缓存 TTL 小时数必须为正数。")
    precheck_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_risk_event_openai_prereview_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        precheck_date,
    )
    try:
        packet = load_llm_claim_precheck_input(input_path)
    except (OSError, ValueError) as exc:
        console.print(f"[red]风险事件 OpenAI 预审输入无法读取或校验失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc

    report = run_openai_risk_event_prereview(
        packet,
        api_key=os.getenv(api_key_env, ""),
        data_sources=load_data_sources(data_sources_path),
        risk_events=load_risk_events(risk_events_path),
        input_path=input_path,
        as_of=precheck_date,
        model=effective_model,
        reasoning_effort=effective_reasoning_effort,
        endpoint=profile.endpoint,
        timeout_seconds=effective_timeout_seconds,
        http_client=effective_http_client,
        openai_cache_dir=openai_cache_dir,
        openai_cache_ttl_seconds=effective_cache_ttl_hours * 3600,
        max_retries=profile.max_retries,
    )
    write_risk_event_prereview_import_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]风险事件 OpenAI 预审状态：" f"{report.status}[/{status_style}]")
    console.print(f"预审报告：{report_path}")
    console.print(
        f"LLM request profile：{profile.profile_id}；"
        f"model={effective_model}；reasoning={effective_reasoning_effort}"
    )
    console.print(
        f"LLM claim 数：{report.row_count}；"
        f"风险事件候选：{report.record_count}；"
        f"L2/L3 候选：{report.high_level_candidate_count}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)

    written_path = write_risk_event_prereview_queue(report, queue_path)
    console.print(f"预审待复核队列：{written_path}")
    console.print("OpenAI 输出保持 llm_extracted / pending_review，不进入评分或仓位闸门。")


@risk_events_app.command("import-prereview-csv")
def import_risk_event_prereview_csv_command(
    input_path: Annotated[
        Path,
        typer.Option(help="OpenAI 结构化预审结果 CSV 输入路径。"),
    ],
    queue_path: Annotated[
        Path,
        typer.Option(help="写入风险事件预审待复核队列 JSON 的路径。"),
    ] = DEFAULT_RISK_EVENT_PREREVIEW_QUEUE_PATH,
    risk_events_path: Annotated[
        Path,
        typer.Option(help="风险事件配置路径，用于检查 matched_risk_ids。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="导入和校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 预审导入报告输出路径。"),
    ] = None,
) -> None:
    """导入 OpenAI 风险事件预审结果，并写入人工复核队列。"""
    import_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_risk_event_prereview_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        import_date,
    )
    import_report = import_risk_event_prereview_csv(
        input_path,
        risk_events=load_risk_events(risk_events_path),
        as_of=import_date,
    )
    write_risk_event_prereview_import_report(import_report, report_path)

    status_style = (
        "green" if import_report.status == "PASS" else "yellow" if import_report.passed else "red"
    )
    console.print(
        f"[{status_style}]风险事件 OpenAI 预审导入状态：" f"{import_report.status}[/{status_style}]"
    )
    console.print(f"导入报告：{report_path}")
    console.print(
        f"CSV 行数：{import_report.row_count}；"
        f"预审记录：{import_report.record_count}；"
        f"L2/L3 候选：{import_report.high_level_candidate_count}"
    )
    console.print(f"错误数：{import_report.error_count}；警告数：{import_report.warning_count}")
    if not import_report.passed:
        raise typer.Exit(code=1)

    written_path = write_risk_event_prereview_queue(import_report, queue_path)
    console.print(f"预审待复核队列：{written_path}")
    console.print("预审记录保持 llm_extracted / pending_review，不进入评分或仓位闸门。")


@risk_events_app.command("validate-occurrences")
def validate_risk_event_occurrences(
    input_path: Annotated[
        Path,
        typer.Option(help="风险事件发生记录 YAML 文件或目录路径。"),
    ] = DEFAULT_RISK_EVENT_OCCURRENCES_PATH,
    config_path: Annotated[
        Path,
        typer.Option(help="风险事件规则配置文件路径。"),
    ] = DEFAULT_RISK_EVENTS_CONFIG_PATH,
    as_of: Annotated[
        str | None,
        typer.Option(help="校验日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 风险事件发生记录报告输出路径。"),
    ] = None,
) -> None:
    """校验实际发生的风险事件记录，并生成可供日报评分引用的报告。"""
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_risk_event_occurrence_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )
    validation_report = validate_risk_event_occurrence_store(
        store=load_risk_event_occurrence_store(input_path),
        risk_events=load_risk_events(config_path),
        as_of=validation_date,
    )
    review_report = build_risk_event_occurrence_review_report(validation_report)
    write_risk_event_occurrence_review_report(review_report, report_path)

    status_style = (
        "green"
        if review_report.status == "PASS"
        else "yellow" if validation_report.passed else "red"
    )
    console.print(f"[{status_style}]风险事件发生记录状态：{review_report.status}[/{status_style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"发生记录数：{validation_report.occurrence_count}；"
        f"活跃/观察：{validation_report.active_occurrence_count}；"
        f"可评分：{len(review_report.score_eligible_active_items)}；"
        f"可触发仓位闸门：{len(review_report.position_gate_eligible_active_items)}"
    )
    console.print(
        f"复核声明数：{validation_report.review_attestation_count}；"
        f"当前有效：{validation_report.current_review_attestation_count}"
    )
    console.print(
        f"错误数：{validation_report.error_count}；警告数：{validation_report.warning_count}"
    )

    if not validation_report.passed:
        raise typer.Exit(code=1)
