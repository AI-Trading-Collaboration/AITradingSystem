from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.config import (
    DEFAULT_DATA_SOURCES_CONFIG_PATH,
    PROJECT_ROOT,
    load_data_sources,
)
from ai_trading_system.llm_precheck import (
    DEFAULT_OPENAI_REQUEST_CACHE_DIR,
    default_llm_claim_precheck_report_path,
    load_llm_claim_precheck_input,
    run_openai_claim_precheck,
    write_llm_claim_precheck_report,
    write_llm_claim_prereview_queue,
)
from ai_trading_system.llm_request_profiles import (
    DEFAULT_LLM_REQUEST_PROFILES_CONFIG_PATH,
    LlmRequestProfile,
    load_llm_request_profiles,
)

console = Console()
llm_app = typer.Typer(help="LLM 结构化预审和待复核队列。", no_args_is_help=True)

DEFAULT_LLM_CLAIM_PREREVIEW_QUEUE_PATH = (
    PROJECT_ROOT / "data" / "processed" / "llm_claim_prereview_queue.json"
)
DEFAULT_OPENAI_REQUEST_CACHE_PATH = PROJECT_ROOT / DEFAULT_OPENAI_REQUEST_CACHE_DIR
DEFAULT_LLM_CLAIM_PREREVIEW_PROFILE = "llm_claim_prereview"


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须是 YYYY-MM-DD 格式。") from exc


def _load_llm_request_profile(
    profiles_path: Path,
    profile_id: str,
) -> LlmRequestProfile:
    try:
        return load_llm_request_profiles(profiles_path).get_profile(profile_id)
    except (OSError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc


def _coalesce_profile_value(value, profile_value):
    return profile_value if value is None else value


@llm_app.command("precheck-claims")
def precheck_llm_claims_command(
    input_path: Annotated[
        Path,
        typer.Option(help="LLM 预审输入 JSON/YAML，包含 source_id 或 source_permission envelope。"),
    ],
    queue_path: Annotated[
        Path,
        typer.Option(help="写入 LLM claim 待复核队列 JSON 的路径。"),
    ] = DEFAULT_LLM_CLAIM_PREREVIEW_QUEUE_PATH,
    data_sources_path: Annotated[
        Path,
        typer.Option(help="数据源目录路径，用于解析 provider LLM 权限。"),
    ] = DEFAULT_DATA_SOURCES_CONFIG_PATH,
    llm_request_profiles_path: Annotated[
        Path,
        typer.Option(help="LLM request profile 配置路径。"),
    ] = DEFAULT_LLM_REQUEST_PROFILES_CONFIG_PATH,
    llm_request_profile: Annotated[
        str,
        typer.Option(help="本次 LLM 请求使用的 profile_id。"),
    ] = DEFAULT_LLM_CLAIM_PREREVIEW_PROFILE,
    as_of: Annotated[
        str | None,
        typer.Option(help="报告日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown LLM 预审报告输出路径。"),
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
    """调用 OpenAI 结构化输出生成 claim 待复核队列。"""
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
    report_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_llm_claim_precheck_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        report_date,
    )
    try:
        packet = load_llm_claim_precheck_input(input_path)
    except (OSError, ValueError) as exc:
        console.print(f"[red]LLM 预审输入无法读取或校验失败：{exc}[/red]")
        raise typer.Exit(code=1) from exc

    report = run_openai_claim_precheck(
        packet,
        api_key=os.getenv(api_key_env, ""),
        data_sources=load_data_sources(data_sources_path),
        input_path=input_path,
        model=effective_model,
        reasoning_effort=effective_reasoning_effort,
        endpoint=profile.endpoint,
        timeout_seconds=effective_timeout_seconds,
        http_client=effective_http_client,
        openai_cache_dir=openai_cache_dir,
        openai_cache_ttl_seconds=effective_cache_ttl_hours * 3600,
        max_retries=profile.max_retries,
    )
    write_llm_claim_precheck_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]LLM 证据预审状态：{report.status}[/{status_style}]")
    console.print(f"预审报告：{report_path}")
    console.print(f"预审记录：{report.record_count}；待复核 claim：{report.pending_review_count}")
    console.print(
        f"LLM request profile：{profile.profile_id}；"
        f"model={effective_model}；reasoning={effective_reasoning_effort}"
    )
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    if not report.passed:
        raise typer.Exit(code=1)

    written_path = write_llm_claim_prereview_queue(report, queue_path)
    console.print(f"LLM claim 待复核队列：{written_path}")
    console.print("LLM 输出保持 llm_extracted / pending_review，不进入评分或仓位闸门。")
