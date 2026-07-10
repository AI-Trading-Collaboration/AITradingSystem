from __future__ import annotations

from datetime import date

import typer
from rich.console import Console

from ai_trading_system.execution_semantics import DEFAULT_AI_REGIME_BACKTEST_START

console = Console()


def cli_scalar(value: object) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def date_range_kwargs(
    as_of: str | None,
    start_date: str | None,
    end_date: str | None,
) -> dict[str, date | None]:
    """Adapt CLI date strings to the established execution-builder contract."""
    return {
        "as_of_date": parse_optional_date(as_of),
        "start_date": parse_optional_date(start_date) or DEFAULT_AI_REGIME_BACKTEST_START,
        "end_date": parse_optional_date(end_date),
    }


def as_of_kwargs(as_of: str | None) -> dict[str, date | None]:
    """Adapt the CLI observation date to the established builder parameter name."""
    return {"as_of_date": parse_optional_date(as_of)}


def print_execution_semantics_payload(title: str, payload: dict[str, object]) -> None:
    status = str(payload.get("status"))
    style = "green" if "READY" in status or "PASS" in status or "SAFE" in status else "yellow"
    if "BLOCKED" in status or "FAIL" in status:
        style = "red"
    console.print(f"[{style}]{title}：{status}[/{style}]")
    paths = payload.get("artifact_paths")
    if isinstance(paths, dict):
        if paths.get("json_path"):
            console.print(f"JSON：{paths.get('json_path')}")
        if paths.get("index"):
            console.print(f"Index：{paths.get('index')}")
        if paths.get("markdown_path"):
            console.print(f"Markdown：{paths.get('markdown_path')}")
        if paths.get("review_markdown"):
            console.print(f"Markdown：{paths.get('review_markdown')}")
        if paths.get("yaml_path"):
            console.print(f"YAML：{paths.get('yaml_path')}")
        if paths.get("review_yaml"):
            console.print(f"YAML：{paths.get('review_yaml')}")
    for field, expected in (
        ("paper_shadow_allowed", False),
        ("production_allowed", False),
        ("broker_action", "none"),
        ("manual_review_required", True),
        ("production_effect", "none"),
    ):
        console.print(f"{field}={payload.get(field, expected)}")


def parse_optional_date(value: str | None) -> date | None:
    if value in (None, ""):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("date must use YYYY-MM-DD") from exc
