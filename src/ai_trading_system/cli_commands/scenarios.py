from __future__ import annotations

from datetime import date

import typer
from rich.console import Console

console = Console()
scenarios_app = typer.Typer(help="AI 产业链情景压力测试库。", no_args_is_help=True)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须是 YYYY-MM-DD") from exc
