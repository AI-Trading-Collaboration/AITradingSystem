from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.report_traceability import lookup_trace_record, render_trace_lookup

console = Console()
trace_app = typer.Typer(help="报告 evidence bundle 反查。", no_args_is_help=True)


@trace_app.command("lookup")
def trace_lookup(
    bundle_path: Annotated[
        Path,
        typer.Option(help="evidence bundle JSON 路径。"),
    ],
    object_id: Annotated[
        str,
        typer.Option("--id", help="claim/evidence/dataset/quality/run id。"),
    ],
) -> None:
    """按 ID 反查报告 evidence bundle 中的上下文。"""
    try:
        record_type, record = lookup_trace_record(bundle_path, object_id)
    except FileNotFoundError as exc:
        raise typer.BadParameter(f"evidence bundle 不存在：{bundle_path}") from exc
    except KeyError as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_trace_lookup(record_type, record))
