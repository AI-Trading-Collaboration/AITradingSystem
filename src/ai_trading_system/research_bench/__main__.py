"""Command line for the research bench. Output is summaries only; no rows of data are printed.

python -m ai_trading_system.research_bench benchmarks [--snapshot ID] [--qqq-series proxy]
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.research_bench.data import QqqSeries, from_snapshot
from ai_trading_system.research_bench.metrics import cvar_field
from ai_trading_system.research_bench.run import benchmark_summaries
from ai_trading_system.research_bench.settings import load_settings
from ai_trading_system.research_data import qqq_long_history as qlh

app = typer.Typer(
    help="研究评估台（TRADING-2565、TRADING-2567 S3）：输出只有汇总。", no_args_is_help=True
)


@app.callback()
def main() -> None:
    """研究评估台。"""


@app.command("benchmarks")
def benchmarks_command(
    snapshot: Annotated[str, typer.Option(help="长历史快照 ID，默认 LATEST。")] = "",
    root: Annotated[Path, typer.Option(help="快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
    qqq_series: Annotated[
        str, typer.Option(help="QQQ 用哪条序列：proxy（总收益指数减费率）或 actual（真实 QQQ）。")
    ] = "proxy",
) -> None:
    """在开发窗口上跑固定基准（买入持有 QQQ、现金、固定 2 倍和 3 倍），核对数据和评估台。"""

    if qqq_series not in ("proxy", "actual"):
        typer.echo("benchmarks: --qqq-series must be proxy or actual")
        raise typer.Exit(code=1)
    series: QqqSeries = "proxy" if qqq_series == "proxy" else "actual"
    loaded = load_settings()
    data = from_snapshot(qlh.resolve_snapshot_dir(root, snapshot or None), qqq_series=series)
    started = time.perf_counter()
    window, summaries = benchmark_summaries(data, loaded)
    seconds = time.perf_counter() - started
    typer.echo(
        f"snapshot {data.source['snapshot_id']} check {data.source['check_status']}"
        f" warnings {','.join(data.source['check_warnings']) or '-'}; qqq {series}"
    )
    typer.echo(
        f"window {window.window_id} ({window.role}) {window.start}..{window.end or 'latest'};"
        f" cost {loaded.cost_bps:g} bps; {len(summaries)} simulations in {seconds:.2f}s"
    )
    tail = cvar_field(max(loaded.settings.cvar_levels))
    typer.echo(
        f"{'benchmark':<18}{'geo/yr':>9}{'vol':>8}{'maxDD':>9}{tail:>11}"
        f"{'worstM':>9}{'roll3y p10':>12}"
    )
    for name, row in summaries.items():
        typer.echo(
            f"{name:<18}{_pct(row['geometric_annual_return']):>9}{_pct(row['annual_volatility']):>8}"
            f"{_pct(row['max_drawdown']):>9}{_pct(row[tail], 2):>11}{_pct(row['worst_month']):>9}"
            f"{_pct(row['rolling_geometric_return_low']):>12}"
        )


def _pct(value: Any, digits: int = 1) -> str:
    if value is None:
        return "-"
    return f"{float(value) * 100:.{digits}f}%"


if __name__ == "__main__":
    app()
